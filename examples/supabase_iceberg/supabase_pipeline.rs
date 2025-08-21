//! Supabase Iceberg CDC Pipeline Example
//!
//! This example demonstrates a real-world CDC (Change Data Capture) pipeline that:
//! - Sets up a PostgreSQL publication for logical replication
//! - Creates a real orders table in PostgreSQL
//! - Uses ETL Pipeline for CDC streaming from PostgreSQL to Supabase Iceberg
//! - Handles all CDC operations (INSERT, UPDATE, DELETE, TRUNCATE) in real-time
//! - Shows performance metrics and error handling

use dotenv::dotenv;
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;
use etl_destinations::iceberg::IcebergDestination;
use std::env;
use std::str::FromStr;
use tokio_postgres::{Config, NoTls};
use tracing::{error, info};
use url::Url;

/// Main CDC pipeline demonstration
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables from .env file
    dotenv().ok();

    info!("🚀 Starting Supabase PostgreSQL CDC → Supabase Iceberg Pipeline");

    // Step 1: Verify environment configuration
    let postgres_url =
        env::var("POSTGRES_DB").map_err(|_| "POSTGRES_DB environment variable is required")?;
    let catalog_uri =
        env::var("CATALOG_URI").map_err(|_| "CATALOG_URI environment variable is required")?;
    let warehouse =
        env::var("WAREHOUSE").map_err(|_| "WAREHOUSE environment variable is required")?;
    let token = env::var("TOKEN")
        .map_err(|_| "TOKEN environment variable is required for Supabase authentication")?;

    // Load S3 credentials for Supabase storage
    let _aws_access_key = env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| "AWS_ACCESS_KEY_ID environment variable is required")?;
    let _aws_secret_key = env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| "AWS_SECRET_ACCESS_KEY environment variable is required")?;

    info!("✅ Environment configuration loaded successfully");

    // Step 2: Set up PostgreSQL database and publication
    info!("📊 Setting up PostgreSQL database and publication...");
    setup_postgres_database(&postgres_url).await?;
    info!("✅ PostgreSQL database and publication configured");

    // Step 3: Parse PostgreSQL connection details
    let pg_config = parse_postgres_config(&postgres_url)?;

    // Step 4: Set up Iceberg destination
    info!("🧊 Setting up Supabase Iceberg destination...");
    let store = MemoryStore::new();
    let iceberg_destination = IcebergDestination::new(
        catalog_uri,
        warehouse,
        "echo".to_string(),
        Some(token),
        store.clone(),
    )
    .await?;
    info!("✅ Supabase Iceberg destination configured");

    // Step 5: Configure the CDC pipeline
    info!("⚡ Configuring CDC pipeline...");
    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: "orders_publication".to_string(),
        pg_connection: pg_config,
        batch: BatchConfig {
            max_size: 1000,
            max_fill_ms: 2000,  // Changed from 5000ms to 2000ms for faster flushing
        },
        table_error_retry_delay_ms: 10000,
        max_table_sync_workers: 4,
    };

    // Step 6: Create and start the CDC pipeline
    info!("🚀 Starting CDC pipeline...");
    let mut pipeline = Pipeline::new(pipeline_config, store, iceberg_destination);

    // Start the pipeline
    pipeline.start().await?;

    info!("✅ CDC Pipeline started successfully!");
    info!("💡 The pipeline is now streaming changes from PostgreSQL to Supabase Iceberg");
    info!("📊 Insert records into your unique table to see CDC in action");
    info!("🔄 Pipeline will continue running for monitoring...");

    // Wait for the pipeline to run (this keeps it running indefinitely)
    pipeline.wait().await?;

    Ok(())
}

/// Set up PostgreSQL database with orders table and publication
async fn setup_postgres_database(postgres_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = Config::from_str(postgres_url)?.connect(NoTls).await?;

    // Handle the connection in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {}", e);
        }
    });

    // Terminate active replication connections and clean up slots
    let _ = client.execute("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name LIKE 'supabase_etl_%' AND active_pid IS NOT NULL", &[]).await;
    let _ = client.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'supabase_etl_%'", &[]).await;

    // Generate unique table name based on timestamp to avoid conflicts
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let table_name = format!("orders_{}", unique_suffix);
    let full_table_name = format!("foo.{}", table_name);
    
    info!(
        "📋 Using unique table name: {} to avoid conflicts",
        full_table_name
    );

    // Setup schema and table with unique name
    client.execute("CREATE SCHEMA IF NOT EXISTS foo", &[]).await?;
    client.execute(&format!(
        "CREATE TABLE IF NOT EXISTS {} (order_id BIGSERIAL PRIMARY KEY, customer_name VARCHAR(255) NOT NULL, product VARCHAR(255) NOT NULL, quantity INTEGER NOT NULL, price DECIMAL(10,2) NOT NULL, order_date TIMESTAMPTZ DEFAULT NOW())",
        full_table_name
    ), &[]).await?;

    // Recreate publication with unique table
    client.execute("DROP PUBLICATION IF EXISTS orders_publication", &[]).await?;
    client.execute(&format!(
        "CREATE PUBLICATION orders_publication FOR TABLE {}",
        full_table_name
    ), &[]).await?;
    Ok(())
}

/// Parse PostgreSQL connection URL into PgConnectionConfig
fn parse_postgres_config(
    postgres_url: &str,
) -> Result<PgConnectionConfig, Box<dyn std::error::Error>> {
    let url = Url::parse(postgres_url)?;

    let host = url.host_str().unwrap_or("localhost").to_string();
    let port = url.port().unwrap_or(5432);
    let database = url.path().trim_start_matches('/');
    let database = if database.is_empty() {
        "postgres"
    } else {
        database
    };

    Ok(PgConnectionConfig {
        host,
        port,
        name: database.to_string(),
        username: url.username().to_string(),
        password: url.password().map(|p| p.to_string().into()),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    })
}
