#!/usr/bin/env cargo
//! Query script to verify data exists in Supabase Iceberg table
//!
//! This script connects to the Supabase Iceberg table and queries for data
//! to prove that the pipeline actually wrote data successfully.

use dotenv::dotenv;
use etl_destinations::iceberg::IcebergClient;
use std::env;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging for detailed output
    tracing_subscriber::fmt::init();

    // Load environment variables
    dotenv().ok();

    info!("🔍 Starting Supabase Iceberg table verification");

    // Get configuration from environment
    let catalog_uri =
        env::var("CATALOG_URI").map_err(|_| "CATALOG_URI environment variable is required")?;
    let warehouse =
        env::var("WAREHOUSE").map_err(|_| "WAREHOUSE environment variable is required")?;
    let token = env::var("TOKEN").map_err(|_| "TOKEN environment variable is required")?;

    info!(
        catalog_uri = %catalog_uri,
        warehouse = %warehouse,
        "Connecting to Supabase Iceberg catalog"
    );

    // Create Iceberg client
    let client = IcebergClient::new_with_rest_catalog(
        catalog_uri,
        warehouse,
        "echo".to_string(), // namespace
        Some(token),
    )
    .await?;

    info!("✅ Successfully connected to Iceberg catalog");

    // Check if table exists
    let table_name = "foo_orders";
    let table_exists = client.table_exists(table_name).await?;

    if !table_exists {
        error!("❌ Table '{}' does not exist!", table_name);
        return Ok(());
    }

    info!("✅ Table '{}' exists", table_name);

    // List all tables in namespace
    info!("📋 Listing all tables in namespace 'echo':");
    let tables = client.list_tables().await?;
    for table in &tables {
        info!("  - {}", table);
    }

    if tables.is_empty() {
        info!("ℹ️  No tables found in namespace");
        return Ok(());
    }

    // Query the table for data
    info!("🔍 Querying table '{}' for data...", table_name);

    // Query with limit to avoid overwhelming output
    let rows = client.query_table(table_name, Some(10)).await?;

    info!("📊 Query results:");
    info!("  Total rows returned: {}", rows.len());

    if rows.is_empty() {
        error!("❌ Table '{}' contains NO DATA!", table_name);
        error!("   This means either:");
        error!("   1. The pipeline didn't actually write data");
        error!("   2. The data was written but not committed properly");
        error!("   3. There's an issue with the query implementation");
    } else {
        info!(
            "✅ SUCCESS! Table '{}' contains {} rows of data!",
            table_name,
            rows.len()
        );

        // Show first few rows as proof
        for (i, row) in rows.iter().take(3).enumerate() {
            info!("  Row {}: {} columns", i + 1, row.values.len());
            for (j, cell) in row.values.iter().enumerate() {
                info!("    Column {}: {:?}", j + 1, cell);
            }
        }

        if rows.len() > 3 {
            info!("  ... and {} more rows", rows.len() - 3);
        }
    }

    // Try querying with no limit to get total count
    info!("🔍 Attempting to get total row count...");
    let all_rows = client.query_table(table_name, None).await?;
    info!("📈 Total rows in table: {}", all_rows.len());

    // Summary
    info!("📋 VERIFICATION SUMMARY:");
    info!("  Catalog URI: {}", client.catalog_uri());
    info!("  Warehouse: {}", client.warehouse());
    info!("  Namespace: {}", client.namespace());
    info!("  Table exists: {}", table_exists);
    info!("  Total tables in namespace: {}", tables.len());
    info!("  Rows in '{}': {}", table_name, all_rows.len());

    if all_rows.is_empty() {
        error!("❌ CONCLUSION: The table exists but contains NO DATA");
        error!("   The pipeline may not be writing data correctly to Iceberg storage");
    } else {
        info!("✅ CONCLUSION: Data successfully verified in Iceberg table!");
        info!("   The pipeline is working and data is persisted in Supabase Iceberg");
    }

    Ok(())
}
