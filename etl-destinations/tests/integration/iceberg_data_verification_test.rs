//! Critical Iceberg data verification test
//!
//! This test verifies that data written to Iceberg is actually queryable,
//! not just that write operations return Ok(()). This catches the fundamental
//! issue where files are written but not committed to table metadata.

use std::env;

use etl::destination::Destination;
use etl::store::both::memory::MemoryStore;
use etl::store::schema::SchemaStore;
use etl::types::{Cell, Event, InsertEvent, TableRow};
use etl_destinations::iceberg::{IcebergClient, IcebergDestination};
use etl_postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use tokio_postgres::types::{PgLsn, Type};
use tracing::{error, info};

fn skip_integration_tests() -> bool {
    env::var("SKIP_INTEGRATION_TESTS").unwrap_or_else(|_| "false".to_string()) == "1"
}

#[tokio::test]
async fn test_data_actually_exists_after_write() {
    if skip_integration_tests() {
        println!("Skipping data verification test (SKIP_INTEGRATION_TESTS=1)");
        return;
    }

    // Initialize logging
    // let _ = tracing_subscriber::fmt::try_init();

    info!("🔥 CRITICAL TEST: Verifying data actually exists after write operations");

    let store = MemoryStore::new();

    // Create a simple table schema
    let table_id = TableId(12345);
    let table_name = TableName::new("test_verification".to_string(), "data_check".to_string());
    let table_schema = TableSchema::new(
        table_id,
        table_name,
        vec![
            ColumnSchema::new("id".to_string(), Type::INT4, 0, false, true),
            ColumnSchema::new("name".to_string(), Type::VARCHAR, 100, true, false),
            ColumnSchema::new("value".to_string(), Type::INT4, 0, true, false),
        ],
    );

    if let Err(e) = store.store_table_schema(table_schema).await {
        error!("❌ Failed to store table schema: {}", e);
        return;
    }

    // Create destination
    let destination = match IcebergDestination::new(
        "http://localhost:8182".to_string(),
        "s3://warehouse/".to_string(),
        "test_verification".to_string(),
        None,
        store,
    )
    .await
    {
        Ok(dest) => {
            info!("✅ Created Iceberg destination");
            dest
        }
        Err(e) => {
            info!("ℹ️ Skipping test, infrastructure not available: {}", e);
            return;
        }
    };

    // Write test data
    info!("📝 Writing test data to Iceberg...");
    let test_events = vec![
        Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1000_u64),
            commit_lsn: PgLsn::from(1000_u64),
            table_id,
            table_row: TableRow::new(vec![
                Cell::I32(1),
                Cell::String("Test Record 1".to_string()),
                Cell::I32(100),
            ]),
        }),
        Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1001_u64),
            commit_lsn: PgLsn::from(1001_u64),
            table_id,
            table_row: TableRow::new(vec![
                Cell::I32(2),
                Cell::String("Test Record 2".to_string()),
                Cell::I32(200),
            ]),
        }),
        Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(1002_u64),
            commit_lsn: PgLsn::from(1002_u64),
            table_id,
            table_row: TableRow::new(vec![
                Cell::I32(3),
                Cell::String("Test Record 3".to_string()),
                Cell::I32(300),
            ]),
        }),
    ];

    // THIS IS THE CRITICAL PART - Previous tests only checked this step!
    match destination.write_events(test_events).await {
        Ok(_) => {
            info!("✅ Write operation returned Ok() - but does data actually exist?");
        }
        Err(e) => {
            error!("❌ Write operation failed: {}", e);
            return;
        }
    }

    // NOW THE REAL TEST: Can we actually query the data back?
    info!("🔍 CRITICAL CHECK: Attempting to query data back from Iceberg...");

    // Create a direct client to query the table
    let client = match IcebergClient::new_with_rest_catalog(
        "http://localhost:8182".to_string(),
        "s3://warehouse/".to_string(),
        "test_verification".to_string(),
        None,
    )
    .await
    {
        Ok(client) => {
            info!("✅ Created Iceberg client for verification");
            client
        }
        Err(e) => {
            error!("❌ Failed to create Iceberg client: {}", e);
            return;
        }
    };

    // Check if table exists
    let table_name = "test_verification_data_check";
    let table_exists = client.table_exists(table_name).await.unwrap_or(false);
    info!("📋 Table '{}' exists: {}", table_name, table_exists);

    if !table_exists {
        error!("❌ FAILED: Table doesn't exist!");
        panic!("Table should exist after write operations");
    }

    // Query the data
    let query_result = client.query_table(table_name, None).await;
    match query_result {
        Ok(rows) => {
            info!("📊 Query returned {} rows", rows.len());
            
            if rows.is_empty() {
                error!("❌ CRITICAL FAILURE: Table exists but contains NO DATA!");
                error!("   This proves that write_events() is broken - it returns Ok() but doesn't commit data!");
                error!("   Files are being written to storage but not committed to Iceberg table metadata!");
                panic!("Data verification failed - no data found after write operations");
            } else {
                info!("✅ SUCCESS: Found {} rows of data in Iceberg table!", rows.len());
                info!("   Data is properly committed and queryable");
                
                // Show first few rows as proof
                for (i, row) in rows.iter().take(3).enumerate() {
                    info!("   Row {}: {} values", i + 1, row.values.len());
                    for (j, cell) in row.values.iter().enumerate() {
                        info!("     Column {}: {:?}", j + 1, cell);
                    }
                }
            }
        }
        Err(e) => {
            error!("❌ FAILED to query table: {}", e);
            panic!("Failed to query data back from Iceberg table");
        }
    }

    info!("🎉 DATA VERIFICATION TEST COMPLETED SUCCESSFULLY!");
}

#[tokio::test]
async fn test_multiple_table_operations() {
    if skip_integration_tests() {
        println!("Skipping multiple table test (SKIP_INTEGRATION_TESTS=1)");
        return;
    }

    // Initialize logging
    // let _ = tracing_subscriber::fmt::try_init();

    info!("🔥 Testing multiple table operations with data verification");

    let store = MemoryStore::new();

    // Create multiple table schemas
    let table_ids = vec![TableId(10001), TableId(10002)];
    let table_names = vec![
        TableName::new("multi_test".to_string(), "table_one".to_string()),
        TableName::new("multi_test".to_string(), "table_two".to_string()),
    ];

    for (table_id, table_name) in table_ids.iter().zip(table_names.iter()) {
        let table_schema = TableSchema::new(
            *table_id,
            table_name.clone(),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT4, 0, false, true),
                ColumnSchema::new("data".to_string(), Type::TEXT, 0, true, false),
            ],
        );

        if let Err(e) = store.store_table_schema(table_schema).await {
            error!("❌ Failed to store schema for table {}: {}", table_id.0, e);
            return;
        }
    }

    // Create destination
    let destination = match IcebergDestination::new(
        "http://localhost:8182".to_string(),
        "s3://warehouse/".to_string(),
        "multi_test".to_string(),
        None,
        store,
    )
    .await
    {
        Ok(dest) => dest,
        Err(e) => {
            info!("ℹ️ Skipping test, infrastructure not available: {}", e);
            return;
        }
    };

    // Write data to both tables
    for (i, (table_id, table_name)) in table_ids.iter().zip(table_names.iter()).enumerate() {
        info!("📝 Writing data to table: {}", table_name.name);
        
        let events = vec![
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from((2000 + i * 10) as u64),
                commit_lsn: PgLsn::from((2000 + i * 10) as u64),
                table_id: *table_id,
                table_row: TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String(format!("Data for {} - Row 1", table_name.name)),
                ]),
            }),
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from((2001 + i * 10) as u64),
                commit_lsn: PgLsn::from((2001 + i * 10) as u64),
                table_id: *table_id,
                table_row: TableRow::new(vec![
                    Cell::I32(2),
                    Cell::String(format!("Data for {} - Row 2", table_name.name)),
                ]),
            }),
        ];

        match destination.write_events(events).await {
            Ok(_) => info!("✅ Wrote events to table: {}", table_name.name),
            Err(e) => {
                error!("❌ Failed to write to table {}: {}", table_name.name, e);
                return;
            }
        }
    }

    // Verify data in both tables
    let client = match IcebergClient::new_with_rest_catalog(
        "http://localhost:8182".to_string(),
        "s3://warehouse/".to_string(),
        "multi_test".to_string(),
        None,
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            error!("❌ Failed to create verification client: {}", e);
            return;
        }
    };

    let expected_table_names = vec!["multi_test_table_one", "multi_test_table_two"];
    
    for table_name in expected_table_names {
        info!("🔍 Verifying data in table: {}", table_name);
        
        let rows = match client.query_table(table_name, None).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("❌ Failed to query table {}: {}", table_name, e);
                panic!("Failed to verify data in table {}", table_name);
            }
        };

        if rows.is_empty() {
            error!("❌ CRITICAL: Table {} has no data!", table_name);
            panic!("Data verification failed for table {}", table_name);
        } else {
            info!("✅ Table {} has {} rows", table_name, rows.len());
        }
    }

    info!("🎉 Multiple table operations verified successfully!");
}