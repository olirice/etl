//! Apache Iceberg destination for ETL pipelines.
//!
//! This module provides a complete implementation of the ETL destination trait for Apache Iceberg,
//! enabling real-time data replication from PostgreSQL to Iceberg tables with full CDC support.
//!
//! # Features
//!
//! - **Real-time CDC**: Support for INSERT, UPDATE, DELETE, and TRUNCATE operations
//! - **Schema Conversion**: PostgreSQL to Iceberg schema mapping (evolution pending upstream support)
//! - **Multiple Catalogs**: Support for REST, SQL, and Glue catalogs (with auto-detection)
//! - **Cloud Storage**: Integration with S3, GCS, Azure, and local filesystem
//! - **Batch Optimization**: Intelligent batching for optimal performance
//! - **Error Recovery**: Comprehensive retry logic with exponential backoff
//! - **Monitoring**: Built-in metrics and structured logging
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use etl_destinations::iceberg::IcebergDestination;
//! use etl::store::both::memory::MemoryStore;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create destination with store - automatically detects catalog type
//! let store = MemoryStore::new();
//! let destination = IcebergDestination::new(
//!     "http://localhost:8181".to_string(),  // or Supabase URL
//!     "s3://my-bucket/warehouse".to_string(),
//!     "etl".to_string(),
//!     None,  // or Some("auth-token".to_string()) for Supabase
//!     store,
//! ).await?;
//!
//! // Use with ETL pipeline
//! // let pipeline = Pipeline::new(pg_config, destination, store);
//! # Ok(())
//! # }
//! ```

// Core modules
pub mod catalog;
pub mod client;
pub mod config;
pub mod constants;
pub mod data;
pub mod destination;

// Re-export main types for convenience
pub use catalog::{CatalogConfig, create_catalog};
pub use client::IcebergClient;
pub use constants::{DEFAULT_NAMESPACE, DEFAULT_TABLE_PREFIX, cdc_columns, cdc_operations};
pub use data::{CellToArrowConverter, SchemaMapper};
pub use destination::IcebergDestination;

// Conditionally export Supabase types
#[cfg(feature = "supabase-iceberg")]
pub use catalog::SupabaseRestCatalog;
