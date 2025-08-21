//! Iceberg client implementations for ETL operations.
//!
//! This module provides the main IcebergClient for interacting with Iceberg catalogs
//! and performing table operations like creation, writing, and schema management.

pub mod iceberg;

// Re-export main types
pub use iceberg::{IcebergClient, IcebergOperationType, PagedTableResult};
