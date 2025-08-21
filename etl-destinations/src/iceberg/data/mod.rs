//! Data processing and transformation for Iceberg tables.
//!
//! This module handles schema mapping, data encoding, and conversion between
//! PostgreSQL types and Iceberg/Arrow formats.

pub mod encoding;
pub mod schema;

// Re-export main types for convenience
pub use encoding::rows_to_record_batch;
pub use schema::{CellToArrowConverter, SchemaMapper};
