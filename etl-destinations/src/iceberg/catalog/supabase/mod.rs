//! Extended Iceberg catalog compatibility layer.
//!
//! This module provides compatibility with REST catalog implementations that
//! use variations of the standard Apache Iceberg REST API specification.
//!
//! # Purpose
//!
//! Bridges API specification differences between catalog providers while
//! maintaining a unified interface for client applications.

pub mod catalog;
pub mod client;

// Re-export main types
pub use catalog::{SupabaseRestCatalog, is_extended_compatibility_catalog};

// Keep old name for backward compatibility
pub use catalog::is_extended_compatibility_catalog as is_supabase_catalog;
pub use client::SupabaseHttpClient;
