//! Iceberg catalog implementations and factories.
//!
//! This module provides unified access to different Iceberg catalog implementations,
//! including standard REST catalogs and Supabase-specific compatibility layers.

pub mod factory;

// Supabase-specific catalog modules (feature-gated)
#[cfg(feature = "supabase-iceberg")]
pub mod supabase;

// Re-export for convenience
pub use factory::{CatalogConfig, CatalogType, create_catalog};

#[cfg(feature = "supabase-iceberg")]
pub use supabase::SupabaseRestCatalog;
