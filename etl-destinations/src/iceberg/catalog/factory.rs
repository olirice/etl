//! Unified catalog factory that auto-detects and creates the appropriate catalog type.
//!
//! This module provides a single code path for creating Iceberg catalogs,
//! automatically detecting whether to use standard REST catalog or Supabase-specific
//! implementation based on the URI.

use iceberg::{Catalog, Error as IcebergError};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

#[cfg(feature = "supabase-iceberg")]
use crate::iceberg::catalog::supabase::{SupabaseRestCatalog, is_extended_compatibility_catalog};

/// Creates the appropriate catalog implementation based on the URI.
///
/// This function automatically detects whether the catalog URI points to:
/// - Extended compatibility catalogs (Supabase Storage, other REST API variations)
/// - Standard Iceberg REST catalog (MinIO, Tabular, etc.)
///
/// # Arguments
/// * `catalog_uri` - The URI of the catalog server
/// * `warehouse` - The warehouse location (S3 bucket, filesystem path, etc.)
/// * `auth_token` - Optional authentication token
///
/// # Returns
/// A boxed Catalog trait object that works with both implementations
pub async fn create_catalog(
    catalog_uri: String,
    warehouse: String,
    auth_token: Option<String>,
) -> Result<Arc<dyn Catalog>, IcebergError> {
    // Check if this catalog requires extended compatibility mode
    #[cfg(feature = "supabase-iceberg")]
    {
        if is_extended_compatibility_catalog(&catalog_uri) {
            info!(
                catalog_uri = %catalog_uri,
                "Detected REST API variations - using extended compatibility layer"
            );

            // Extended compatibility mode requires auth token
            let token = auth_token.ok_or_else(|| {
                IcebergError::from(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Extended catalog compatibility requires authentication token",
                ))
            })?;

            let catalog = SupabaseRestCatalog::new(catalog_uri, warehouse, token).await?;

            return Ok(Arc::new(catalog) as Arc<dyn Catalog>);
        }
    }

    // Standard Iceberg REST catalog
    info!(
        catalog_uri = %catalog_uri,
        "Using standard Iceberg REST catalog"
    );

    let mut props = HashMap::new();
    if let Some(token) = auth_token {
        debug!("Adding Bearer token authentication to REST catalog");
        props.insert("token".to_string(), token);
    }

    // Configure S3 for Supabase storage - needs BOTH token AND S3 credentials
    if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
        props.insert("s3.access-key-id".to_string(), access_key);
        debug!("Added S3 access key ID to catalog configuration");
    }

    if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        props.insert("s3.secret-access-key".to_string(), secret_key);
        debug!("Added S3 secret access key to catalog configuration");
    }

    if let Ok(s3_endpoint) = std::env::var("S3_ENDPOINT") {
        props.insert("s3.endpoint".to_string(), s3_endpoint.clone());
        debug!(
            "Added S3 endpoint to catalog configuration: {}",
            s3_endpoint
        );
    }

    // Disable path-style access (use virtual-hosted-style like in Python example)
    props.insert(
        "s3.force-virtual-addressing".to_string(),
        "false".to_string(),
    );

    // Set region (required by S3 protocol)
    props.insert("s3.region".to_string(), "us-east-1".to_string());

    let config = RestCatalogConfig::builder()
        .uri(catalog_uri)
        .warehouse(warehouse)
        .props(props)
        .build();

    Ok(Arc::new(RestCatalog::new(config)) as Arc<dyn Catalog>)
}

/// Configuration for catalog creation
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    pub uri: String,
    pub warehouse: String,
    pub auth_token: Option<String>,
    /// Force a specific catalog type instead of auto-detection
    pub force_type: Option<CatalogType>,
    /// Force extended compatibility mode even for standard catalogs
    pub force_extended_compatibility: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogType {
    /// Standard Iceberg REST catalog
    Standard,
    /// Supabase Storage API with compatibility layer
    #[cfg(feature = "supabase-iceberg")]
    Supabase,
}

impl CatalogConfig {
    /// Creates a new catalog configuration with auto-detection
    pub fn new(uri: String, warehouse: String) -> Self {
        Self {
            uri,
            warehouse,
            auth_token: None,
            force_type: None,
            force_extended_compatibility: false,
        }
    }

    /// Sets the authentication token
    pub fn with_auth_token(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }

    /// Forces a specific catalog type, overriding auto-detection
    pub fn with_catalog_type(mut self, catalog_type: CatalogType) -> Self {
        self.force_type = Some(catalog_type);
        self
    }

    /// Forces extended compatibility mode even for standard catalogs
    /// This allows using the same implementation for both Supabase and MinIO
    pub fn with_extended_compatibility(mut self) -> Self {
        self.force_extended_compatibility = true;
        self
    }

    /// Creates the appropriate catalog based on configuration
    pub async fn build(self) -> Result<Arc<dyn Catalog>, IcebergError> {
        match self.force_type {
            #[cfg(feature = "supabase-iceberg")]
            Some(CatalogType::Supabase) => {
                info!("Forced Supabase catalog type");
                let token = self.auth_token.ok_or_else(|| {
                    IcebergError::from(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Supabase catalog requires authentication token",
                    ))
                })?;

                let catalog = SupabaseRestCatalog::new(self.uri, self.warehouse, token).await?;

                Ok(Arc::new(catalog) as Arc<dyn Catalog>)
            }
            Some(CatalogType::Standard) => {
                info!("Forced standard catalog type");
                let mut props = HashMap::new();
                if let Some(token) = self.auth_token {
                    props.insert("token".to_string(), token);
                }

                let config = RestCatalogConfig::builder()
                    .uri(self.uri)
                    .warehouse(self.warehouse)
                    .props(props)
                    .build();

                Ok(Arc::new(RestCatalog::new(config)) as Arc<dyn Catalog>)
            }
            None => {
                // Auto-detect based on URI or force extended compatibility
                if self.force_extended_compatibility {
                    info!("Forcing extended compatibility mode");
                    #[cfg(feature = "supabase-iceberg")]
                    {
                        let token = self.auth_token.unwrap_or_else(|| {
                            warn!("No auth token provided for extended compatibility mode");
                            String::new()
                        });

                        let catalog =
                            SupabaseRestCatalog::new(self.uri, self.warehouse, token).await?;

                        Ok(Arc::new(catalog) as Arc<dyn Catalog>)
                    }
                    #[cfg(not(feature = "supabase-iceberg"))]
                    {
                        Err(IcebergError::from(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Extended compatibility mode requires 'supabase-iceberg' feature",
                        )))
                    }
                } else {
                    // Auto-detect based on URI
                    create_catalog(self.uri, self.warehouse, self.auth_token).await
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_config_builder() {
        let config = CatalogConfig::new(
            "https://catalog.example.com".to_string(),
            "s3://bucket/warehouse".to_string(),
        )
        .with_auth_token("token123".to_string());

        assert_eq!(config.uri, "https://catalog.example.com");
        assert_eq!(config.warehouse, "s3://bucket/warehouse");
        assert_eq!(config.auth_token, Some("token123".to_string()));
        assert_eq!(config.force_type, None);
    }

    #[cfg(feature = "supabase-iceberg")]
    #[test]
    fn test_force_catalog_type() {
        let config = CatalogConfig::new(
            "https://localhost:8080".to_string(),
            "warehouse".to_string(),
        )
        .with_catalog_type(CatalogType::Supabase);

        assert_eq!(config.force_type, Some(CatalogType::Supabase));
    }
}
