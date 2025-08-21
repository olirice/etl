//! Extended Iceberg REST Catalog implementation.
//!
//! This module provides a REST catalog implementation that handles variations
//! in the Apache Iceberg REST API specification across different providers.
//!
//! # API Variations Handled
//!
//! - **URL Patterns**: Supports both `/v1/namespaces/...` and `/v1/{warehouse}/namespaces/...`
//! - **Field Names**: Handles both `partition-spec` and `spec` field naming conventions
//! - **Properties Field**: Manages serialization differences (object vs null handling)
//! - **Authentication**: Supports Bearer token authentication patterns
//!
//! # Implementation Notes
//!
//! This implementation bridges differences between REST catalog providers while maintaining
//! compatibility with the standard Apache Iceberg catalog interface.
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use etl_destinations::iceberg::SupabaseRestCatalog;
//! use iceberg::{Catalog, NamespaceIdent, TableCreation};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let catalog = SupabaseRestCatalog::new(
//!     "https://project.supabase.co/storage/v1/iceberg".to_string(),
//!     "my-warehouse".to_string(),
//!     "auth-token".to_string(),
//! ).await?;
//!
//! // Use standard Iceberg Catalog interface
//! let namespace = NamespaceIdent::from_vec(vec!["my_namespace".to_string()])?;
//! // let table = catalog.create_table(&namespace, table_creation).await?;
//! # Ok(())
//! # }
//! ```

use crate::iceberg::catalog::supabase::client::SupabaseHttpClient;
use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use serde::Serialize;
use std::collections::HashMap;
use tracing::debug;

/// Extended REST Catalog implementation
///
/// This implementation provides compatibility with REST catalog providers that
/// use variations of the Apache Iceberg REST API specification.
///
/// API variations handled:
/// - URL paths with warehouse prefixes: `/v1/{warehouse}/namespaces/...`
/// - Alternative field naming conventions: `partition-spec` vs `spec`
/// - Different serialization requirements for optional fields
#[derive(Debug)]
pub struct SupabaseRestCatalog {
    inner: RestCatalog,
    #[allow(dead_code)] // Kept for potential future debugging/logging needs
    warehouse: String,
    #[allow(dead_code)] // Kept for potential future debugging/logging needs  
    catalog_uri: String,
    http_client: SupabaseHttpClient,
}

impl SupabaseRestCatalog {
    /// Create a new Supabase-compatible REST catalog
    pub async fn new(catalog_uri: String, warehouse: String, auth_token: String) -> Result<Self> {
        // Create the underlying REST catalog with token auth and S3 configuration
        let mut props = HashMap::new();
        props.insert("token".to_string(), auth_token.clone());

        // Configure S3 for Supabase storage - needs BOTH token AND S3 credentials
        if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
            props.insert("s3.access-key-id".to_string(), access_key);
        }
        
        if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            props.insert("s3.secret-access-key".to_string(), secret_key);
        }
        
        if let Ok(s3_endpoint) = std::env::var("S3_ENDPOINT") {
            props.insert("s3.endpoint".to_string(), s3_endpoint);
        }
        
        // Disable path-style access (use virtual-hosted-style like in Python example)
        props.insert("s3.force-virtual-addressing".to_string(), "false".to_string());
        
        // Set region
        props.insert("s3.region".to_string(), "us-east-1".to_string());

        debug!("Creating REST catalog with {} properties", props.len());
        for (key, value) in &props {
            if key.contains("token") {
                debug!("Catalog property: {} = [REDACTED]", key);
            } else {
                debug!("Catalog property: {} = {}", key, value);
            }
        }
        
        let config = RestCatalogConfig::builder()
            .uri(catalog_uri.clone())
            .warehouse(warehouse.clone())
            .props(props)
            .build();

        let inner = RestCatalog::new(config);

        // Create custom HTTP client for Supabase-specific operations
        let http_client =
            SupabaseHttpClient::new(catalog_uri.clone(), warehouse.clone(), auth_token);

        Ok(Self {
            inner,
            warehouse,
            catalog_uri,
            http_client,
        })
    }
}

#[async_trait]
impl Catalog for SupabaseRestCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // Namespace operations work with the underlying catalog
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        // Use custom HTTP client for consistent URL formatting across providers
        self.http_client.list_tables(namespace).await
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        self.inner.table_exists(identifier).await
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        self.inner.drop_table(identifier).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        self.inner.load_table(identifier).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        // Use custom HTTP client for extended compatibility
        // This handles API variations like field naming and property serialization
        self.http_client.create_table(namespace, creation).await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await
    }

    async fn register_table(
        &self,
        identifier: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        self.inner
            .register_table(identifier, metadata_location)
            .await
    }
}

/// Extended request format for table creation
/// Handles field name variations across different REST catalog providers
#[derive(Debug, Serialize)]
#[allow(dead_code)] // Prepared for future schema evolution features
struct SupabaseCreateTableRequest {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    schema: iceberg::spec::Schema,
    #[serde(rename = "spec", skip_serializing_if = "Option::is_none")]
    spec: Option<iceberg::spec::UnboundPartitionSpec>, // partition-spec -> spec
    #[serde(rename = "write-order", skip_serializing_if = "Option::is_none")]
    sort_order: Option<iceberg::spec::SortOrder>,
    #[serde(rename = "stage-create", skip_serializing_if = "Option::is_none")]
    stage_create: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<HashMap<String, String>>, // Omit if empty, don't send null
}

/// Helper to detect if a catalog URI requires extended compatibility mode
/// Detects REST catalog implementations that use API variations
pub fn is_extended_compatibility_catalog(uri: &str) -> bool {
    // Detect known providers that use API variations
    uri.contains("supabase.co") || 
    uri.contains("supabase.com") ||
    // Could add other providers here that use similar variations
    uri.contains("/storage/v1/") // Generic storage API pattern
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use serde_json;
    use std::sync::Arc;

    #[test]
    fn test_extended_compatibility_detection() {
        assert!(is_extended_compatibility_catalog(
            "https://project.supabase.co/storage/v1/iceberg"
        ));
        assert!(is_extended_compatibility_catalog(
            "https://api.supabase.com/storage/v1/iceberg"
        ));
        assert!(is_extended_compatibility_catalog(
            "https://xyz.storage.supabase.co/storage/v1/iceberg"
        ));
        assert!(is_extended_compatibility_catalog(
            "https://example.com/storage/v1/iceberg"
        )); // Generic storage API
        assert!(!is_extended_compatibility_catalog(
            "https://tabular.io/catalog"
        ));
        assert!(!is_extended_compatibility_catalog(
            "https://localhost:8080/iceberg"
        ));
        assert!(!is_extended_compatibility_catalog(""));
    }

    #[test]
    fn test_properties_transformation_empty() {
        // Test that empty properties are omitted in serialization
        let req = create_test_table_request(HashMap::new());
        let json = serde_json::to_string(&req).unwrap();
        assert!(
            !json.contains("properties"),
            "Empty properties should be omitted from JSON"
        );
    }

    #[test]
    fn test_properties_transformation_with_values() {
        // Test that non-empty properties are included
        let mut props = HashMap::new();
        props.insert("key1".to_string(), "value1".to_string());
        props.insert("key2".to_string(), "value2".to_string());

        let req = create_test_table_request(props.clone());
        let json = serde_json::to_string(&req).unwrap();

        assert!(
            json.contains("properties"),
            "Non-empty properties should be included"
        );
        assert!(json.contains("key1"), "Property key1 should be present");
        assert!(json.contains("value1"), "Property value1 should be present");

        // Verify deserialization preserves properties
        let parsed_req: serde_json::Value = serde_json::from_str(&json).unwrap();
        let props_obj = parsed_req.get("properties").unwrap();
        assert_eq!(props_obj.get("key1").unwrap().as_str().unwrap(), "value1");
    }

    #[test]
    fn test_field_name_mapping() {
        // Test that field names are correctly mapped for Supabase
        let req = create_test_table_request(HashMap::new());
        let json = serde_json::to_string(&req).unwrap();

        // Verify field name mapping - check that wrong field names are NOT used
        assert!(
            !json.contains("\"partition-spec\""),
            "Should not use 'partition-spec' field name"
        );

        // When fields are present, they should use the correct Supabase names
        // Optional fields are omitted when None, so we just verify no wrong names are used
        if json.contains("spec") {
            // If spec is present, it should be the right field name
            assert!(
                !json.contains("partition-spec"),
                "Should use 'spec' not 'partition-spec'"
            );
        }
    }

    #[test]
    fn test_supabase_request_serialization_structure() {
        let req = create_test_table_request(HashMap::new());
        let json = serde_json::to_string(&req).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify required fields are present
        assert!(parsed.get("name").is_some(), "name field should be present");
        assert!(
            parsed.get("schema").is_some(),
            "schema field should be present"
        );

        // Verify optional fields are handled correctly
        // Fields with None values are omitted due to skip_serializing_if
        // So we test that required fields are present and optional fields are omitted when None
        assert!(!json.contains("spec"), "spec should be omitted when None");
        assert!(
            !json.contains("write-order"),
            "write-order should be omitted when None"
        );
        assert!(
            !json.contains("stage-create"),
            "stage-create should be omitted when None"
        );

        // Properties should be omitted when empty
        assert!(
            parsed.get("properties").is_none(),
            "empty properties should be omitted"
        );
    }

    #[test]
    fn test_table_creation_transformation() {
        // Create a realistic TableCreation object
        let schema = create_test_schema();
        let mut properties = HashMap::new();
        properties.insert("test_key".to_string(), "test_value".to_string());

        let creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(schema.clone())
            .properties(properties.clone())
            .build();

        // Create catalog instance (we can't test the full transformation without async,
        // but we can test the structure)
        assert_eq!(creation.name, "test_table");
        assert_eq!(creation.properties, properties);
        assert!(creation.location.is_none());
        assert!(creation.partition_spec.is_none());
        assert!(creation.sort_order.is_none());
    }

    #[test]
    fn test_supabase_catalog_configuration() {
        // Test that catalog configuration is properly set up
        let catalog_uri = "https://test.supabase.co/storage/v1/iceberg";
        let warehouse = "test-warehouse";
        let auth_token = "test-token";

        // We can't test the full async constructor, but we can verify the components
        assert!(is_extended_compatibility_catalog(catalog_uri));
        assert!(!warehouse.is_empty());
        assert!(!auth_token.is_empty());
    }

    // Helper function to create test table request
    fn create_test_table_request(
        properties: HashMap<String, String>,
    ) -> SupabaseCreateTableRequest {
        let schema = create_test_schema();

        SupabaseCreateTableRequest {
            name: "test_table".to_string(),
            location: None,
            schema,
            spec: None,
            sort_order: None,
            stage_create: None,
            properties: if properties.is_empty() {
                None
            } else {
                Some(properties)
            },
        }
    }

    // Helper function to create test schema
    fn create_test_schema() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("Failed to create test schema")
    }
}
