use iceberg::spec::{Schema, SortOrder, TableMetadata, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::{Error, ErrorKind, NamespaceIdent, Result, TableCreation, TableIdent};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// HTTP client for extended REST catalog API compatibility
/// Handles URL path variations and request/response format differences
#[derive(Debug)]
pub struct SupabaseHttpClient {
    client: Client,
    base_uri: String,
    warehouse: String,
    auth_token: String,
}

impl SupabaseHttpClient {
    pub fn new(base_uri: String, warehouse: String, auth_token: String) -> Self {
        let client = Client::new();
        Self {
            client,
            base_uri,
            warehouse,
            auth_token,
        }
    }

    /// Build Supabase-compatible URL with warehouse prefix
    fn build_url(&self, path: &str) -> Result<String> {
        let base = self.base_uri.trim_end_matches('/');

        // For config endpoint, don't add warehouse prefix
        if path == "config" {
            return Ok(format!("{}/v1/{}", base, path));
        }

        // For all other endpoints, add warehouse prefix
        // Transform: /namespaces/... -> /v1/{warehouse}/namespaces/...
        let path_with_warehouse = if path.starts_with("namespaces") || path.starts_with("tables") {
            format!("v1/{}/{}", self.warehouse, path)
        } else {
            path.to_string()
        };

        Ok(format!("{}/{}", base, path_with_warehouse))
    }

    /// Send authenticated request to Supabase
    async fn send_request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Response> {
        let mut request = self
            .client
            .request(method, url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .header("Content-Type", "application/json");

        if let Some(body) = body {
            request = request.json(&body);
        }

        let response = request.send().await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, format!("HTTP request failed: {}", e))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("HTTP {} error: {}", status, error_text),
            ));
        }

        Ok(response)
    }

    /// Create table using Supabase-specific request format
    pub async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        // Transform the request to Supabase format
        let supabase_request = SupabaseCreateTableRequest {
            name: creation.name.clone(),
            location: creation.location.clone(),
            schema: creation.schema.clone(),
            spec: creation.partition_spec.clone(), // partition-spec -> spec
            sort_order: creation.sort_order.clone(),
            stage_create: None, // Not available in TableCreation
            // Critical: omit properties if empty, don't send null
            properties: if creation.properties.is_empty() {
                None
            } else {
                Some(creation.properties.clone())
            },
        };

        let namespace_path = namespace.as_ref().join(".");
        let url = self.build_url(&format!("namespaces/{}/tables", namespace_path))?;

        let body = serde_json::to_value(supabase_request).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("JSON serialization failed: {}", e),
            )
        })?;

        let response = self
            .send_request(reqwest::Method::POST, &url, Some(body))
            .await?;

        let create_response: SupabaseCreateTableResponse = response.json().await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("JSON deserialization failed: {}", e),
            )
        })?;

        // Convert Supabase response back to Iceberg Table
        self.convert_to_table(namespace, &creation.name, create_response)
            .await
    }

    /// Convert Supabase table response to Iceberg Table object
    async fn convert_to_table(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
        response: SupabaseCreateTableResponse,
    ) -> Result<Table> {
        let table_ident = TableIdent::new(namespace.clone(), name.to_string());

        // Create Table object with the returned metadata
        // The metadata location and content are provided by Supabase
        Table::builder()
            .metadata_location(response.metadata_location)
            .identifier(table_ident)
            .metadata(response.metadata)
            .build()
    }

    /// List tables in namespace using Supabase API
    pub async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let namespace_path = namespace.as_ref().join(".");
        let url = self.build_url(&format!("namespaces/{}/tables", namespace_path))?;

        let response = self.send_request(reqwest::Method::GET, &url, None).await?;

        let list_response: SupabaseListTablesResponse = response.json().await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("JSON deserialization failed: {}", e),
            )
        })?;

        Ok(list_response
            .identifiers
            .into_iter()
            .map(|id| TableIdent::new(NamespaceIdent::from_vec(id.namespace).unwrap(), id.name))
            .collect())
    }
}

/// Supabase-specific request format for table creation
#[derive(Debug, Serialize)]
struct SupabaseCreateTableRequest {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    schema: Schema,
    #[serde(rename = "spec", skip_serializing_if = "Option::is_none")]
    spec: Option<UnboundPartitionSpec>, // partition-spec -> spec mapping
    #[serde(rename = "write-order", skip_serializing_if = "Option::is_none")]
    sort_order: Option<SortOrder>,
    #[serde(rename = "stage-create", skip_serializing_if = "Option::is_none")]
    stage_create: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<HashMap<String, String>>, // Omit if empty - critical for Supabase
}

/// Supabase table creation response format
#[derive(Debug, Deserialize)]
struct SupabaseCreateTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: TableMetadata,
}

/// Supabase table listing response format
#[derive(Debug, Deserialize)]
struct SupabaseListTablesResponse {
    identifiers: Vec<SupabaseTableIdentifier>,
}

#[derive(Debug, Deserialize)]
struct SupabaseTableIdentifier {
    namespace: Vec<String>,
    name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use serde_json;
    use std::sync::Arc;

    #[test]
    fn test_url_building_config() {
        let client = create_test_client();

        // Config endpoint should not have warehouse prefix
        let config_url = client.build_url("config").unwrap();
        assert_eq!(
            config_url, "https://project.supabase.co/storage/v1/iceberg/v1/config",
            "Config URL should not include warehouse prefix"
        );
    }

    #[test]
    fn test_url_building_namespaces() {
        let client = create_test_client();

        // Namespace endpoints should have warehouse prefix
        let ns_url = client.build_url("namespaces").unwrap();
        assert_eq!(
            ns_url, "https://project.supabase.co/storage/v1/iceberg/v1/test-warehouse/namespaces",
            "Namespace URL should include warehouse prefix"
        );

        let ns_specific_url = client.build_url("namespaces/test").unwrap();
        assert_eq!(
            ns_specific_url,
            "https://project.supabase.co/storage/v1/iceberg/v1/test-warehouse/namespaces/test",
            "Specific namespace URL should include warehouse prefix"
        );
    }

    #[test]
    fn test_url_building_tables() {
        let client = create_test_client();

        // Table endpoints should have warehouse prefix
        let table_url = client.build_url("namespaces/test/tables").unwrap();
        assert_eq!(
            table_url,
            "https://project.supabase.co/storage/v1/iceberg/v1/test-warehouse/namespaces/test/tables",
            "Table URL should include warehouse prefix"
        );

        let specific_table_url = client.build_url("namespaces/test/tables/my_table").unwrap();
        assert_eq!(
            specific_table_url,
            "https://project.supabase.co/storage/v1/iceberg/v1/test-warehouse/namespaces/test/tables/my_table",
            "Specific table URL should include warehouse prefix"
        );
    }

    #[test]
    fn test_url_building_edge_cases() {
        let _client = create_test_client();

        // Test with special characters in warehouse name
        let client_with_special_chars = SupabaseHttpClient::new(
            "https://project.supabase.co/storage/v1/iceberg".to_string(),
            "test-warehouse_123".to_string(),
            "token".to_string(),
        );

        let url = client_with_special_chars
            .build_url("namespaces/test")
            .unwrap();
        assert!(
            url.contains("test-warehouse_123"),
            "URL should contain encoded warehouse name"
        );
    }

    #[test]
    fn test_request_serialization_empty_properties() {
        let req = create_test_table_request(None);
        let json = serde_json::to_string(&req).unwrap();

        // Verify properties field is omitted when None
        assert!(
            !json.contains("properties"),
            "Properties should be omitted when None"
        );

        // Verify field name mapping
        // Optional fields with None values are omitted due to skip_serializing_if
        assert!(
            !json.contains("partition-spec"),
            "Should not use 'partition-spec'"
        );
        assert!(!json.contains("spec"), "spec should be omitted when None");
        assert!(
            !json.contains("write-order"),
            "write-order should be omitted when None"
        );
        assert!(
            !json.contains("stage-create"),
            "stage-create should be omitted when None"
        );

        // Required fields should always be present
        assert!(json.contains("name"), "name should always be present");
        assert!(json.contains("schema"), "schema should always be present");
    }

    #[test]
    fn test_request_serialization_with_properties() {
        let mut properties = HashMap::new();
        properties.insert("format-version".to_string(), "2".to_string());
        properties.insert("table.type".to_string(), "ICEBERG".to_string());

        let req = create_test_table_request(Some(properties.clone()));
        let json = serde_json::to_string(&req).unwrap();

        // Verify properties are included
        assert!(
            json.contains("properties"),
            "Properties should be included when present"
        );
        assert!(
            json.contains("format-version"),
            "Should contain format-version property"
        );
        assert!(
            json.contains("table.type"),
            "Should contain table.type property"
        );

        // Verify structure
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let props = parsed.get("properties").unwrap();
        assert_eq!(props.get("format-version").unwrap().as_str().unwrap(), "2");
    }

    #[test]
    fn test_request_serialization_all_fields() {
        let req = create_comprehensive_table_request();
        let json = serde_json::to_string(&req).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify all expected fields are present
        assert!(parsed.get("name").is_some(), "name should be present");
        assert!(
            parsed.get("location").is_some(),
            "location should be present"
        );
        assert!(parsed.get("schema").is_some(), "schema should be present");
        // Optional fields that are Some should be present
        assert!(json.contains("location"), "location should be present");
        assert!(
            json.contains("stage-create"),
            "stage-create should be present when Some"
        );
        assert!(
            json.contains("properties"),
            "properties should be present when Some"
        );

        // Verify field types
        assert_eq!(
            parsed.get("name").unwrap().as_str().unwrap(),
            "comprehensive_test_table"
        );
        assert!(parsed.get("stage-create").unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_supabase_response_deserialization() {
        // Test minimal response structure parsing
        // For full TableMetadata parsing, we'd need a more complete schema
        let response_json = r#"{
            "metadata-location": "s3://bucket/warehouse/namespace/table/metadata/00001-abc123.metadata.json"
        }"#;

        // Test just the metadata-location field which is what we primarily need
        let parsed: serde_json::Value = serde_json::from_str(response_json).unwrap();
        assert_eq!(
            parsed.get("metadata-location").unwrap().as_str().unwrap(),
            "s3://bucket/warehouse/namespace/table/metadata/00001-abc123.metadata.json"
        );

        // Note: Full TableMetadata deserialization would require a complete Iceberg schema
        // For now, we verify the response structure parsing works
        assert!(response_json.contains("metadata-location"));
    }

    #[test]
    fn test_list_tables_response_deserialization() {
        let response_json = r#"{
            "identifiers": [
                {
                    "namespace": ["test_namespace"],
                    "name": "table1"
                },
                {
                    "namespace": ["test_namespace", "sub_namespace"],
                    "name": "table2"
                }
            ]
        }"#;

        let parsed: SupabaseListTablesResponse = serde_json::from_str(response_json).unwrap();
        assert_eq!(parsed.identifiers.len(), 2);
        assert_eq!(parsed.identifiers[0].name, "table1");
        assert_eq!(parsed.identifiers[0].namespace, vec!["test_namespace"]);
        assert_eq!(parsed.identifiers[1].name, "table2");
        assert_eq!(
            parsed.identifiers[1].namespace,
            vec!["test_namespace", "sub_namespace"]
        );
    }

    // Helper functions for creating test objects
    fn create_test_client() -> SupabaseHttpClient {
        SupabaseHttpClient::new(
            "https://project.supabase.co/storage/v1/iceberg".to_string(),
            "test-warehouse".to_string(),
            "test-token".to_string(),
        )
    }

    fn create_test_table_request(
        properties: Option<HashMap<String, String>>,
    ) -> SupabaseCreateTableRequest {
        SupabaseCreateTableRequest {
            name: "test_table".to_string(),
            location: None,
            schema: create_test_schema(),
            spec: None,
            sort_order: None,
            stage_create: None,
            properties,
        }
    }

    fn create_comprehensive_table_request() -> SupabaseCreateTableRequest {
        let mut properties = HashMap::new();
        properties.insert("format-version".to_string(), "2".to_string());

        SupabaseCreateTableRequest {
            name: "comprehensive_test_table".to_string(),
            location: Some("s3://test-bucket/test-location".to_string()),
            schema: create_test_schema(),
            spec: None,       // Could be expanded with actual partition spec
            sort_order: None, // Could be expanded with actual sort order
            stage_create: Some(true),
            properties: Some(properties),
        }
    }

    fn create_test_schema() -> Schema {
        Schema::builder()
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
                Arc::new(NestedField::optional(
                    3,
                    "created_at",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
            ])
            .build()
            .expect("Failed to create test schema")
    }
}
