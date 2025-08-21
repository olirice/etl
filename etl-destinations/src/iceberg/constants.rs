//! Constants used throughout the Iceberg ETL implementation.

/// Default namespace for Iceberg tables.
pub const DEFAULT_NAMESPACE: &str = "etl";

/// Default table prefix for PostgreSQL tables.
pub const DEFAULT_TABLE_PREFIX: &str = "pg_";

/// CDC metadata column names for consistency across ETL destinations.
pub mod cdc_columns {
    /// Column indicating the type of change (INSERT, UPDATE, DELETE, UPSERT).
    pub const CHANGE_TYPE: &str = "_CHANGE_TYPE";

    /// Column containing the sequence number for ordering events.
    pub const CHANGE_SEQUENCE_NUMBER: &str = "_CHANGE_SEQUENCE_NUMBER";

    /// Column containing the timestamp when the change occurred.
    pub const CHANGE_TIMESTAMP: &str = "_CHANGE_TIMESTAMP";
}

/// CDC operation types.
pub mod cdc_operations {
    /// Insert operation.
    pub const INSERT: &str = "INSERT";

    /// Update operation.
    pub const UPDATE: &str = "UPDATE";

    /// Delete operation.
    pub const DELETE: &str = "DELETE";

    /// Upsert operation (used for initial table sync).
    pub const UPSERT: &str = "UPSERT";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_NAMESPACE, "etl");
        assert_eq!(DEFAULT_TABLE_PREFIX, "pg_");
        assert_eq!(cdc_columns::CHANGE_TYPE, "_CHANGE_TYPE");
        assert_eq!(cdc_operations::INSERT, "INSERT");
    }
}
