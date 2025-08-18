//! Error handling utilities for Iceberg operations.
//!
//! This module provides error mapping functions to convert Iceberg-specific
//! errors into ETL framework errors with appropriate context and categorization.

use etl::error::{ErrorKind, EtlError};
use etl::etl_error;
use iceberg::Error as IcebergError;

/// Maps Iceberg errors to ETL errors with appropriate context.
///
/// Categorizes Iceberg errors into ETL error types for consistent error handling
/// across the destination implementation.
///
/// # Error Categories
///
/// - **Table errors**: Missing tables, invalid schemas
/// - **Network errors**: Connection failures, timeouts  
/// - **Permission errors**: Authentication and authorization failures
/// - **Data errors**: Invalid data, serialization failures
/// - **State errors**: Invalid operations, transaction conflicts
pub fn iceberg_error_to_etl_error(e: IcebergError) -> EtlError {
    let error_msg = e.to_string();

    // Categorize the error based on the message or error type
    let (kind, context) = match error_msg.as_str() {
        // Table and schema errors
        msg if msg.contains("table") && msg.contains("not found") => (
            ErrorKind::DestinationError,
            "Table not found in Iceberg catalog",
        ),
        msg if msg.contains("schema") || msg.contains("field") => {
            (ErrorKind::DestinationError, "Iceberg schema error")
        }

        // Network and connection errors
        msg if msg.contains("connection") || msg.contains("network") => {
            (ErrorKind::DestinationError, "Iceberg connection error")
        }
        msg if msg.contains("timeout") => {
            (ErrorKind::DestinationError, "Iceberg operation timeout")
        }

        // Permission errors
        msg if msg.contains("auth") || msg.contains("permission") || msg.contains("forbidden") => {
            (ErrorKind::DestinationError, "Iceberg permission denied")
        }

        // Data and serialization errors
        msg if msg.contains("serialize") || msg.contains("deserialize") => {
            (ErrorKind::DestinationIoError, "Iceberg serialization error")
        }
        msg if msg.contains("invalid") && msg.contains("data") => {
            (ErrorKind::DestinationError, "Invalid data for Iceberg")
        }

        // Writer and transaction errors
        msg if msg.contains("writer") => (ErrorKind::DestinationIoError, "Iceberg writer error"),
        msg if msg.contains("transaction") => {
            (ErrorKind::InvalidState, "Iceberg transaction error")
        }

        // Default case
        _ => (ErrorKind::DestinationError, "Iceberg operation failed"),
    };

    etl_error!(kind, context, error_msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_mapping_table_not_found() {
        let iceberg_err = IcebergError::new(
            iceberg::ErrorKind::DataInvalid,
            "table 'test_table' not found",
        );
        let etl_err = iceberg_error_to_etl_error(iceberg_err);
        assert!(matches!(etl_err.kind(), ErrorKind::DestinationError));
    }

    #[test]
    fn test_error_mapping_io_error() {
        let iceberg_err = IcebergError::new(
            iceberg::ErrorKind::DataInvalid,
            "writer failed to serialize data",
        );
        let etl_err = iceberg_error_to_etl_error(iceberg_err);
        assert!(matches!(etl_err.kind(), ErrorKind::DestinationIoError));
    }

    #[test]
    fn test_error_mapping_transaction_error() {
        let iceberg_err = IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            "transaction conflict detected",
        );
        let etl_err = iceberg_error_to_etl_error(iceberg_err);
        assert!(matches!(etl_err.kind(), ErrorKind::InvalidState));
    }

    #[test]
    fn test_error_mapping_default() {
        let iceberg_err =
            IcebergError::new(iceberg::ErrorKind::Unexpected, "some unexpected error");
        let etl_err = iceberg_error_to_etl_error(iceberg_err);
        assert!(matches!(etl_err.kind(), ErrorKind::DestinationError));
    }
}
