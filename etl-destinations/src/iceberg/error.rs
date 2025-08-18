//! Error handling utilities for Iceberg operations.
//!
//! This module provides error mapping functions to convert Iceberg-specific
//! errors into ETL framework errors with appropriate context and categorization.

use etl::error::{ErrorKind, EtlError};

/// Error handling for Iceberg operations is implemented in client.rs
/// This module serves as a placeholder for future error-related utilities.

#[cfg(test)]
mod tests {
    use super::*;
    use etl::etl_error;
    
    #[test]
    fn test_error_creation() {
        let err = etl_error!(
            ErrorKind::DestinationError,
            "Test error",
            "Test details".to_string()
        );
        assert!(matches!(err.kind(), ErrorKind::DestinationError));
    }
}