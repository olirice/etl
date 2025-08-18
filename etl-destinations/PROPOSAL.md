# Iceberg Integration PR Breakdown Proposal

## Overview

The current `or/iceberg` branch contains a comprehensive Iceberg destination implementation (~6,000 lines of code) that needs to be broken down into manageable, mergeable chunks. This proposal outlines a strategic breakdown that ensures each PR is testable, logical, and represents meaningful progress.

## Current Branch Analysis

- **Total Changes**: 23 files modified, 5,953 insertions, 10 deletions
- **Core Implementation**: 6 Rust modules (5,469 lines)
- **Test Coverage**: 855 lines of integration tests + 335 lines of common test utilities
- **Infrastructure**: CI/CD, Docker compose, benchmarks, configuration

## Proposed PR Breakdown

### PR 1: Foundation & Dependencies (Small - ~200 lines)
**Goal**: Establish the foundation without functionality

**Files**:
- `etl-destinations/Cargo.toml` (Iceberg feature flag and dependencies)
- `etl-config/src/shared/destination.rs` (Configuration support)
- `etl-destinations/src/lib.rs` (Module declaration)
- `etl-destinations/src/iceberg/mod.rs` (Module structure and documentation)

**Tests**: Unit tests for constants and module structure
**Merge Criteria**: Compiles cleanly, tests pass, no functionality exposed yet

### PR 2: Core Configuration & Error Handling (Small - ~300 lines)
**Goal**: Establish configuration and error handling patterns

**Files**:
- `src/iceberg/config.rs` (WriterConfig implementation)
- Error handling utilities in `src/iceberg/client.rs` (error mapping functions only)

**Tests**: Configuration serialization/deserialization, error mapping
**Merge Criteria**: Configuration types are complete and well-tested

### PR 3: Schema Mapping System (Medium - ~900 lines)
**Goal**: PostgreSQL to Iceberg schema conversion

**Files**:
- `src/iceberg/schema.rs` (Complete schema mapping implementation)

**Tests**: 
- PostgreSQL type to Arrow type conversions
- Schema evolution scenarios
- Edge cases for complex types (arrays, JSON, etc.)

**Merge Criteria**: All PostgreSQL types correctly map to Iceberg/Arrow equivalents

### PR 4: Data Encoding & Serialization (Medium - ~600 lines)
**Goal**: Row data conversion to Arrow format

**Files**:
- `src/iceberg/encoding.rs` (Arrow RecordBatch conversion)

**Tests**:
- Cell to Arrow value conversion
- Batch creation and validation
- Performance characteristics

**Merge Criteria**: Can successfully convert PostgreSQL rows to Arrow format

### PR 5: Iceberg Client Core (Large - ~1,000 lines)
**Goal**: Basic Iceberg operations without CDC

**Files**:
- `src/iceberg/client.rs` (Core client, table operations)
- Subset focused on table creation, basic writes

**Tests**:
- Table creation and management
- Basic data insertion
- Connection and authentication

**Merge Criteria**: Can create tables and perform basic writes to Iceberg

### PR 6: Destination Implementation & CDC (Large - ~800 lines)
**Goal**: Complete ETL destination with CDC support

**Files**:
- `src/iceberg/core.rs` (IcebergDestination implementation)

**Tests**:
- INSERT/UPDATE/DELETE operations
- Batching logic
- State management

**Merge Criteria**: Full CDC pipeline functional, passes destination trait tests

### PR 7: Integration Infrastructure (Medium - ~400 lines)
**Goal**: Docker, CI/CD, and integration test framework

**Files**:
- `docker-compose.test.yml`
- `.github/workflows/ci.yml` (Iceberg test integration)
- `Makefile` (Test commands)
- `tests/common/iceberg.rs` (Test utilities)

**Tests**: Docker environment setup, CI pipeline validation
**Merge Criteria**: Integration tests can run in CI environment

### PR 8: Comprehensive Integration Tests (Large - ~900 lines)
**Goal**: Full end-to-end test coverage

**Files**:
- `tests/integration/iceberg_tests.rs` (Complete test suite)

**Tests**:
- Live pipeline tests
- Schema evolution scenarios
- Error handling and recovery
- Performance benchmarks

**Merge Criteria**: All integration tests pass, covers major use cases

### PR 9: Replicator Integration & Performance (Small - ~200 lines)
**Goal**: Integration with replicator and benchmarks

**Files**:
- `etl-replicator/src/core.rs` (Iceberg destination support)
- `etl-benchmarks/benches/table_copies.rs` (Benchmarks)
- `scripts/benchmark.sh` (Updated benchmark scripts)

**Tests**: Replicator integration, performance benchmarks
**Merge Criteria**: Benchmarks show acceptable performance characteristics

## PR Dependencies

```
PR 1 (Foundation)
  ↓
PR 2 (Config)
  ↓
PR 3 (Schema) ← Independent from PR 4
  ↓           
PR 4 (Encoding) ← Independent from PR 3
  ↓
PR 5 (Client) ← Depends on PR 3 & 4
  ↓
PR 6 (Destination) ← Depends on PR 5
  ↓
PR 7 (Infrastructure) ← Can be parallel with PR 6
  ↓
PR 8 (Tests) ← Depends on PR 6 & 7
  ↓
PR 9 (Integration) ← Depends on PR 8
```

## Key Benefits of This Approach

1. **Incremental Value**: Each PR adds meaningful functionality
2. **Testability**: Each component can be thoroughly tested in isolation
3. **Reviewability**: Manageable PR sizes (200-1000 lines each)
4. **Rollback Safety**: Any PR can be reverted without breaking others
5. **Parallel Development**: Some PRs can be developed concurrently
6. **Clear Interfaces**: Each PR establishes clear contracts between components

## Testing Strategy

- **Unit Tests**: Each PR includes comprehensive unit tests
- **Integration Points**: Test interfaces between components at PR boundaries
- **Smoke Tests**: Each PR includes basic functionality validation
- **Performance Gates**: PR 5, 6, and 9 include performance validation
- **End-to-End**: Final validation in PR 8 with complete pipeline tests

## Risk Mitigation

- **Early Integration**: PRs 1-2 establish foundation without risk
- **Component Isolation**: Schema and encoding can be developed/tested independently
- **Incremental Complexity**: Each PR builds on proven foundation
- **Rollback Strategy**: Clean revert path at any point
- **Performance Validation**: Multiple checkpoints ensure performance requirements are met

This breakdown transforms a risky 6K-line PR into 9 manageable, logical, and testable increments that each represent meaningful progress toward the complete Iceberg integration.