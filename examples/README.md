# Examples

This directory contains example implementations and benchmarks for the ETL pipeline.

## Available Examples

### Supabase Iceberg Integration (`supabase_iceberg/`)

Examples for using both Supabase and standard Iceberg catalogs with automatic detection:

- **e2e_supabase_pipeline**: End-to-end example demonstrating PostgreSQL to Iceberg replication
- **comprehensive_throughput_benchmark**: Performance comparison between local, hybrid, and remote configurations  
- **iceberg**: Basic Iceberg integration example
- **cdc_pipeline_demo**: Change Data Capture demonstration

The examples automatically detect whether you're using:
- **Supabase Storage** (URLs containing `supabase.co` or `supabase.com`)
- **Standard Iceberg** (MinIO, Tabular, AWS Glue, etc.)

### Quick Start

1. Copy the environment template:
   ```bash
   cd examples/supabase_iceberg
   cp .env.template .env
   ```

2. Configure your credentials in `.env`:
   - For **Supabase**: Set `CATALOG_URI`, `WAREHOUSE`, `TOKEN`
   - For **Standard Iceberg**: Set local MinIO or cloud provider credentials

3. Run an example:
   ```bash
   # Works with both Supabase and standard catalogs
   cargo run --example e2e_supabase_pipeline --features etl-destinations/supabase-iceberg
   ```

4. For performance testing:
   ```bash
   # Start local Docker infrastructure first
   docker-compose -f ../../etl-destinations/docker-compose.test.yml up -d
   
   # Run comprehensive benchmarks
   cargo run --example comprehensive_throughput_benchmark --features etl-destinations/supabase-iceberg
   ```

## Prerequisites

- PostgreSQL database with logical replication enabled (`wal_level = logical`)
- **For Supabase**: Project with Storage API access and service role token
- **For Standard Iceberg**: Compatible catalog (MinIO, Tabular, AWS Glue, etc.)

For local testing, Docker and Docker Compose are required.

See individual example files and `.env.template` for detailed configuration instructions.