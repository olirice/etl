# Supabase Iceberg ETL Pipeline Example

This example demonstrates a real-world ETL pipeline that connects remote PostgreSQL (Supabase) to remote Iceberg storage using Supabase's Storage API.

## Overview

This CDC pipeline example demonstrates how to:
- Set up PostgreSQL logical replication with a publication
- Create a real orders table in PostgreSQL 
- Stream CDC changes from PostgreSQL to Supabase Iceberg in real-time
- Handle all CDC operations (INSERT, UPDATE, DELETE, TRUNCATE) automatically
- Monitor pipeline performance and status

## Example

### `supabase_pipeline`
Production-ready CDC pipeline demonstrating real-time PostgreSQL to Supabase Iceberg replication.

```bash
cargo run --example supabase_pipeline
```

## Setup

### Environment Configuration

Create a `.env` file in this directory with your Supabase credentials:

```bash
# Supabase PostgreSQL connection
POSTGRES_DB=postgresql://postgres.project:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres

# Supabase Iceberg configuration  
CATALOG_URI=https://[PROJECT-ID].supabase.co/storage/v1/iceberg
WAREHOUSE=[YOUR-WAREHOUSE-NAME]
TOKEN=[SUPABASE-SERVICE-ROLE-TOKEN]
```

Required environment variables:
- `POSTGRES_DB`: Supabase PostgreSQL connection string
- `CATALOG_URI`: Your Supabase Storage API endpoint
- `WAREHOUSE`: Warehouse identifier for Iceberg tables
- `TOKEN`: Supabase service role token

## Running the Example

```bash
# Make sure you're in the examples/supabase_iceberg directory
cd examples/supabase_iceberg

# Run the ETL pipeline
cargo run --example supabase_pipeline
```

The pipeline will:
1. Connect to your Supabase PostgreSQL database
2. Create the `foo.orders` table if it doesn't exist
3. Set up a PostgreSQL publication (`orders_publication`) for CDC
4. Configure the Supabase Iceberg destination
5. Start the CDC pipeline to stream changes in real-time
6. Wait for you to insert/update/delete records in PostgreSQL

## Expected Output

```
🚀 Starting Supabase PostgreSQL CDC → Supabase Iceberg Pipeline
✅ Environment configuration loaded successfully
📊 Setting up PostgreSQL database and publication...
✅ Created foo.orders table and orders_publication
✅ PostgreSQL database and publication configured
🧊 Setting up Supabase Iceberg destination...
✅ Supabase Iceberg destination configured
⚡ Configuring CDC pipeline...
🚀 Starting CDC pipeline...
✅ CDC Pipeline started successfully!
💡 The pipeline is now streaming changes from PostgreSQL to Supabase Iceberg
📊 Insert records into foo.orders table to see CDC in action
🔄 Pipeline will continue running for monitoring...
```

## Architecture

This example demonstrates a simplified, production-ready architecture using only remote services:

- **Remote PostgreSQL**: Supabase PostgreSQL database
- **Remote Iceberg Storage**: Supabase Storage API with Iceberg REST catalog compatibility
- **ETL Pipeline**: Rust-based ETL worker that connects the two

The implementation includes a Supabase compatibility layer that handles:
- URL pattern remapping for Supabase endpoints
- Field name translations for API differences  
- Proper authentication with service role tokens

## Troubleshooting

### Common Issues

1. **Connection Errors**
   ```
   Error: Failed to connect to PostgreSQL
   ```
   - Verify your `POSTGRES_DB` connection string is correct
   - Ensure the Supabase project is active
   - Check that pooler access is enabled

2. **Authentication Errors**
   ```
   Error: HTTP 401 error: Unauthorized
   ```
   - Verify your `TOKEN` is a valid service role key
   - Check that Storage API access is enabled
   - Ensure the token has necessary permissions

3. **Iceberg Catalog Errors**
   ```
   Error: HTTP 404 error: Not Found
   ```
   - Verify your `CATALOG_URI` uses the correct project ID
   - Check that the `WAREHOUSE` name is valid
   - Ensure Iceberg feature is enabled in Supabase

### Debug Logging

Enable detailed logging:
```bash
RUST_LOG=etl_destinations=debug cargo run --example supabase_pipeline
```

## Next Steps

- Scale up data volume and measure throughput for your use case
- Implement error handling and retry logic for production workloads
- Consider batch sizes and CDC patterns based on your data characteristics
- Monitor [Supabase Storage API](https://github.com/supabase/storage-api) for native Iceberg improvements