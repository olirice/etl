#!/bin/bash
set -e

echo "🔧 Fixing Cargo.toml files across all branches..."

# Save current branch
CURRENT_BRANCH=$(git branch --show-current)

# Branch 1: Foundation - Just the feature flag, no deps
echo "Branch 1: Foundation"
git checkout or/iceberg-1-foundation

cat > etl-destinations/Cargo.toml << 'EOF'
[package]
name = "etl-destinations"
version = "0.1.0"
edition = "2024"

[features]
bigquery = [
    "dep:futures",
    "dep:gcp-bigquery-client",
    "dep:prost",
    "dep:rustls",
    "dep:tracing",
    "dep:tokio",
]
iceberg = []

[dependencies]
etl = { workspace = true }
etl-postgres = { workspace = true }

# BigQuery dependencies
futures = { workspace = true, optional = true }
gcp-bigquery-client = { workspace = true, optional = true, features = [
    "rust-tls",
    "aws-lc-rs",
] }
prost = { workspace = true, optional = true }
rustls = { workspace = true, optional = true, features = [
    "aws-lc-rs",
    "logging",
] }
tokio = { workspace = true, optional = true, features = ["sync"] }
tracing = { workspace = true, optional = true, default-features = true }

[dev-dependencies]
etl = { workspace = true, features = ["test-utils"] }
etl-telemetry = { workspace = true }

base64 = { workspace = true }
chrono = { workspace = true }
rand = { workspace = true, features = ["thread_rng"] }
serde_json = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
EOF

git add -A
git commit --amend --no-edit

# Branch 2: Config - Add serde
echo "Branch 2: Config"
git checkout or/iceberg-2-config

cat > etl-destinations/Cargo.toml << 'EOF'
[package]
name = "etl-destinations"
version = "0.1.0"
edition = "2024"

[features]
bigquery = [
    "dep:futures",
    "dep:gcp-bigquery-client",
    "dep:prost",
    "dep:rustls",
    "dep:tracing",
    "dep:tokio",
]
iceberg = [
    "dep:serde",
    "dep:serde_json",
]

[dependencies]
etl = { workspace = true }
etl-postgres = { workspace = true }

# Common dependencies
serde = { workspace = true, optional = true, features = ["derive"] }
serde_json = { workspace = true, optional = true }

# BigQuery dependencies
futures = { workspace = true, optional = true }
gcp-bigquery-client = { workspace = true, optional = true, features = [
    "rust-tls",
    "aws-lc-rs",
] }
prost = { workspace = true, optional = true }
rustls = { workspace = true, optional = true, features = [
    "aws-lc-rs",
    "logging",
] }
tokio = { workspace = true, optional = true, features = ["sync"] }
tracing = { workspace = true, optional = true, default-features = true }

[dev-dependencies]
etl = { workspace = true, features = ["test-utils"] }
etl-telemetry = { workspace = true }

base64 = { workspace = true }
chrono = { workspace = true }
rand = { workspace = true, features = ["thread_rng"] }
serde_json = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
EOF

git add -A
git commit --amend --no-edit

# Branch 3: Schema - Add schema dependencies
echo "Branch 3: Schema"
git checkout or/iceberg-3-schema

cat > etl-destinations/Cargo.toml << 'EOF'
[package]
name = "etl-destinations"
version = "0.1.0"
edition = "2024"

[features]
bigquery = [
    "dep:futures",
    "dep:gcp-bigquery-client",
    "dep:prost",
    "dep:rustls",
    "dep:tracing",
    "dep:tokio",
]
iceberg = [
    "dep:arrow",
    "dep:base64",
    "dep:chrono",
    "dep:iceberg",
    "dep:serde",
    "dep:serde_json",
    "dep:tokio-postgres",
    "dep:tracing",
]

[dependencies]
etl = { workspace = true }
etl-postgres = { workspace = true }

# Common dependencies
base64 = { workspace = true, optional = true }
chrono = { workspace = true, optional = true }
serde = { workspace = true, optional = true, features = ["derive"] }
serde_json = { workspace = true, optional = true }
tokio-postgres = { workspace = true, optional = true }
tracing = { workspace = true, optional = true, default-features = true }

# BigQuery dependencies
futures = { workspace = true, optional = true }
gcp-bigquery-client = { workspace = true, optional = true, features = [
    "rust-tls",
    "aws-lc-rs",
] }
prost = { workspace = true, optional = true }
rustls = { workspace = true, optional = true, features = [
    "aws-lc-rs",
    "logging",
] }
tokio = { workspace = true, optional = true, features = ["sync"] }

# Iceberg dependencies
arrow = { version = "55.0", optional = true, features = ["prettyprint"] }
iceberg = { version = "0.6", optional = true }

[dev-dependencies]
etl = { workspace = true, features = ["test-utils"] }
etl-telemetry = { workspace = true }

base64 = { workspace = true }
chrono = { workspace = true }
rand = { workspace = true, features = ["thread_rng"] }
serde_json = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
EOF

git add -A
git commit --amend --no-edit

# Branch 4: Encoding - Same as schema
echo "Branch 4: Encoding"
git checkout or/iceberg-4-encoding
git checkout or/iceberg-3-schema -- etl-destinations/Cargo.toml
git add -A
git commit --amend --no-edit

# Branch 5: Client - Add client dependencies
echo "Branch 5: Client"
git checkout or/iceberg-5-client

cat > etl-destinations/Cargo.toml << 'EOF'
[package]
name = "etl-destinations"
version = "0.1.0"
edition = "2024"

[features]
bigquery = [
    "dep:futures",
    "dep:gcp-bigquery-client",
    "dep:prost",
    "dep:rustls",
    "dep:tracing",
    "dep:tokio",
]
iceberg = [
    "dep:arrow",
    "dep:base64",
    "dep:chrono",
    "dep:iceberg",
    "dep:iceberg-catalog-rest",
    "dep:object_store",
    "dep:parquet",
    "dep:reqwest",
    "dep:serde",
    "dep:serde_json",
    "dep:tokio-postgres",
    "dep:tracing",
    "dep:url",
    "dep:uuid",
]

[dependencies]
etl = { workspace = true }
etl-postgres = { workspace = true }

# Common dependencies
base64 = { workspace = true, optional = true }
chrono = { workspace = true, optional = true }
serde = { workspace = true, optional = true, features = ["derive"] }
serde_json = { workspace = true, optional = true }
tokio-postgres = { workspace = true, optional = true }
tracing = { workspace = true, optional = true, default-features = true }
uuid = { workspace = true, optional = true, features = ["v4"] }

# BigQuery dependencies
futures = { workspace = true, optional = true }
gcp-bigquery-client = { workspace = true, optional = true, features = [
    "rust-tls",
    "aws-lc-rs",
] }
prost = { workspace = true, optional = true }
rustls = { workspace = true, optional = true, features = [
    "aws-lc-rs",
    "logging",
] }
tokio = { workspace = true, optional = true, features = ["sync"] }

# Iceberg dependencies
arrow = { version = "55.0", optional = true, features = ["prettyprint"] }
iceberg = { version = "0.6", optional = true }
iceberg-catalog-rest = { version = "0.6", optional = true }
object_store = { version = "0.11", optional = true, features = ["aws"] }
parquet = { version = "55.0", optional = true, features = ["async", "arrow"] }
reqwest = { version = "0.12", optional = true, features = ["json"] }
url = { version = "2.5", optional = true }

[dev-dependencies]
etl = { workspace = true, features = ["test-utils"] }
etl-telemetry = { workspace = true }

base64 = { workspace = true }
chrono = { workspace = true }
rand = { workspace = true, features = ["thread_rng"] }
serde_json = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
EOF

git add -A
git commit --amend --no-edit

# Branch 6: Destination - Add async dependencies
echo "Branch 6: Destination"
git checkout or/iceberg-6-destination

cat > etl-destinations/Cargo.toml << 'EOF'
[package]
name = "etl-destinations"
version = "0.1.0"
edition = "2024"

[features]
bigquery = [
    "dep:futures",
    "dep:gcp-bigquery-client",
    "dep:prost",
    "dep:rustls",
    "dep:tracing",
    "dep:tokio",
]
iceberg = [
    "dep:arrow",
    "dep:async-trait",
    "dep:base64",
    "dep:chrono",
    "dep:futures",
    "dep:iceberg",
    "dep:iceberg-catalog-rest",
    "dep:object_store",
    "dep:parquet",
    "dep:reqwest",
    "dep:serde",
    "dep:serde_json",
    "dep:tokio",
    "dep:tokio-postgres",
    "dep:tracing",
    "dep:url",
    "dep:uuid",
]

[dependencies]
etl = { workspace = true }
etl-postgres = { workspace = true }

# Common dependencies
async-trait = { workspace = true, optional = true }
base64 = { workspace = true, optional = true }
chrono = { workspace = true, optional = true }
futures = { workspace = true, optional = true }
serde = { workspace = true, optional = true, features = ["derive"] }
serde_json = { workspace = true, optional = true }
tokio = { workspace = true, optional = true, features = ["sync"] }
tokio-postgres = { workspace = true, optional = true }
tracing = { workspace = true, optional = true, default-features = true }
uuid = { workspace = true, optional = true, features = ["v4"] }

# BigQuery dependencies
gcp-bigquery-client = { workspace = true, optional = true, features = [
    "rust-tls",
    "aws-lc-rs",
] }
prost = { workspace = true, optional = true }
rustls = { workspace = true, optional = true, features = [
    "aws-lc-rs",
    "logging",
] }

# Iceberg dependencies
arrow = { version = "55.0", optional = true, features = ["prettyprint"] }
iceberg = { version = "0.6", optional = true }
iceberg-catalog-rest = { version = "0.6", optional = true }
object_store = { version = "0.11", optional = true, features = ["aws"] }
parquet = { version = "55.0", optional = true, features = ["async", "arrow"] }
reqwest = { version = "0.12", optional = true, features = ["json"] }
url = { version = "2.5", optional = true }

[dev-dependencies]
etl = { workspace = true, features = ["test-utils"] }
etl-telemetry = { workspace = true }

base64 = { workspace = true }
chrono = { workspace = true }
rand = { workspace = true, features = ["thread_rng"] }
serde_json = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
EOF

git add -A
git commit --amend --no-edit

# Branch 7: Infrastructure - Add etl-config
echo "Branch 7: Infrastructure"
git checkout or/iceberg-7-infrastructure

cat > etl-destinations/Cargo.toml << 'EOF'
[package]
name = "etl-destinations"
version = "0.1.0"
edition = "2024"

[features]
bigquery = [
    "dep:futures",
    "dep:gcp-bigquery-client",
    "dep:prost",
    "dep:rustls",
    "dep:tracing",
    "dep:tokio",
]
iceberg = [
    "dep:arrow",
    "dep:async-trait",
    "dep:base64",
    "dep:chrono",
    "dep:etl-config",
    "dep:futures",
    "dep:iceberg",
    "dep:iceberg-catalog-rest",
    "dep:object_store",
    "dep:parquet",
    "dep:reqwest",
    "dep:serde",
    "dep:serde_json",
    "dep:tokio",
    "dep:tokio-postgres",
    "dep:tracing",
    "dep:url",
    "dep:uuid",
]

[dependencies]
etl = { workspace = true }
etl-config = { workspace = true, optional = true }
etl-postgres = { workspace = true }

# Common dependencies
async-trait = { workspace = true, optional = true }
base64 = { workspace = true, optional = true }
chrono = { workspace = true, optional = true }
futures = { workspace = true, optional = true }
serde = { workspace = true, optional = true, features = ["derive"] }
serde_json = { workspace = true, optional = true }
tokio = { workspace = true, optional = true, features = ["sync"] }
tokio-postgres = { workspace = true, optional = true }
tracing = { workspace = true, optional = true, default-features = true }
uuid = { workspace = true, optional = true, features = ["v4"] }

# BigQuery dependencies
gcp-bigquery-client = { workspace = true, optional = true, features = [
    "rust-tls",
    "aws-lc-rs",
] }
prost = { workspace = true, optional = true }
rustls = { workspace = true, optional = true, features = [
    "aws-lc-rs",
    "logging",
] }

# Iceberg dependencies
arrow = { version = "55.0", optional = true, features = ["prettyprint"] }
iceberg = { version = "0.6", optional = true }
iceberg-catalog-rest = { version = "0.6", optional = true }
object_store = { version = "0.11", optional = true, features = ["aws"] }
parquet = { version = "55.0", optional = true, features = ["async", "arrow"] }
reqwest = { version = "0.12", optional = true, features = ["json"] }
url = { version = "2.5", optional = true }

[dev-dependencies]
etl = { workspace = true, features = ["test-utils"] }
etl-telemetry = { workspace = true }

base64 = { workspace = true }
chrono = { workspace = true }
rand = { workspace = true, features = ["thread_rng"] }
serde_json = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
EOF

git add -A
git commit --amend --no-edit

# Branch 8 & 9: Keep all dependencies
echo "Branch 8: Tests"
git checkout or/iceberg-8-tests
git checkout or/iceberg-7-infrastructure -- etl-destinations/Cargo.toml
git add -A
git commit --amend --no-edit

echo "Branch 9: Integration"
git checkout or/iceberg-9-integration
git checkout or/iceberg-8-tests -- etl-destinations/Cargo.toml
git add -A
git commit --amend --no-edit

# Return to original branch
git checkout $CURRENT_BRANCH

echo "✅ All Cargo.toml files fixed!"