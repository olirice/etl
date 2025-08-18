#!/bin/bash
set -e

echo "🔧 Reorganizing dependencies to add them only when needed..."

# Save current branch
CURRENT_BRANCH=$(git branch --show-current)

# Branch 1: Foundation - No dependencies needed (just module doc)
echo "═══════════════════════════════════════════════════════"
echo "Branch 1: Foundation - Removing all Iceberg dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-1-foundation

# Remove all iceberg dependencies from Cargo.toml since we're just adding a doc module
git checkout main -- etl-destinations/Cargo.toml

# Add just the iceberg feature flag (empty for now)
cat >> etl-destinations/Cargo.toml << 'EOF'

[features]
iceberg = []
EOF

git add -A
git commit --amend --no-edit

# Branch 2: Config - Add serde dependencies
echo "═══════════════════════════════════════════════════════"
echo "Branch 2: Config - Adding only serde dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-2-config

# Start with foundation's Cargo.toml
git checkout or/iceberg-1-foundation -- etl-destinations/Cargo.toml

# Add only the dependencies needed for config
sed -i '' 's/iceberg = \[\]/iceberg = ["dep:serde", "dep:serde_json"]/' etl-destinations/Cargo.toml

# Add the actual dependency lines
cat >> etl-destinations/Cargo.toml << 'EOF'

# Iceberg dependencies
serde = { workspace = true, optional = true, features = ["derive"] }
serde_json = { workspace = true, optional = true }
EOF

git add -A
git commit --amend --no-edit

# Branch 3: Schema - Add arrow, chrono, base64, tokio-postgres, tracing
echo "═══════════════════════════════════════════════════════"
echo "Branch 3: Schema - Adding schema-related dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-3-schema

# Start fresh
git checkout or/iceberg-2-config -- etl-destinations/Cargo.toml

# Update feature list
sed -i '' 's/iceberg = \["dep:serde", "dep:serde_json"\]/iceberg = ["dep:serde", "dep:serde_json", "dep:arrow", "dep:iceberg", "dep:chrono", "dep:base64", "dep:tokio-postgres", "dep:tracing"]/' etl-destinations/Cargo.toml

# Add the new dependencies
sed -i '' '/# Iceberg dependencies/a\
arrow = { version = "55.0", optional = true, features = ["prettyprint"] }\
iceberg = { version = "0.6", optional = true }\
chrono = { workspace = true, optional = true }\
base64 = { workspace = true, optional = true }\
tokio-postgres = { workspace = true, optional = true }\
tracing = { workspace = true, optional = true, default-features = true }
' etl-destinations/Cargo.toml

git add -A
git commit --amend --no-edit

# Branch 4: Encoding - No new dependencies needed
echo "═══════════════════════════════════════════════════════"
echo "Branch 4: Encoding - No new dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-4-encoding
# Encoding uses same deps as schema
git checkout or/iceberg-3-schema -- etl-destinations/Cargo.toml
git add -A
git commit --amend --no-edit

# Branch 5: Client - Add REST catalog, parquet, url, uuid, reqwest, object_store
echo "═══════════════════════════════════════════════════════"
echo "Branch 5: Client - Adding client dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-5-client

git checkout or/iceberg-4-encoding -- etl-destinations/Cargo.toml

# Update feature list
sed -i '' 's/iceberg = \["dep:serde", "dep:serde_json", "dep:arrow", "dep:iceberg", "dep:chrono", "dep:base64", "dep:tokio-postgres", "dep:tracing"\]/iceberg = ["dep:serde", "dep:serde_json", "dep:arrow", "dep:iceberg", "dep:iceberg-catalog-rest", "dep:chrono", "dep:base64", "dep:tokio-postgres", "dep:tracing", "dep:parquet", "dep:url", "dep:uuid", "dep:reqwest", "dep:object_store"]/' etl-destinations/Cargo.toml

# Add the new dependencies
sed -i '' '/iceberg = { version = "0.6"/a\
iceberg-catalog-rest = { version = "0.6", optional = true }\
parquet = { version = "55.0", optional = true, features = ["async", "arrow"] }\
url = { version = "2.5", optional = true }\
uuid = { workspace = true, optional = true, features = ["v4"] }\
reqwest = { version = "0.12", optional = true, features = ["json"] }\
object_store = { version = "0.11", optional = true, features = ["aws"] }
' etl-destinations/Cargo.toml

git add -A
git commit --amend --no-edit

# Branch 6: Destination - Add async-trait, tokio, futures
echo "═══════════════════════════════════════════════════════"
echo "Branch 6: Destination - Adding async dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-6-destination

git checkout or/iceberg-5-client -- etl-destinations/Cargo.toml

# Update feature list
sed -i '' 's/iceberg = \["dep:serde", "dep:serde_json", "dep:arrow", "dep:iceberg", "dep:iceberg-catalog-rest", "dep:chrono", "dep:base64", "dep:tokio-postgres", "dep:tracing", "dep:parquet", "dep:url", "dep:uuid", "dep:reqwest", "dep:object_store"\]/iceberg = ["dep:serde", "dep:serde_json", "dep:arrow", "dep:iceberg", "dep:iceberg-catalog-rest", "dep:chrono", "dep:base64", "dep:tokio-postgres", "dep:tracing", "dep:parquet", "dep:url", "dep:uuid", "dep:reqwest", "dep:object_store", "dep:async-trait", "dep:tokio", "dep:futures"]/' etl-destinations/Cargo.toml

# Add the new dependencies
sed -i '' '/uuid = { workspace/a\
async-trait = { workspace = true, optional = true }\
tokio = { workspace = true, optional = true, features = ["sync"] }\
futures = { workspace = true, optional = true }
' etl-destinations/Cargo.toml

git add -A
git commit --amend --no-edit

# Branch 7: Infrastructure - Add etl-config
echo "═══════════════════════════════════════════════════════"
echo "Branch 7: Infrastructure - Adding etl-config"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-7-infrastructure

git checkout or/iceberg-6-destination -- etl-destinations/Cargo.toml

# Update feature list
sed -i '' 's/iceberg = \["dep:serde", "dep:serde_json", "dep:arrow", "dep:iceberg", "dep:iceberg-catalog-rest", "dep:chrono", "dep:base64", "dep:tokio-postgres", "dep:tracing", "dep:parquet", "dep:url", "dep:uuid", "dep:reqwest", "dep:object_store", "dep:async-trait", "dep:tokio", "dep:futures"\]/iceberg = ["dep:serde", "dep:serde_json", "dep:arrow", "dep:iceberg", "dep:iceberg-catalog-rest", "dep:chrono", "dep:base64", "dep:tokio-postgres", "dep:tracing", "dep:parquet", "dep:url", "dep:uuid", "dep:reqwest", "dep:object_store", "dep:async-trait", "dep:tokio", "dep:futures", "dep:etl-config"]/' etl-destinations/Cargo.toml

# Add etl-config
sed -i '' '/^etl-postgres = /a\
etl-config = { workspace = true, optional = true }
' etl-destinations/Cargo.toml

git add -A
git commit --amend --no-edit

# Branch 8 & 9: Tests and Integration - Keep all dependencies
echo "═══════════════════════════════════════════════════════"
echo "Branch 8: Tests - Keeping all dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-8-tests
git checkout or/iceberg-7-infrastructure -- etl-destinations/Cargo.toml
git add -A
git commit --amend --no-edit

echo "═══════════════════════════════════════════════════════"
echo "Branch 9: Integration - Keeping all dependencies"
echo "═══════════════════════════════════════════════════════"
git checkout or/iceberg-9-integration
git checkout or/iceberg-8-tests -- etl-destinations/Cargo.toml
git add -A
git commit --amend --no-edit

# Return to original branch
git checkout $CURRENT_BRANCH

echo "✅ Dependencies reorganized successfully!"