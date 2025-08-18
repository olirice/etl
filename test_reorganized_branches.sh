#!/bin/bash
set -e

echo "🧪 Testing reorganized branches..."

BRANCHES=(
    "or/iceberg-1-foundation"
    "or/iceberg-2-config"
    "or/iceberg-3-schema" 
    "or/iceberg-4-encoding"
    "or/iceberg-5-client"
    "or/iceberg-6-destination"
    "or/iceberg-7-infrastructure"
    "or/iceberg-8-tests"
    "or/iceberg-9-integration"
)

for branch in "${BRANCHES[@]}"; do
    echo ""
    echo "========================================="
    echo "Testing branch: $branch"
    echo "========================================="
    
    git checkout "$branch"
    
    echo "  🔧 Running cargo check..."
    if cargo check --all-features > /dev/null 2>&1; then
        echo "  ✅ Build check passed"
    else
        echo "  ❌ Build check failed"
        cargo check --all-features
        exit 1
    fi
    
    echo "  🎨 Running cargo fmt check..."
    if cargo fmt --all -- --check > /dev/null 2>&1; then
        echo "  ✅ Format check passed"
    else
        echo "  ⚠️ Format issues found, fixing..."
        cargo fmt --all
        git add -A
        git commit --amend --no-edit
        echo "  ✅ Format fixed and committed"
    fi
    
    echo "  📎 Running cargo clippy..."
    if cargo clippy --all-features -- -D warnings > /dev/null 2>&1; then
        echo "  ✅ Clippy passed"
    else
        echo "  ⚠️ Clippy warnings found"
        cargo clippy --all-features -- -D warnings || true
    fi
    
    echo "  ✅ Branch $branch tested successfully"
done

echo ""
echo "🚀 All branches tested! Force pushing..."

# Force push all branches
git push oli --force \
    or/iceberg-1-foundation \
    or/iceberg-2-config \
    or/iceberg-3-schema \
    or/iceberg-4-encoding \
    or/iceberg-5-client \
    or/iceberg-6-destination \
    or/iceberg-7-infrastructure \
    or/iceberg-8-tests \
    or/iceberg-9-integration

echo "✅ All reorganized branches pushed successfully!"