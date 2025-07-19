#!/bin/bash

# Iceberg Writer Performance Benchmarks
# This script runs comprehensive performance benchmarks for the Iceberg writer

set -e

echo "🚀 Starting Iceberg Writer Performance Benchmarks"
echo "=================================================="

# Ensure we're in the writer directory
cd "$(dirname "$0")"

# Set environment variables for optimal performance
export RUST_LOG=warn  # Reduce logging overhead during benchmarks
export RUSTFLAGS="-C target-cpu=native"  # Optimize for current CPU

echo "📊 Running benchmarks with criterion..."
echo "This may take 10-15 minutes to complete all benchmarks."
echo ""

# Run the benchmarks
cargo bench --bench iceberg_benchmarks

echo ""
echo "✅ Benchmarks completed!"
echo ""
echo "📈 Results have been saved to:"
echo "   - HTML Report: target/criterion/report/index.html"
echo "   - CSV Data: target/criterion/*/base/raw.csv"
echo ""
echo "🔍 To view the HTML report:"
echo "   open target/criterion/report/index.html"
echo ""
echo "📋 Benchmark Summary:"
echo "   - Single batch writes: 100, 1K, 10K, 50K rows"
echo "   - Multi-batch writes: 2-20 batches"
echo "   - Transaction overhead analysis"
echo "   - Writer creation performance"
echo "   - Simulated concurrent writes"
echo "   - Memory usage patterns"