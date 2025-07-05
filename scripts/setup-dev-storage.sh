#!/bin/bash

# SignalDB Development Storage Setup
# Creates necessary directories for WAL and data storage in development environments

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
BASE_DIR="./data"
WAL_DIRS=("writer-wal" "acceptor-wal")
STORAGE_DIRS=("writer-storage")

echo -e "${GREEN}Setting up SignalDB development storage directories...${NC}"

# Create base data directory
if [ ! -d "$BASE_DIR" ]; then
    echo "Creating base data directory: $BASE_DIR"
    mkdir -p "$BASE_DIR"
else
    echo "Base data directory already exists: $BASE_DIR"
fi

# Create WAL directories
echo -e "\n${YELLOW}Creating WAL directories for durability:${NC}"
for wal_dir in "${WAL_DIRS[@]}"; do
    full_path="$BASE_DIR/$wal_dir"
    if [ ! -d "$full_path" ]; then
        echo "  Creating: $full_path"
        mkdir -p "$full_path"
        # Set appropriate permissions for container access
        chmod 755 "$full_path"
    else
        echo "  Already exists: $full_path"
    fi
done

# Create storage directories  
echo -e "\n${YELLOW}Creating storage directories:${NC}"
for storage_dir in "${STORAGE_DIRS[@]}"; do
    full_path="$BASE_DIR/$storage_dir"
    if [ ! -d "$full_path" ]; then
        echo "  Creating: $full_path"
        mkdir -p "$full_path"
        chmod 755 "$full_path"
    else
        echo "  Already exists: $full_path"
    fi
done

# Create .gitignore for data directory
if [ ! -f "$BASE_DIR/.gitignore" ]; then
    echo -e "\n${YELLOW}Creating .gitignore for data directory:${NC}"
    cat > "$BASE_DIR/.gitignore" << EOF
# Ignore all contents of data directory except this .gitignore
*
!.gitignore

# WAL files contain sensitive data and should not be committed
*.wal
*.checkpoint

# Storage files are typically large and should not be committed  
*.parquet
*.arrow
EOF
    echo "  Created: $BASE_DIR/.gitignore"
fi

# Verify directory structure
echo -e "\n${GREEN}Directory structure created:${NC}"
tree "$BASE_DIR" 2>/dev/null || find "$BASE_DIR" -type d | sed 's|[^/]*/|  |g'

# Display environment variable recommendations
echo -e "\n${YELLOW}Recommended environment variables for development:${NC}"
echo "export WRITER_WAL_DIR=\"$(pwd)/$BASE_DIR/writer-wal\""
echo "export ACCEPTOR_WAL_DIR=\"$(pwd)/$BASE_DIR/acceptor-wal\""
echo "export SIGNALDB_STORAGE_ADAPTERS_LOCAL_URL=\"file://$(pwd)/$BASE_DIR/writer-storage\""

# Docker Compose setup instructions
echo -e "\n${YELLOW}For Docker Compose, ensure these volumes are configured:${NC}"
echo "  writer-wal -> $BASE_DIR/writer-wal"
echo "  acceptor-wal -> $BASE_DIR/acceptor-wal"
echo "  writer-data -> $BASE_DIR/writer-storage"

# Warnings and recommendations
echo -e "\n${RED}Important Notes:${NC}"
echo "  • WAL directories contain critical durability data"
echo "  • Do not delete WAL files while services are running"
echo "  • Backup WAL directories before major changes"
echo "  • Monitor disk space usage in production"

echo -e "\n${GREEN}Setup complete! ✅${NC}"
echo "You can now start SignalDB services with persistent storage."