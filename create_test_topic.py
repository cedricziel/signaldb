#!/usr/bin/env python3
"""
Script to create a test topic in Heraclitus by directly writing to object storage.
This simulates topic creation until we implement the CreateTopics API.
"""

import json
import os
from datetime import datetime, timezone

def create_topic_metadata(name, partitions=3, replication_factor=1):
    """Create topic metadata JSON."""
    return {
        "name": name,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "config": {},
        "created_at": datetime.now(timezone.utc).isoformat()
    }

def main():
    # Create test topic metadata
    topic_name = "test-topic"
    metadata = create_topic_metadata(topic_name)
    
    # Write to the expected location
    # Assuming file-based object storage at .data/storage/heraclitus/
    storage_path = ".data/storage/heraclitus/metadata/topics"
    os.makedirs(storage_path, exist_ok=True)
    
    file_path = os.path.join(storage_path, f"{topic_name}.json")
    with open(file_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created topic '{topic_name}' metadata at: {file_path}")
    print(f"Metadata: {json.dumps(metadata, indent=2)}")

if __name__ == "__main__":
    main()