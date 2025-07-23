#!/usr/bin/env python3
"""
Test script for Heraclitus Kafka protocol implementation.
Requires: pip install kafka-python
"""

import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import sys

logging.basicConfig(level=logging.DEBUG)

def test_metadata_request():
    """Test basic metadata request to Heraclitus"""
    print("Testing Kafka protocol connection to localhost:9092...")
    
    try:
        # Create a Kafka producer which will send a metadata request on connect
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10, 0),  # Use an older API version for simplicity
            request_timeout_ms=5000,
            metadata_max_age_ms=1000,
        )
        
        print("✓ Successfully connected to Kafka broker")
        print(f"✓ Broker metadata: {producer._metadata}")
        
        # Get cluster metadata
        metadata = producer._metadata
        brokers = metadata.brokers()
        print(f"\nDiscovered brokers:")
        for broker_id, broker_metadata in brokers.items():
            print(f"  - Broker {broker_id}: {broker_metadata}")
        
        topics = metadata.topics()
        print(f"\nDiscovered topics: {list(topics) if topics else 'None'}")
        
        producer.close()
        print("\n✓ Test completed successfully!")
        
    except KafkaError as e:
        print(f"✗ Kafka error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_metadata_request()