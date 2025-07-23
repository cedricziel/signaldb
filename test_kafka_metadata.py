#!/usr/bin/env python3
"""
Simple test script to verify Kafka Metadata API implementation in Heraclitus.
"""

import socket
import struct
import sys

def send_kafka_request(host, port, api_key, api_version, correlation_id, client_id, request_body):
    """Send a Kafka request and receive the response."""
    # Connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    
    try:
        # Build request header
        request = bytearray()
        request.extend(struct.pack('>h', api_key))  # API key
        request.extend(struct.pack('>h', api_version))  # API version
        request.extend(struct.pack('>i', correlation_id))  # Correlation ID
        
        # Client ID (nullable string)
        if client_id:
            client_bytes = client_id.encode('utf-8')
            request.extend(struct.pack('>h', len(client_bytes)))
            request.extend(client_bytes)
        else:
            request.extend(struct.pack('>h', -1))  # Null string
        
        # Add request body
        request.extend(request_body)
        
        # Send request with length prefix
        message = struct.pack('>i', len(request)) + request
        sock.sendall(message)
        
        # Read response length
        length_bytes = sock.recv(4)
        if len(length_bytes) < 4:
            raise Exception("Failed to read response length")
        
        response_length = struct.unpack('>i', length_bytes)[0]
        
        # Read response
        response = bytearray()
        while len(response) < response_length:
            chunk = sock.recv(response_length - len(response))
            if not chunk:
                raise Exception("Connection closed while reading response")
            response.extend(chunk)
        
        return response
        
    finally:
        sock.close()

def parse_metadata_response(response):
    """Parse a metadata response."""
    offset = 0
    
    # Read correlation ID
    correlation_id = struct.unpack('>i', response[offset:offset+4])[0]
    offset += 4
    print(f"Correlation ID: {correlation_id}")
    
    # Read broker count
    broker_count = struct.unpack('>i', response[offset:offset+4])[0]
    offset += 4
    print(f"Broker count: {broker_count}")
    
    # Read brokers
    for i in range(broker_count):
        node_id = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        
        host_len = struct.unpack('>h', response[offset:offset+2])[0]
        offset += 2
        host = response[offset:offset+host_len].decode('utf-8')
        offset += host_len
        
        port = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        
        print(f"  Broker {i}: node_id={node_id}, host={host}, port={port}")
    
    # Read topic count
    topic_count = struct.unpack('>i', response[offset:offset+4])[0]
    offset += 4
    print(f"Topic count: {topic_count}")
    
    # Read topics
    for i in range(topic_count):
        error_code = struct.unpack('>h', response[offset:offset+2])[0]
        offset += 2
        
        name_len = struct.unpack('>h', response[offset:offset+2])[0]
        offset += 2
        name = response[offset:offset+name_len].decode('utf-8')
        offset += name_len
        
        partition_count = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        
        print(f"  Topic {i}: name={name}, error_code={error_code}, partitions={partition_count}")
        
        # Skip partition details for brevity
        for j in range(partition_count):
            offset += 2  # error_code
            offset += 4  # partition_id
            offset += 4  # leader
            
            # Replicas
            replica_count = struct.unpack('>i', response[offset:offset+4])[0]
            offset += 4
            offset += replica_count * 4
            
            # ISR
            isr_count = struct.unpack('>i', response[offset:offset+4])[0]
            offset += 4
            offset += isr_count * 4

def test_metadata_api(host='localhost', port=9092):
    """Test the Metadata API."""
    print(f"Testing Kafka Metadata API on {host}:{port}")
    print("-" * 50)
    
    # Test 1: Request metadata for all topics
    print("\nTest 1: Request all topics")
    request_body = struct.pack('>i', -1)  # Null array = all topics
    
    try:
        response = send_kafka_request(
            host, port,
            api_key=3,  # Metadata API
            api_version=0,
            correlation_id=1,
            client_id="test-client",
            request_body=request_body
        )
        parse_metadata_response(response)
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 2: Request metadata for specific topic
    print("\n\nTest 2: Request specific topic")
    request_body = bytearray()
    request_body.extend(struct.pack('>i', 1))  # Array with 1 topic
    topic_name = "test-topic"
    request_body.extend(struct.pack('>h', len(topic_name)))
    request_body.extend(topic_name.encode('utf-8'))
    
    try:
        response = send_kafka_request(
            host, port,
            api_key=3,  # Metadata API
            api_version=0,
            correlation_id=2,
            client_id="test-client",
            request_body=bytes(request_body)
        )
        parse_metadata_response(response)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
        test_metadata_api(port=port)
    else:
        test_metadata_api()