#!/usr/bin/env python3
import socket
import struct

def create_api_versions_request():
    """Create a simple API Versions request"""
    # API Versions request format:
    # Request Header: api_key(2), api_version(2), correlation_id(4), client_id_length(2), client_id(var)
    # Request Body: (empty for API Versions)
    
    api_key = 18  # ApiVersions
    api_version = 3  # Use v3 which has flexible format
    correlation_id = 1
    client_id = b"debug-client"
    
    # Build request header with proper Kafka string encoding
    header = struct.pack(">HHI", api_key, api_version, correlation_id)
    # For nullable string: length as i16, then string data
    header += struct.pack(">H", len(client_id))  # String length as i16
    header += client_id
    
    # No body for API Versions request
    request_data = header
    
    # Prepend the total length
    total_length = len(request_data)
    return struct.pack(">I", total_length) + request_data

def test_heraclitus():
    """Test connection to Heraclitus"""
    try:
        print("Connecting to Heraclitus on localhost:9092...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect(('localhost', 9092))
        print("Connected successfully!")
        
        # Send API Versions request
        request = create_api_versions_request()
        print(f"Sending API Versions request ({len(request)} bytes)...")
        sock.sendall(request)
        print("Request sent, waiting for response...")
        
        # Read response length
        length_bytes = sock.recv(4)
        if len(length_bytes) < 4:
            print("Failed to read response length")
            return
            
        response_length = struct.unpack(">I", length_bytes)[0]
        print(f"Response length: {response_length}")
        
        # Read response
        response_data = sock.recv(response_length)
        print(f"Received {len(response_data)} bytes")
        print(f"Response data: {response_data.hex()}")
        
        sock.close()
        print("Test completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_heraclitus()