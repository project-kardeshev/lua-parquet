#!/usr/bin/env python3
"""
Analyze Parquet file binary structure to understand Thrift encoding.
This script helps to understand the binary structure of valid Parquet files.
"""

import os
import sys
import pyarrow.parquet as pq
import binascii

def hex_dump(data, offset=0, length=16, width=16):
    """
    Format binary data as a hex dump with offset, hex values and ASCII representation.
    """
    result = []
    for i in range(0, len(data), width):
        line_data = data[i:i+width]
        hex_part = " ".join(f"{b:02x}" for b in line_data)
        ascii_part = "".join(chr(b) if 32 <= b <= 126 else "." for b in line_data)
        result.append(f"{offset+i:08x}: {hex_part.ljust(width*3-1)}  |{ascii_part}|")
    return "\n".join(result)

def analyze_parquet_file(file_path):
    """
    Analyze a Parquet file and print detailed information about its structure.
    """
    try:
        # Read the whole file
        with open(file_path, 'rb') as f:
            content = f.read()
        
        print(f"Analyzing Parquet file: {file_path}")
        print(f"File size: {len(content)} bytes")
        
        # Check if it starts and ends with PAR1
        if content[:4] != b'PAR1' or content[-4:] != b'PAR1':
            print("WARNING: File does not have PAR1 magic bytes at start/end!")
        else:
            print("Magic bytes (PAR1) found at start and end")
        
        # Read the footer metadata
        footer_metadata_size = int.from_bytes(content[-8:-4], byteorder='little')
        print(f"Footer metadata size: {footer_metadata_size} bytes")
        
        # Extract the footer metadata bytes
        footer_offset = len(content) - footer_metadata_size - 8
        footer_metadata = content[footer_offset:footer_offset+footer_metadata_size]
        
        print("\n=== First 64 bytes of file ===")
        print(hex_dump(content[:64]))
        
        print("\n=== Footer metadata (first 64 bytes) ===")
        print(hex_dump(footer_metadata[:64]))
        
        print("\n=== Last 64 bytes of file ===")
        print(hex_dump(content[-64:]))
        
        # Use PyArrow to get the file metadata
        metadata = pq.read_metadata(file_path)
        print("\n=== File Metadata ===")
        print(f"Version: {metadata.format_version}")
        print(f"Created by: {metadata.created_by}")
        print(f"Num columns: {metadata.num_columns}")
        print(f"Num rows: {metadata.num_rows}")
        print(f"Num row groups: {metadata.num_row_groups}")
        
        # Print schema
        print("\n=== Schema ===")
        schema = metadata.schema
        for i in range(metadata.num_columns):
            column = schema.column(i)
            print(f"Column {i}: {column.name} (type: {column.physical_type}, "
                  f"repetition: {column.repetition}, length: {column.length})")
            
        # Examine each row group
        print("\n=== Row Groups ===")
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            print(f"Row Group {i}:")
            print(f"  Num rows: {row_group.num_rows}")
            print(f"  Total byte size: {row_group.total_byte_size}")
            
            # Print column chunk information
            print("  Column Chunks:")
            for j in range(row_group.num_columns):
                col = row_group.column(j)
                print(f"    Column {j}: {col.path_in_schema} "
                      f"(offset: {col.file_offset}, "
                      f"size: {col.total_compressed_size})")
                
                # If this is the first row group, extract column data
                if i == 0:
                    # Extract column chunk bytes
                    chunk_offset = col.file_offset
                    chunk_size = col.total_compressed_size
                    chunk_data = content[chunk_offset:chunk_offset+chunk_size]
                    
                    print(f"      Column Chunk Data (first 32 bytes):")
                    print(f"      {hex_dump(chunk_data[:32], offset=chunk_offset, width=16)}")
        
    except Exception as e:
        print(f"Error analyzing file: {e}")

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <parquet_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found")
        sys.exit(1)
        
    analyze_parquet_file(file_path)

if __name__ == "__main__":
    main() 