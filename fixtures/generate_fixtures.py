#!/usr/bin/env python3
"""
Generate reference Parquet files for testing Lua Parquet writer.
This script creates simple Parquet files with known schemas and data.
"""

import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

OUTPUT_DIR = "."

def create_simple_int32_parquet():
    """Create a simple Parquet file with a single INT32 column."""
    # Schema: one INT32 column named "id"
    schema = pa.schema([
        pa.field("id", pa.int32())
    ])
    
    # Data: [1, 2, 3]
    data = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32())
    }, schema=schema)
    
    # Write to Parquet file
    output_path = os.path.join(OUTPUT_DIR, "int32_only.parquet")
    pq.write_table(
        data, 
        output_path,
        compression="UNCOMPRESSED",
        version="1.0"
    )
    
    print(f"Created {output_path}")
    
    # Hex dump the first and last bytes
    dump_file_info(output_path)

def create_simple_str_parquet():
    """Create a simple Parquet file with a single BYTE_ARRAY (string) column."""
    # Schema: one BYTE_ARRAY column named "name"
    schema = pa.schema([
        pa.field("name", pa.string())
    ])
    
    # Data: ["Alice", "Bob", "Charlie"]
    data = pa.table({
        "name": pa.array(["Alice", "Bob", "Charlie"])
    }, schema=schema)
    
    # Write to Parquet file
    output_path = os.path.join(OUTPUT_DIR, "string_only.parquet")
    pq.write_table(
        data, 
        output_path,
        compression="UNCOMPRESSED",
        version="1.0"
    )
    
    print(f"Created {output_path}")
    
    # Hex dump the first and last bytes
    dump_file_info(output_path)

def create_mixed_types_parquet():
    """Create a Parquet file with both INT32 and BYTE_ARRAY columns."""
    # Schema: INT32 "id" and BYTE_ARRAY "name"
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string())
    ])
    
    # Data: ids=[1, 2, 3], names=["Alice", "Bob", "Charlie"]
    data = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "name": pa.array(["Alice", "Bob", "Charlie"])
    }, schema=schema)
    
    # Write to Parquet file
    output_path = os.path.join(OUTPUT_DIR, "mixed_types.parquet")
    pq.write_table(
        data, 
        output_path,
        compression="UNCOMPRESSED",
        version="1.0"
    )
    
    print(f"Created {output_path}")
    
    # Hex dump the first and last bytes
    dump_file_info(output_path)

def create_empty_parquet():
    """Create an empty Parquet file with schema but no data."""
    # Schema: INT32 "id" and BYTE_ARRAY "name"
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string())
    ])
    
    # Empty data
    data = pa.table({
        "id": pa.array([], type=pa.int32()),
        "name": pa.array([])
    }, schema=schema)
    
    # Write to Parquet file
    output_path = os.path.join(OUTPUT_DIR, "empty.parquet")
    pq.write_table(
        data, 
        output_path,
        compression="UNCOMPRESSED",
        version="1.0"
    )
    
    print(f"Created {output_path}")
    
    # Hex dump the first and last bytes
    dump_file_info(output_path)

def dump_file_info(file_path):
    """Dump file size and hex representation of first/last bytes."""
    with open(file_path, 'rb') as f:
        content = f.read()
    
    size = len(content)
    header = content[:16]
    footer = content[-16:]
    
    print(f"  Size: {size} bytes")
    print(f"  Header: {header.hex(' ')}")
    print(f"  Footer: {footer.hex(' ')}")
    
    # Also read and display metadata
    metadata = pq.read_metadata(file_path)
    print(f"  Metadata: {metadata}")
    print(f"  Num columns: {metadata.num_columns}")
    print(f"  Num rows: {metadata.num_rows}")
    print(f"  Num row groups: {metadata.num_row_groups}")
    
    # If there are row groups, print their details
    if metadata.num_row_groups > 0:
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            print(f"  Row group {i}:")
            print(f"    Num rows: {row_group.num_rows}")
            for j in range(row_group.num_columns):
                column = row_group.column(j)
                print(f"    Column {j}: {column}")
    
    print("")

def main():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate reference files
    create_simple_int32_parquet()
    create_simple_str_parquet()
    create_mixed_types_parquet()
    create_empty_parquet()

if __name__ == "__main__":
    main() 