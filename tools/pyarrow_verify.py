#!/usr/bin/env python
import sys
import json
import pyarrow.parquet as pq
import pandas as pd
import os

def verify_parquet(file_path, expected_data=None):
    """
    Verify that a Parquet file can be read by PyArrow and optionally check its contents.
    
    Args:
        file_path: Path to the Parquet file
        expected_data: Optional JSON string with expected data to compare against
        
    Returns:
        Dictionary with verification results
    """
    result = {
        "success": False,
        "error": None,
        "row_count": 0,
        "columns": [],
        "data_matches": None,
        "data_sample": None
    }
    
    try:
        # Print some debug info
        print(f"Attempting to read file: {file_path}", file=sys.stderr)
        if not os.path.exists(file_path):
            print(f"ERROR: File does not exist: {file_path}", file=sys.stderr)
            result["error"] = f"File does not exist: {file_path}"
            print(json.dumps(result))
            sys.exit(1)
            
        file_size = os.path.getsize(file_path)
        print(f"File size: {file_size} bytes", file=sys.stderr)
        
        if file_size < 8:
            print(f"ERROR: File is too small to be a valid Parquet file: {file_size} bytes", file=sys.stderr)
            result["error"] = f"File is too small: {file_size} bytes"
            print(json.dumps(result))
            sys.exit(1)
            
        # Check for PAR1 magic bytes at start and end
        with open(file_path, 'rb') as f:
            header = f.read(4)
            f.seek(max(0, file_size - 4))
            footer = f.read(4)
            
        if header != b'PAR1' or footer != b'PAR1':
            print(f"ERROR: File does not have PAR1 magic bytes. Header: {header}, Footer: {footer}", file=sys.stderr)
            result["error"] = f"Not a Parquet file. Missing PAR1 magic bytes."
            print(json.dumps(result))
            sys.exit(1)
        
        # Read the parquet file
        print("Reading parquet file with PyArrow...", file=sys.stderr)
        table = pq.read_table(file_path)
        print("Successfully read table with PyArrow", file=sys.stderr)
        df = table.to_pandas()
        
        # Basic file info
        result["success"] = True
        result["row_count"] = len(df)
        result["columns"] = df.columns.tolist()
        
        # Compare with expected data if provided
        if expected_data:
            try:
                expected = json.loads(expected_data)
                expected_df = pd.DataFrame(expected)
                
                # Check if columns match
                if set(df.columns) != set(expected_df.columns):
                    result["data_matches"] = False
                    result["error"] = f"Column mismatch: got {df.columns.tolist()}, expected {expected_df.columns.tolist()}"
                    return result
                
                # Reorder columns to match for comparison
                expected_df = expected_df[df.columns.tolist()]
                
                # Convert integer columns for proper comparison
                for col in df.columns:
                    if df[col].dtype == 'int64' or df[col].dtype == 'int32':
                        expected_df[col] = expected_df[col].astype(df[col].dtype)
                
                # Check if data matches (allowing for reordering)
                sorted_df = df.sort_values(by=df.columns.tolist()[0]).reset_index(drop=True)
                sorted_expected = expected_df.sort_values(by=df.columns.tolist()[0]).reset_index(drop=True)
                
                # Compare DataFrames
                if sorted_df.equals(sorted_expected):
                    result["data_matches"] = True
                else:
                    result["data_matches"] = False
                    # Include samples of both dataframes for debugging
                    result["actual_data"] = sorted_df.head(5).to_dict(orient="records")
                    result["expected_data"] = sorted_expected.head(5).to_dict(orient="records")
            except Exception as e:
                result["data_matches"] = False
                result["error"] = f"Error comparing data: {str(e)}"
        
        # Include a sample of the data
        result["data_sample"] = df.head(5).to_dict(orient="records")
        
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
    
    return result

if __name__ == "__main__":
    # Check arguments
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "No file path provided"}))
        sys.exit(1)
    
    file_path = sys.argv[1]
    expected_data = None
    
    # If second argument is provided, it's the expected data
    if len(sys.argv) > 2:
        expected_data = sys.argv[2]
    
    result = verify_parquet(file_path, expected_data)
    
    # Print JSON result that can be captured by the calling process
    print(json.dumps(result)) 