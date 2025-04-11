import pyarrow.parquet as pq
import pandas as pd

try:
    # Read the parquet file
    table = pq.read_table('test.parquet')
    
    # Convert to pandas DataFrame for easy viewing
    df = table.to_pandas()
    
    print("Successfully read Parquet file!")
    print("\nContents:")
    print(df)
    
    print("\nSchema:")
    print(table.schema)
    
except Exception as e:
    print(f"Error reading Parquet file: {str(e)}") 