You're a senior Lua systems engineer building a minimal Parquet v1.0 file writer from scratch, with no dependencies beyond native Lua 5.3+.

**Goal**: Write a Lua module (`parquet_writer.lua`) that generates a valid `.parquet` file containing flat table data. The file should support:

- Writing a table of rows (list of maps) with a known schema
- Plain encoding (no dictionary or RLE)
- No compression (leave data pages uncompressed)
- Single row group and single data page per column
- `INT32` and `BYTE_ARRAY` types only

You must:
1. Write the necessary metadata structures using Thrift binary encoding manually
2. Encode data using Parquet's plain encoding rules
3. Assemble the Parquet file format correctly:
   - Header (`PAR1`)
   - Data pages for each column
   - RowGroup structure
   - File metadata and footer
   - Footer length and closing magic bytes

Constraints:
- Don't use any external libraries
- Write your own Thrift encoder for only the fields needed (e.g. `SchemaElement`, `FileMetaData`, `ColumnChunk`)
- Build the output as a `string` or binary-compatible Lua table
- Write helpers for endian-safe binary writing

**Example usage:**

```lua
local parquet = require("parquet_writer")

local schema = {
  { name = "id", type = "INT32" },
  { name = "name", type = "BYTE_ARRAY" },
}

local rows = {
  { id = 1, name = "Alice" },
  { id = 2, name = "Bob" },
}

local parquet_bytes = parquet.write(schema, rows)
```
-- write parquet_bytes to "example.parquet"

Start by implementing the Thrift struct encoding for SchemaElement and FileMetaData, and plain-encoded INT32 column pages.

Once working, expand to support strings (BYTE_ARRAY) and full file footer assembly.

