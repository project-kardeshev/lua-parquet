# lua-parquet

A minimal Parquet v1.0 file writer implemented in pure Lua. This library supports writing tables with INT32 and BYTE_ARRAY columns using plain encoding, with no compression.

## Features

- Write Parquet v1.0 format files from Lua tables
- Support for INT32 and BYTE_ARRAY (string) column types
- Plain encoding (no dictionary or RLE)
- No compression (leaves data pages uncompressed)
- No external dependencies beyond Lua 5.3+
- Single row group with single data page per column

## Installation

You can install via LuaRocks:

```
luarocks install lua-parquet
```

Or directly from the source:

```
git clone https://github.com/username/lua-parquet.git
cd lua-parquet
luarocks make
```

## Usage

```lua
local parquet = require("parquet")

-- Define schema
local schema = {
    { name = "id", type = "INT32" },
    { name = "name", type = "BYTE_ARRAY" },
}

-- Create data
local rows = {
    { id = 1, name = "Alice" },
    { id = 2, name = "Bob" },
    { id = 3, name = "Charlie" },
}

-- Write Parquet data to a binary string
local parquet_bytes = parquet.write(schema, rows)

-- Write to file
local file = io.open("output.parquet", "wb")
file:write(parquet_bytes)
file:close()
```

## Module Structure

- `parquet.lua`: Main module that ties everything together
- `parquet/utils.lua`: Binary encoding utilities
- `parquet/thrift.lua`: Thrift encoding for Parquet metadata
- `parquet/schema.lua`: Schema encoding functions
- `parquet/encoding.lua`: Data encoding for different types
- `parquet/writer.lua`: Main writer implementation

## Development

### Running Tests

To run the tests, you need to install the busted test framework:

```
luarocks install busted
```

Then you can run the tests:

```
export LUA_PATH="./src/?.lua;./src/?/init.lua;$LUA_PATH"
busted
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Limitations

- Currently only supports INT32 and BYTE_ARRAY column types
- No support for nested schemas
- No support for compression
- No support for dictionary encoding
- No support for optional or repeated fields (all fields are required)
- Single row group only 