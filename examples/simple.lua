-- simple.lua
-- Simple example demonstrating Parquet file creation

local parquet = require("parquet")

-- Define schema
local schema = {
    { name = "id", type = "INT32" },
    { name = "name", type = "BYTE_ARRAY" },
}

-- Create test data
local rows = {
    { id = 1, name = "Alice" },
    { id = 2, name = "Bob" },
    { id = 3, name = "Charlie" },
}

-- Write parquet file
local parquet_bytes = parquet.write(schema, rows)

-- Write to file
local file = io.open("output.parquet", "wb")
file:write(parquet_bytes)
file:close()

print("Parquet file written successfully to output.parquet")
print("File size: " .. #parquet_bytes .. " bytes")

-- Print the first and last few bytes as hex
local function hex_dump(str, length)
    local result = ""
    local len = math.min(#str, length)
    for i = 1, len do
        result = result .. string.format("%02X ", str:byte(i))
    end
    return result
end

print("First 16 bytes: " .. hex_dump(parquet_bytes, 16))
print("Last 16 bytes: " .. hex_dump(parquet_bytes:sub(-16), 16)) 