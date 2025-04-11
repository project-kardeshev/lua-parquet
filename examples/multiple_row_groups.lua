-- multiple_row_groups.lua
-- Example demonstrating the use of multiple row groups

local parquet = require("parquet")

print("Creating a large dataset with 10,000 rows")

-- Define schema
local schema = {
    { name = "id", type = "INT32" },
    { name = "value", type = "FLOAT" },
    { name = "name", type = "BYTE_ARRAY" }
}

-- Create a larger dataset
local rows = {}
for i = 1, 10000 do
    rows[i] = {
        id = i,
        value = i * 0.5,
        name = "Name-" .. i
    }
end

print("Writing to multiple_row_groups.parquet with default row groups (all data in one group)")
local default_file = "multiple_row_groups.parquet"
local content = parquet.write(schema, rows)

local file = io.open(default_file, "wb")
file:write(content)
file:close()

print("File size: " .. #content .. " bytes")

print("\nWriting to multiple_row_groups_1000.parquet with row_group_size = 1000")
local group_1000_file = "multiple_row_groups_1000.parquet"
local content_1000 = parquet.write(schema, rows, { row_group_size = 1000 })

local file_1000 = io.open(group_1000_file, "wb")
file_1000:write(content_1000)
file_1000:close()

print("File size: " .. #content_1000 .. " bytes")

print("\nWriting to multiple_row_groups_100.parquet with row_group_size = 100")
local group_100_file = "multiple_row_groups_100.parquet"
local content_100 = parquet.write(schema, rows, { row_group_size = 100 })

local file_100 = io.open(group_100_file, "wb")
file_100:write(content_100)
file_100:close()

print("File size: " .. #content_100 .. " bytes")

print("\nCreating manually specified row groups")
local row_groups = {
    -- Group 1: Odd IDs
    {},
    -- Group 2: Even IDs
    {}
}

for i = 1, 100 do
    if i % 2 == 1 then
        table.insert(row_groups[1], {
            id = i,
            value = i * 0.5,
            name = "Odd-" .. i
        })
    else
        table.insert(row_groups[2], {
            id = i,
            value = i * 0.5,
            name = "Even-" .. i
        })
    end
end

print("Writing to manual_row_groups.parquet")
local manual_file = "manual_row_groups.parquet"
local manual_content = parquet.write_row_groups(schema, row_groups)

local manual_file_handle = io.open(manual_file, "wb")
manual_file_handle:write(manual_content)
manual_file_handle:close()

print("File size: " .. #manual_content .. " bytes")

print("\nSummary:")
print("- Default (single row group): " .. #content .. " bytes")
print("- 10 row groups (1000 rows each): " .. #content_1000 .. " bytes")
print("- 100 row groups (100 rows each): " .. #content_100 .. " bytes")
print("- Manual (2 row groups): " .. #manual_content .. " bytes")

print("\nFiles created:")
print("- " .. default_file)
print("- " .. group_1000_file)
print("- " .. group_100_file)
print("- " .. manual_file) 