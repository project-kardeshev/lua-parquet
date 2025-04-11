-- parquet/schema.lua
-- Schema element encoding for Parquet

local thrift = require("parquet.thrift")
local errors = require("parquet.errors")
local utils = require("parquet.utils")

local schema = {}

-- Encode schema element using direct Thrift field writing
function schema.encode_element(name, type_value, num_children, repetition_type)
    local result = ""
    
    -- Validate inputs
    if name ~= nil and type(name) ~= "string" then
        errors.raise(errors.TYPE_ERROR, "Schema element name must be a string", {
            actual_type = type(name)
        })
    end
    
    if type_value ~= nil and type(type_value) ~= "number" then
        errors.raise(errors.TYPE_ERROR, "Schema element type must be a number", {
            actual_type = type(type_value)
        })
    end
    
    if not utils.is_integer(num_children) then
        errors.raise(errors.TYPE_ERROR, "Number of children must be an integer", {
            actual_type = type(num_children),
            actual_value = num_children
        })
    end
    
    if repetition_type ~= nil and type(repetition_type) ~= "number" then
        errors.raise(errors.TYPE_ERROR, "Repetition type must be a number", {
            actual_type = type(repetition_type)
        })
    end
    
    -- Field order matters in Parquet, so we need to write in the expected order
    result = result .. thrift.write_field(thrift.TType.I32, 1, type_value)
    result = result .. thrift.write_field(thrift.TType.I32, 3, repetition_type or thrift.FieldRepetitionType.REQUIRED)
    
    if name then
        result = result .. thrift.write_field(thrift.TType.STRING, 4, name)
    end
    
    result = result .. thrift.write_field(thrift.TType.I32, 5, num_children)
    result = result .. thrift.write_stop()
    
    return result
end

-- Encode page header
function schema.encode_page_header(uncompressed_size, compressed_size, num_values)
    local result = ""
    
    -- Validate inputs
    if not utils.is_integer(uncompressed_size) then
        errors.raise(errors.TYPE_ERROR, "Uncompressed size must be an integer", {
            actual_type = type(uncompressed_size),
            actual_value = uncompressed_size
        })
    end
    
    if not utils.is_integer(compressed_size) then
        errors.raise(errors.TYPE_ERROR, "Compressed size must be an integer", {
            actual_type = type(compressed_size),
            actual_value = compressed_size
        })
    end
    
    if not utils.is_integer(num_values) then
        errors.raise(errors.TYPE_ERROR, "Number of values must be an integer", {
            actual_type = type(num_values),
            actual_value = num_values
        })
    end
    
    -- PageType: DATA_PAGE = 0
    result = result .. thrift.write_field(thrift.TType.I32, 1, 0)
    -- Uncompressed size
    result = result .. thrift.write_field(thrift.TType.I32, 2, uncompressed_size)
    -- Compressed size
    result = result .. thrift.write_field(thrift.TType.I32, 3, compressed_size)
    
    -- Data page header
    local data_header = ""
    data_header = data_header .. thrift.write_field(thrift.TType.I32, 1, num_values)
    data_header = data_header .. thrift.write_field(thrift.TType.I32, 2, thrift.Encoding.PLAIN)  -- Encoding
    data_header = data_header .. thrift.write_field(thrift.TType.I32, 3, thrift.Encoding.PLAIN)  -- Definition levels encoding
    data_header = data_header .. thrift.write_field(thrift.TType.I32, 4, thrift.Encoding.PLAIN)  -- Repetition levels encoding
    data_header = data_header .. thrift.write_stop()
    
    result = result .. thrift.write_field(thrift.TType.STRUCT, 5, data_header)
    result = result .. thrift.write_stop()
    
    return result
end

-- Encode column metadata
function schema.encode_column_metadata(type_value, path_in_schema, num_values, data_page_offset, page_size)
    local result = ""
    
    -- Validate inputs
    if type(type_value) ~= "number" then
        errors.raise(errors.TYPE_ERROR, "Type value must be a number", {
            actual_type = type(type_value)
        })
    end
    
    if type(path_in_schema) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Path in schema must be a table", {
            actual_type = type(path_in_schema)
        })
    end
    
    for i, path_elem in ipairs(path_in_schema) do
        if type(path_elem) ~= "string" then
            errors.raise(errors.TYPE_ERROR, "Path element must be a string", {
                index = i,
                actual_type = type(path_elem)
            })
        end
    end
    
    if not utils.is_integer(num_values) then
        errors.raise(errors.TYPE_ERROR, "Number of values must be an integer", {
            actual_type = type(num_values),
            actual_value = num_values
        })
    end
    
    if not utils.is_integer(data_page_offset) then
        errors.raise(errors.TYPE_ERROR, "Data page offset must be an integer", {
            actual_type = type(data_page_offset),
            actual_value = data_page_offset
        })
    end
    
    if not utils.is_integer(page_size) then
        errors.raise(errors.TYPE_ERROR, "Page size must be an integer", {
            actual_type = type(page_size),
            actual_value = page_size
        })
    end
    
    -- Type
    result = result .. thrift.write_field(thrift.TType.I32, 1, type_value)
    
    -- Encodings (only PLAIN for now)
    result = result .. thrift.write_list(2, thrift.TType.I32, {thrift.Encoding.PLAIN})
    
    -- Path in schema
    result = result .. thrift.write_list(3, thrift.TType.STRING, path_in_schema)
    
    -- Codec
    result = result .. thrift.write_field(thrift.TType.I32, 4, thrift.CompressionCodec.UNCOMPRESSED)
    
    -- Num values
    result = result .. thrift.write_field(thrift.TType.I64, 5, num_values)
    
    -- Total uncompressed size
    result = result .. thrift.write_field(thrift.TType.I64, 6, page_size)
    
    -- Total compressed size (same as uncompressed)
    result = result .. thrift.write_field(thrift.TType.I64, 7, page_size)
    
    -- Data page offset
    result = result .. thrift.write_field(thrift.TType.I64, 8, data_page_offset)
    
    result = result .. thrift.write_stop()
    
    return result
end

-- Encode file metadata
function schema.encode_file_metadata(schema_elements, schema_version, num_rows, row_groups)
    local result = ""
    
    -- Validate inputs
    if type(schema_elements) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Schema elements must be a table", {
            actual_type = type(schema_elements)
        })
    end
    
    if not utils.is_integer(schema_version) then
        errors.raise(errors.TYPE_ERROR, "Schema version must be an integer", {
            actual_type = type(schema_version),
            actual_value = schema_version
        })
    end
    
    if not utils.is_integer(num_rows) then
        errors.raise(errors.TYPE_ERROR, "Number of rows must be an integer", {
            actual_type = type(num_rows),
            actual_value = num_rows
        })
    end
    
    if type(row_groups) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Row groups must be a table", {
            actual_type = type(row_groups)
        })
    end
    
    -- Version
    result = result .. thrift.write_field(thrift.TType.I32, 1, schema_version)
    
    -- Schema as a list
    result = result .. thrift.write_list(2, thrift.TType.STRUCT, schema_elements)
    
    -- Row groups as a list
    result = result .. thrift.write_list(3, thrift.TType.STRUCT, row_groups)
    
    -- Num rows
    result = result .. thrift.write_field(thrift.TType.I64, 5, num_rows)
    
    -- Creator (optional)
    result = result .. thrift.write_field(thrift.TType.STRING, 6, "lua-parquet")
    
    result = result .. thrift.write_stop()
    
    return result
end

-- Encode row group metadata
function schema.encode_row_group(total_byte_size, num_rows, column_chunks)
    local result = ""
    
    -- Validate inputs
    if not utils.is_integer(total_byte_size) then
        errors.raise(errors.TYPE_ERROR, "Total byte size must be an integer", {
            actual_type = type(total_byte_size),
            actual_value = total_byte_size
        })
    end
    
    if not utils.is_integer(num_rows) then
        errors.raise(errors.TYPE_ERROR, "Number of rows must be an integer", {
            actual_type = type(num_rows),
            actual_value = num_rows
        })
    end
    
    if type(column_chunks) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Column chunks must be a table", {
            actual_type = type(column_chunks)
        })
    end
    
    -- Total byte size
    result = result .. thrift.write_field(thrift.TType.I64, 1, total_byte_size)
    
    -- Num rows
    result = result .. thrift.write_field(thrift.TType.I64, 2, num_rows)
    
    -- Column chunks
    result = result .. thrift.write_list(3, thrift.TType.STRUCT, column_chunks)
    
    result = result .. thrift.write_stop()
    
    return result
end

-- Encode column chunk metadata
function schema.encode_column_chunk(file_offset, meta_data)
    local result = ""
    
    -- Validate inputs
    if not utils.is_integer(file_offset) then
        errors.raise(errors.TYPE_ERROR, "File offset must be an integer", {
            actual_type = type(file_offset),
            actual_value = file_offset
        })
    end
    
    if type(meta_data) ~= "string" then
        errors.raise(errors.TYPE_ERROR, "Metadata must be a pre-serialized string", {
            actual_type = type(meta_data)
        })
    end
    
    -- File offset
    result = result .. thrift.write_field(thrift.TType.I64, 1, file_offset)
    
    -- Metadata
    result = result .. thrift.write_field(thrift.TType.STRUCT, 3, meta_data)
    
    result = result .. thrift.write_stop()
    
    return result
end

return schema 