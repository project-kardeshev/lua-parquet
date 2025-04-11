-- parquet_writer.lua
-- A minimal Parquet v1.0 file writer implementation

local M = {}

-- Binary encoding utilities
local function write_int16(value)
    local bytes = {}
    for i = 0, 1 do
        bytes[i + 1] = string.char((value >> (i * 8)) & 0xFF)
    end
    return table.concat(bytes)
end

local function write_int32(value)
    local bytes = {}
    for i = 0, 3 do
        bytes[i + 1] = string.char((value >> (i * 8)) & 0xFF)
    end
    return table.concat(bytes)
end

local function write_int64(value)
    local bytes = {}
    for i = 0, 7 do
        bytes[i + 1] = string.char((value >> (i * 8)) & 0xFF)
    end
    return table.concat(bytes)
end

-- Thrift encoding constants
local TType = {
    STOP = 0,
    BOOL = 2,
    BYTE = 3,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    STRING = 11,
    STRUCT = 12,
    MAP = 13,
    SET = 14,
    LIST = 15
}

-- Parquet type constants
local Type = {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7
}

-- Repetition type constants
local FieldRepetitionType = {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2
}

-- Encoding constants
local Encoding = {
    PLAIN = 0,
    RLE = 3,
    BIT_PACKED = 4
}

-- Compression codec constants
local CompressionCodec = {
    UNCOMPRESSED = 0
}

-- Thrift encoding functions
local function write_thrift_field(type_id, field_id, value)
    local result = string.char(type_id) .. write_int16(field_id)
    
    if type_id == TType.BOOL then
        result = result .. string.char(value and 1 or 0)
    elseif type_id == TType.I32 then
        result = result .. write_int32(value)
    elseif type_id == TType.I64 then
        result = result .. write_int64(value)
    elseif type_id == TType.STRING then
        result = result .. write_int32(#value) .. value
    end
    
    return result
end

local function write_thrift_list(field_id, elem_type, values)
    local result = string.char(TType.LIST) .. write_int16(field_id)
    result = result .. string.char(elem_type) .. write_int32(#values)
    
    for _, value in ipairs(values) do
        if elem_type == TType.I32 then
            result = result .. write_int32(value)
        elseif elem_type == TType.STRING then
            result = result .. write_int32(#value) .. value
        elseif elem_type == TType.STRUCT then
            result = result .. value
        end
    end
    
    return result
end

local function write_thrift_stop()
    return string.char(TType.STOP)
end

-- Encode schema element
local function encode_schema_element(name, type_value, num_children, repetition_type)
    local result = ""
    
    if name then
        result = result .. write_thrift_field(TType.STRING, 4, name)
    end
    
    result = result .. write_thrift_field(TType.I32, 1, type_value)
    result = result .. write_thrift_field(TType.I32, 5, num_children)
    result = result .. write_thrift_field(TType.I32, 3, repetition_type or FieldRepetitionType.REQUIRED)
    result = result .. write_thrift_stop()
    
    return result
end

-- Plain encoding for INT32
local function encode_int32_values(values)
    local result = ""
    for _, v in ipairs(values) do
        result = result .. write_int32(v)
    end
    return result
end

-- Plain encoding for BYTE_ARRAY
local function encode_byte_array_values(values)
    local result = ""
    for _, v in ipairs(values) do
        result = result .. write_int32(#v) .. v
    end
    return result
end

-- Encode page header
local function encode_page_header(uncompressed_size, compressed_size, num_values)
    local result = ""
    
    -- PageType: DATA_PAGE = 0
    result = result .. write_thrift_field(TType.I32, 1, 0)
    -- Uncompressed size
    result = result .. write_thrift_field(TType.I32, 2, uncompressed_size)
    -- Compressed size
    result = result .. write_thrift_field(TType.I32, 3, compressed_size)
    
    -- Data page header
    local data_header = ""
    data_header = data_header .. write_thrift_field(TType.I32, 1, num_values)
    data_header = data_header .. write_thrift_field(TType.I32, 2, Encoding.PLAIN)  -- Encoding
    data_header = data_header .. write_thrift_field(TType.I32, 3, Encoding.PLAIN)  -- Definition levels encoding
    data_header = data_header .. write_thrift_field(TType.I32, 4, Encoding.PLAIN)  -- Repetition levels encoding
    data_header = data_header .. write_thrift_stop()
    
    result = result .. write_thrift_field(TType.STRUCT, 5, data_header)
    result = result .. write_thrift_stop()
    
    return result
end

-- Encode column metadata
local function encode_column_metadata(type_value, path_in_schema, num_values, data_page_offset, page_size)
    local result = ""
    
    -- Type
    result = result .. write_thrift_field(TType.I32, 1, type_value)
    
    -- Encodings (only PLAIN for now)
    result = result .. write_thrift_list(2, TType.I32, {Encoding.PLAIN})
    
    -- Path in schema
    result = result .. write_thrift_list(3, TType.STRING, path_in_schema)
    
    -- Codec
    result = result .. write_thrift_field(TType.I32, 4, CompressionCodec.UNCOMPRESSED)
    
    -- Num values
    result = result .. write_thrift_field(TType.I64, 5, num_values)
    
    -- Total uncompressed size
    result = result .. write_thrift_field(TType.I64, 6, page_size)
    
    -- Total compressed size (same as uncompressed)
    result = result .. write_thrift_field(TType.I64, 7, page_size)
    
    -- Data page offset
    result = result .. write_thrift_field(TType.I64, 8, data_page_offset)
    
    result = result .. write_thrift_stop()
    
    return result
end

-- Create a minimal Parquet file
function M.write(schema, rows)
    -- Simple check to ensure we have exactly 2 columns: INT32 and BYTE_ARRAY
    assert(#schema == 2, "This simplified writer supports exactly 2 columns")
    assert(schema[1].type == "INT32", "First column must be INT32")
    assert(schema[2].type == "BYTE_ARRAY", "Second column must be BYTE_ARRAY")
    
    local id_col = {}
    local name_col = {}
    
    -- Extract column data
    for _, row in ipairs(rows) do
        table.insert(id_col, row[schema[1].name])
        table.insert(name_col, row[schema[2].name])
    end
    
    -- Start with file magic bytes
    local file = "PAR1"
    
    -- Column data and metadata
    local column_data = {}
    local column_metadata = {}
    local column_offsets = {}
    
    -- Prepare and write INT32 column
    local id_values = encode_int32_values(id_col)
    local id_header = encode_page_header(#id_values, #id_values, #id_col)
    table.insert(column_offsets, #file)
    file = file .. id_header .. id_values
    
    -- Prepare and write BYTE_ARRAY column
    local name_values = encode_byte_array_values(name_col)
    local name_header = encode_page_header(#name_values, #name_values, #name_col)
    table.insert(column_offsets, #file)
    file = file .. name_header .. name_values
    
    -- Create schema elements
    local schema_elements = {}
    
    -- Root schema element
    table.insert(schema_elements, encode_schema_element(nil, 0, 2, 0))
    -- INT32 column schema
    table.insert(schema_elements, encode_schema_element(schema[1].name, Type.INT32, 0, FieldRepetitionType.REQUIRED))
    -- BYTE_ARRAY column schema
    table.insert(schema_elements, encode_schema_element(schema[2].name, Type.BYTE_ARRAY, 0, FieldRepetitionType.REQUIRED))
    
    -- Create column metadata
    table.insert(column_metadata, encode_column_metadata(
        Type.INT32, 
        {schema[1].name}, 
        #id_col, 
        column_offsets[1], 
        #id_values
    ))
    
    table.insert(column_metadata, encode_column_metadata(
        Type.BYTE_ARRAY, 
        {schema[2].name}, 
        #name_col, 
        column_offsets[2], 
        #name_values
    ))
    
    -- Create row group
    local row_group = ""
    row_group = row_group .. write_thrift_list(1, TType.STRUCT, column_metadata)
    row_group = row_group .. write_thrift_field(TType.I64, 2, #file - 4) -- Total byte size
    row_group = row_group .. write_thrift_field(TType.I64, 3, #rows) -- Num rows
    row_group = row_group .. write_thrift_stop()
    
    -- Create file metadata
    local file_metadata = ""
    file_metadata = file_metadata .. write_thrift_field(TType.I32, 1, 1) -- Version
    file_metadata = file_metadata .. write_thrift_list(2, TType.STRUCT, schema_elements) -- Schema
    file_metadata = file_metadata .. write_thrift_field(TType.I64, 3, #rows) -- Num rows
    file_metadata = file_metadata .. write_thrift_list(4, TType.STRUCT, {row_group}) -- Row groups
    file_metadata = file_metadata .. write_thrift_field(TType.STRING, 5, "lua-parquet-writer") -- Created by
    file_metadata = file_metadata .. write_thrift_stop()
    
    -- Add metadata length and closing magic
    file = file .. file_metadata .. write_int32(#file_metadata) .. "PAR1"
    
    return file
end

return M 