-- parquet/thrift.lua
-- Full Thrift binary protocol encoding/decoding implementation

local utils = require("parquet.utils")
local errors = require("parquet.errors")

local thrift = {}

-- Thrift protocol constants
thrift.VERSION_1 = 0x80010000
thrift.TYPE_MASK = 0x000000ff

-- Thrift encoding constants
thrift.TType = {
    STOP = 0,
    VOID = 1,
    BOOL = 2,
    BYTE = 3,
    DOUBLE = 4,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    STRING = 11,
    STRUCT = 12,
    MAP = 13,
    SET = 14,
    LIST = 15,
    ENUM = 16,
    BINARY = 17,
    UUID = 18
}

-- Parquet constants
thrift.Type = {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7
}

thrift.FieldRepetitionType = {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2
}

thrift.Encoding = {
    PLAIN = 0,
    RLE = 3,
    BIT_PACKED = 4
}

thrift.CompressionCodec = {
    UNCOMPRESSED = 0
}

-- Thrift encoding functions

-- Write a message header
function thrift.write_message_begin(name, type_id, seq_id)
    local result = utils.write_int32(thrift.VERSION_1 | type_id)
    result = result .. utils.write_int32(#name)
    result = result .. name
    result = result .. utils.write_int32(seq_id)
    return result
end

function thrift.write_message_end()
    return ""
end

-- Write a struct header (no contents, just marks the beginning)
function thrift.write_struct_begin(name)
    return ""
end

function thrift.write_struct_end()
    return ""
end

-- Write a field header with type and ID
function thrift.write_field(type_id, field_id, value)
    if type(field_id) ~= "number" then
        errors.raise(errors.TYPE_ERROR, "Field ID must be a number", {
            actual_type = type(field_id)
        })
    end

    local result = string.char(type_id) .. utils.write_int16(field_id)
    
    if type_id == thrift.TType.BOOL then
        utils.validate_type(value, "boolean", field_id)
        result = result .. string.char(value and 1 or 0)
    elseif type_id == thrift.TType.BYTE then
        utils.validate_value(value, "BYTE", field_id)
        result = result .. string.char(value)
    elseif type_id == thrift.TType.I16 then
        utils.validate_value(value, "I16", field_id)
        result = result .. utils.write_int16(value)
    elseif type_id == thrift.TType.I32 then
        utils.validate_value(value, "INT32", field_id)
        result = result .. utils.write_int32(value)
    elseif type_id == thrift.TType.I64 then
        utils.validate_value(value, "INT64", field_id)
        result = result .. utils.write_int64(value)
    elseif type_id == thrift.TType.DOUBLE then
        utils.validate_type(value, "number", field_id)
        result = result .. utils.write_double(value)
    elseif type_id == thrift.TType.STRING or type_id == thrift.TType.BINARY then
        utils.validate_type(value, "string", field_id)
        result = result .. utils.write_int32(#value) .. value
    elseif type_id == thrift.TType.STRUCT then
        utils.validate_type(value, "string", field_id)
        result = result .. value  -- Value should be a pre-serialized struct
    else
        errors.raise(errors.UNSUPPORTED_FEATURE, "Unsupported Thrift type", {
            type_id = type_id
        })
    end
    
    return result
end

function thrift.write_field_begin(type_id, field_id)
    return string.char(type_id) .. utils.write_int16(field_id)
end

function thrift.write_field_end()
    return ""
end

function thrift.write_field_stop()
    return string.char(thrift.TType.STOP)
end

-- Encode a value based on its thrift type
function thrift.encode_thrift_value(elem_type, value, field_id, context)
    local utils = require("parquet.utils")
    local errors = require("parquet.errors")
    local ctx = context or {}
    
    if elem_type == thrift.TType.BOOL then
        utils.validate_type(value, "boolean", field_id, ctx)
        return string.char(value and 1 or 0)
    elseif elem_type == thrift.TType.BYTE then
        utils.validate_value(value, "BYTE", field_id, ctx)
        return string.char(value)
    elseif elem_type == thrift.TType.I16 then
        utils.validate_value(value, "I16", field_id, ctx)
        return utils.write_int16(value)
    elseif elem_type == thrift.TType.I32 then
        utils.validate_value(value, "INT32", field_id, ctx)
        return utils.write_int32(value)
    elseif elem_type == thrift.TType.I64 then
        utils.validate_value(value, "INT64", field_id, ctx)
        return utils.write_int64(value)
    elseif elem_type == thrift.TType.DOUBLE then
        utils.validate_type(value, "number", field_id, ctx)
        return utils.write_double(value)
    elseif elem_type == thrift.TType.STRING or elem_type == thrift.TType.BINARY then
        utils.validate_type(value, "string", field_id, ctx)
        return utils.write_int32(#value) .. value
    elseif elem_type == thrift.TType.STRUCT then
        utils.validate_type(value, "string", field_id, ctx)
        return value  -- Pre-serialized struct
    else
        errors.raise(errors.UNSUPPORTED_FEATURE, "Unsupported Thrift type", {
            elem_type = elem_type,
            field_id = field_id,
            context = ctx
        })
        return ""
    end
end

-- List encoding
function thrift.write_list_begin(elem_type, size)
    return string.char(elem_type) .. utils.write_int32(size)
end

function thrift.write_list(field_id, elem_type, values, value_encoder)
    local utils = require("parquet.utils")
    local errors = require("parquet.errors")
    
    if type(values) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Expected table for list values", {
            field_id = field_id,
            actual_type = type(values)
        })
    end
    
    local buffer = utils.string_buffer()
    
    -- Write list header
    buffer.add(string.char(thrift.TType.LIST) .. utils.write_int16(field_id))
    buffer.add(thrift.write_list_begin(elem_type, #values))
    
    -- Write elements
    for i, value in ipairs(values) do
        if value_encoder then
            buffer.add(value_encoder(value))
        else
            local context = { index = i }
            buffer.add(thrift.encode_thrift_value(elem_type, value, field_id, context))
        end
    end
    
    return buffer.get()
end

function thrift.write_list_end()
    return ""
end

-- Map encoding
function thrift.write_map_begin(key_type, val_type, size)
    return string.char(key_type) .. string.char(val_type) .. utils.write_int32(size)
end

function thrift.write_map(field_id, key_type, val_type, map_table, key_encoder, val_encoder)
    local result = string.char(thrift.TType.MAP) .. utils.write_int16(field_id)
    
    local size = 0
    for _ in pairs(map_table) do size = size + 1 end
    
    result = result .. thrift.write_map_begin(key_type, val_type, size)
    
    for k, v in pairs(map_table) do
        -- Encode key
        if key_encoder then
            result = result .. key_encoder(k)
        elseif key_type == thrift.TType.BOOL then
            result = result .. string.char(k and 1 or 0)
        elseif key_type == thrift.TType.BYTE then
            result = result .. string.char(k)
        elseif key_type == thrift.TType.I16 then
            result = result .. utils.write_int16(k)
        elseif key_type == thrift.TType.I32 then
            result = result .. utils.write_int32(k)
        elseif key_type == thrift.TType.I64 then
            result = result .. utils.write_int64(k)
        elseif key_type == thrift.TType.STRING or key_type == thrift.TType.BINARY then
            result = result .. utils.write_int32(#k) .. k
        end
        
        -- Encode value
        if val_encoder then
            result = result .. val_encoder(v)
        elseif val_type == thrift.TType.BOOL then
            result = result .. string.char(v and 1 or 0)
        elseif val_type == thrift.TType.BYTE then
            result = result .. string.char(v)
        elseif val_type == thrift.TType.I16 then
            result = result .. utils.write_int16(v)
        elseif val_type == thrift.TType.I32 then
            result = result .. utils.write_int32(v)
        elseif val_type == thrift.TType.I64 then
            result = result .. utils.write_int64(v)
        elseif val_type == thrift.TType.DOUBLE then
            result = result .. utils.write_double(v)
        elseif val_type == thrift.TType.STRING or val_type == thrift.TType.BINARY then
            result = result .. utils.write_int32(#v) .. v
        elseif val_type == thrift.TType.STRUCT then
            result = result .. v  -- Pre-serialized struct
        end
    end
    
    return result
end

function thrift.write_map_end()
    return ""
end

-- Set encoding (implemented as a list in binary protocol)
function thrift.write_set_begin(elem_type, size)
    return thrift.write_list_begin(elem_type, size)
end

function thrift.write_set(field_id, elem_type, set_values, value_encoder)
    -- Sets are implemented as lists in binary protocol
    return thrift.write_list(field_id, elem_type, set_values, value_encoder)
end

function thrift.write_set_end()
    return ""
end

-- Simple stop marker
function thrift.write_stop()
    return string.char(thrift.TType.STOP)
end

-- Utility to encode a complete struct with multiple fields
function thrift.encode_struct(fields)
    local result = ""
    for _, field in ipairs(fields) do
        result = result .. thrift.write_field(field.type_id, field.id, field.value)
    end
    result = result .. thrift.write_stop()
    return result
end

return thrift 