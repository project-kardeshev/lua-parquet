-- parquet/utils.lua
-- Binary utility functions for Parquet encoding

local utils = {}

-- Constants
utils.PARQUET_MAGIC = "PAR1"

-- Binary encoding utilities
function utils.write_int16(value)
    local bytes = {}
    for i = 0, 1 do
        bytes[i + 1] = string.char((value >> (i * 8)) & 0xFF)
    end
    return table.concat(bytes)
end

function utils.write_int32(value)
    local bytes = {}
    for i = 0, 3 do
        bytes[i + 1] = string.char((value >> (i * 8)) & 0xFF)
    end
    return table.concat(bytes)
end

function utils.write_int64(value)
    local bytes = {}
    for i = 0, 7 do
        bytes[i + 1] = string.char((value >> (i * 8)) & 0xFF)
    end
    return table.concat(bytes)
end

-- Pack a double value into IEEE 754 binary representation
function utils.write_double(value)
    -- This library targets Lua 5.3+, so we can use string.pack directly
    return string.pack("<d", value)
end

-- Pack a float value into IEEE 754 binary representation (single precision)
function utils.write_float(value)
    -- This library targets Lua 5.3+, so we can use string.pack directly
    return string.pack("<f", value)
end

-- Binary decoding utilities
function utils.read_int16(bytes, offset)
    offset = offset or 1
    local value = 0
    for i = 0, 1 do
        value = value | (bytes:byte(offset + i) << (i * 8))
    end
    return value, offset + 2
end

function utils.read_int32(bytes, offset)
    offset = offset or 1
    local value = 0
    for i = 0, 3 do
        value = value | (bytes:byte(offset + i) << (i * 8))
    end
    return value, offset + 4
end

function utils.read_int64(bytes, offset)
    offset = offset or 1
    local high, low = 0, 0
    for i = 0, 3 do
        low = low | (bytes:byte(offset + i) << (i * 8))
    end
    for i = 0, 3 do
        high = high | (bytes:byte(offset + i + 4) << (i * 8))
    end
    -- In Lua 5.3+ we could use 64-bit integers, but for compatibility
    -- we'll return two 32-bit integers (low, high)
    return {low = low, high = high}, offset + 8
end

-- Read a string with length prefix
function utils.read_string(bytes, offset)
    offset = offset or 1
    local length, new_offset = utils.read_int32(bytes, offset)
    local str = bytes:sub(new_offset, new_offset + length - 1)
    return str, new_offset + length
end

-- Utility for hex dumping binary data (useful for debugging)
function utils.hex_dump(bytes)
    local result = {}
    for i = 1, #bytes do
        result[i] = string.format("%02X", bytes:byte(i))
    end
    return table.concat(result, " ")
end

-- Get keys of a table as an array
function utils.get_table_keys(tbl)
    local keys = {}
    for k, _ in pairs(tbl) do
        table.insert(keys, k)
    end
    return keys
end

-- Check if a value is an integer (Lua 5.3+ compatible)
function utils.is_integer(value)
    if type(value) ~= "number" then
        return false
    end
    
    -- Use math.type if available (Lua 5.3+)
    if math.type then
        return math.type(value) == "integer"
    else
        -- Fallback for older Lua versions
        return value == math.floor(value)
    end
end

-- Type validation utilities to centralize repeated validation code

-- Validate a value against a type
function utils.validate_type(value, expected_type, field_id, context)
    local errors = require("parquet.errors")
    local ctx = context or {}
    
    local value_type = type(value)
    
    if expected_type == "integer" then
        if not utils.is_integer(value) then
            errors.raise(errors.VALUE_ERROR, "Expected integer value", 
                {field_id = field_id, actual_type = value_type, value = value, context = ctx})
        end
    elseif value_type ~= expected_type then
        errors.raise(errors.TYPE_ERROR, "Invalid value type", 
            {field_id = field_id, expected_type = expected_type, actual_type = value_type, context = ctx})
    end
end

-- Validate integer range
function utils.validate_range(value, min_value, max_value, field_id, context)
    local errors = require("parquet.errors")
    local ctx = context or {}
    
    if value < min_value or value > max_value then
        errors.raise(errors.RANGE_ERROR, "Value out of range", 
            {field_id = field_id, value = value, min_value = min_value, max_value = max_value, context = ctx})
    end
end

-- Type and range constraints for common types
utils.type_constraints = {
    INT32 = {type = "integer", min = -2147483648, max = 2147483647},
    INT64 = {type = "integer", min = -9223372036854775808, max = 9223372036854775807},
    BYTE = {type = "integer", min = 0, max = 255},
    I16 = {type = "integer", min = -32768, max = 32767},
    DOUBLE = {type = "number"},
    FLOAT = {type = "number"},
    BOOLEAN = {type = "boolean"},
    STRING = {type = "string"},
    BYTE_ARRAY = {type = "string"}
}

-- Validate a value against a type constraint
function utils.validate_value(value, type_name, field_id, context)
    local constraint = utils.type_constraints[type_name]
    if not constraint then
        local errors = require("parquet.errors")
        errors.raise(errors.UNSUPPORTED_FEATURE, "Unsupported type", {type_name = type_name})
    end
    
    utils.validate_type(value, constraint.type, field_id, context)
    
    if constraint.min and constraint.max then
        utils.validate_range(value, constraint.min, constraint.max, field_id, context)
    end
end

-- Create a string buffer for efficient concatenation
function utils.string_buffer()
    local buffer = {}
    
    return {
        -- Add string to buffer
        add = function(s)
            buffer[#buffer + 1] = s
            return buffer
        end,
        
        -- Get concatenated string
        get = function()
            return table.concat(buffer)
        end,
        
        -- Get current size
        size = function()
            return #buffer
        end,
        
        -- Clear buffer
        clear = function()
            buffer = {}
        end
    }
end

return utils 