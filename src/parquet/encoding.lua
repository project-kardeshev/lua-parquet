-- parquet/encoding.lua
-- Data encoding for Parquet

local utils = require("parquet.utils")
local errors = require("parquet.errors")

local encoding = {}

-- Plain encoding for INT32
function encoding.encode_int32(values)
    local buffer = utils.string_buffer()
    for i, v in ipairs(values) do
        -- Type and range checking using centralized utilities
        utils.validate_value(v, "INT32", nil, {index = i})
        buffer.add(utils.write_int32(v))
    end
    return buffer.get()
end

-- Plain encoding for INT64
function encoding.encode_int64(values)
    local buffer = utils.string_buffer()
    for i, v in ipairs(values) do
        -- Type and range checking using centralized utilities
        utils.validate_value(v, "INT64", nil, {index = i})
        buffer.add(utils.write_int64(v))
    end
    return buffer.get()
end

-- Plain encoding for DOUBLE
function encoding.encode_double(values)
    local buffer = utils.string_buffer()
    for i, v in ipairs(values) do
        -- Type checking using centralized utilities
        utils.validate_type(v, "number", nil, {index = i})
        buffer.add(utils.write_double(v))
    end
    return buffer.get()
end

-- Plain encoding for FLOAT
function encoding.encode_float(values)
    local buffer = utils.string_buffer()
    for i, v in ipairs(values) do
        -- Type checking using centralized utilities
        utils.validate_type(v, "number", nil, {index = i})
        buffer.add(utils.write_float(v))
    end
    return buffer.get()
end

-- Plain encoding for BOOLEAN
function encoding.encode_boolean(values)
    local buffer = utils.string_buffer()
    for i, v in ipairs(values) do
        -- Type checking using centralized utilities
        utils.validate_type(v, "boolean", nil, {index = i})
        buffer.add(string.char(v and 1 or 0))
    end
    return buffer.get()
end

-- Plain encoding for BYTE_ARRAY
function encoding.encode_byte_array(values)
    local buffer = utils.string_buffer()
    for i, v in ipairs(values) do
        -- Nil check
        if v == nil then
            errors.raise(errors.MISSING_VALUE_ERROR, "Nil value not allowed for BYTE_ARRAY", {
                index = i
            })
        end
        
        -- Type checking using centralized utilities
        utils.validate_type(v, "string", nil, {index = i})
        buffer.add(utils.write_int32(#v))
        buffer.add(v)
    end
    return buffer.get()
end

-- Extract column values from rows
function encoding.extract_column_values(rows, column_index, column_name)
    if type(rows) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Expected table for rows", {
            actual_type = type(rows)
        })
    end
    
    if type(column_name) ~= "string" then
        errors.raise(errors.TYPE_ERROR, "Expected string for column_name", {
            actual_type = type(column_name)
        })
    end
    
    local values = {}
    for i, row in ipairs(rows) do
        if type(row) ~= "table" then
            errors.raise(errors.TYPE_ERROR, "Expected table for row", {
                row_index = i,
                actual_type = type(row)
            })
        end
        values[i] = row[column_name]  -- Preserve nil values with explicit index
    end
    return values
end

-- Encoding dispatcher based on type
function encoding.encode_values(values, type_name)
    if not values or #values == 0 then
        return ""
    end
    
    if type_name == "INT32" then
        return encoding.encode_int32(values)
    elseif type_name == "INT64" then
        return encoding.encode_int64(values)
    elseif type_name == "DOUBLE" then
        return encoding.encode_double(values)
    elseif type_name == "FLOAT" then
        return encoding.encode_float(values)
    elseif type_name == "BOOLEAN" then
        return encoding.encode_boolean(values)
    elseif type_name == "BYTE_ARRAY" then
        return encoding.encode_byte_array(values)
    else
        errors.raise(errors.UNSUPPORTED_FEATURE, "Unsupported type for encoding", {
            type_name = type_name
        })
    end
end

return encoding 