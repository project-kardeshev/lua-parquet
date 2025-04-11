-- parquet.lua
-- Main module for Parquet writer

local writer = require("parquet.writer")
local errors = require("parquet.errors")

local parquet = {}

-- Write rows to a Parquet file
function parquet.write(schema, rows, options)
    return writer.write_file(schema, rows, options)
end

-- Write manually specified row groups to a Parquet file
function parquet.write_row_groups(schema, row_groups, options)
    return writer.write_row_groups(schema, row_groups, options)
end

-- Safe version of write that returns nil, error_message on failure
parquet.write_safe = errors.safe(parquet.write)

-- Safe version of write_row_groups that returns nil, error_message on failure
parquet.write_row_groups_safe = errors.safe(parquet.write_row_groups)

-- Get the last error that occurred
function parquet.get_last_error()
    return errors.get_last_error()
end

-- Try to execute a function and handle errors
-- Returns: success (boolean), result (if successful) or error (if failed)
function parquet.try(func, ...)
    return errors.try(func, ...)
end

-- Clear the last error
function parquet.clear_last_error()
    errors.clear_last_error()
end

-- Version info
parquet.version = "1.0.0"

-- Error codes
parquet.errors = {
    SCHEMA_ERROR = errors.SCHEMA_ERROR,
    TYPE_ERROR = errors.TYPE_ERROR,
    VALUE_ERROR = errors.VALUE_ERROR,
    RANGE_ERROR = errors.RANGE_ERROR,
    MISSING_VALUE_ERROR = errors.MISSING_VALUE_ERROR,
    ENCODING_ERROR = errors.ENCODING_ERROR,
    IO_ERROR = errors.IO_ERROR,
    THRIFT_ERROR = errors.THRIFT_ERROR,
    UNSUPPORTED_FEATURE = errors.UNSUPPORTED_FEATURE,
    INTERNAL_ERROR = errors.INTERNAL_ERROR
}

return parquet 