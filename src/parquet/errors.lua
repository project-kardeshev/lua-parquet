-- parquet/errors.lua
-- Error handling and diagnostics for Parquet writer

local errors = {}

-- Error code constants
errors.SCHEMA_ERROR = "SCHEMA_ERROR"          -- Schema-related errors
errors.TYPE_ERROR = "TYPE_ERROR"              -- Type validation errors
errors.VALUE_ERROR = "VALUE_ERROR"            -- Value validation errors
errors.RANGE_ERROR = "RANGE_ERROR"            -- Range validation errors
errors.MISSING_VALUE_ERROR = "MISSING_VALUE_ERROR" -- Missing value errors
errors.ENCODING_ERROR = "ENCODING_ERROR"      -- Data encoding errors
errors.IO_ERROR = "IO_ERROR"                  -- I/O related errors
errors.THRIFT_ERROR = "THRIFT_ERROR"          -- Thrift encoding errors
errors.UNSUPPORTED_FEATURE = "UNSUPPORTED_FEATURE" -- Unsupported feature
errors.INTERNAL_ERROR = "INTERNAL_ERROR"      -- Internal errors

-- Store the last error
local last_error = {
    code = nil,
    message = nil,
    context = nil
}

-- Create a new error object with diagnostic information
function errors.create_error(code, message, context)
    local err = {
        code = code,
        message = message,
        context = context or {}
    }
    
    -- Store as last error
    last_error = err
    
    return err
end

-- Format an error object for display
function errors.format_error(err)
    local result = string.format("Parquet Error [%s]: %s", err.code, err.message)
    
    -- Add context information if available
    if err.context and next(err.context) then
        result = result .. "\nContext:"
        for key, value in pairs(err.context) do
            result = result .. string.format("\n  %s: %s", key, tostring(value))
        end
    end
    
    return result
end

-- Raise an error (standard Lua error mechanism)
function errors.raise(code, message, context)
    local err = errors.create_error(code, message, context)
    error(errors.format_error(err), 2)
end

-- Get the last error without raising it
function errors.get_last_error()
    return last_error
end

-- Clear the last error
function errors.clear_last_error()
    last_error = {
        code = nil,
        message = nil,
        context = nil
    }
end

-- Try to execute a function and handle errors
-- Returns: success (boolean), result (if successful) or error (if failed)
function errors.try(func, ...)
    local status, result = pcall(func, ...)
    if status then
        return true, result
    else
        -- Capture the error if it's not already a structured error
        if type(result) == "string" then
            -- Try to parse the error message if it's from our system
            local code = result:match("Parquet Error %[([%w_]+)%]")
            if code then
                -- Already a structured error, leave as is
                return false, result
            else
                -- Unstructured error, create internal error
                errors.create_error(errors.INTERNAL_ERROR, result, {
                    traceback = debug.traceback()
                })
                return false, errors.format_error(last_error)
            end
        else
            -- Unknown error type
            errors.create_error(errors.INTERNAL_ERROR, tostring(result), {
                traceback = debug.traceback()
            })
            return false, errors.format_error(last_error)
        end
    end
end

-- Safe version of functions that may raise errors
-- Executes the function and returns nil, error_message on failure
function errors.safe(func)
    return function(...)
        local success, result = errors.try(func, ...)
        if success then
            return result
        else
            return nil, result
        end
    end
end

return errors 