# Error Handling in lua-parquet

This document describes the error handling system implemented in the lua-parquet library.

## Overview

lua-parquet provides a structured error handling system that:

1. Categorizes errors by type with error codes
2. Provides rich diagnostic information with context
3. Offers multiple ways to handle errors (exceptions, return values, try/catch)
4. Maintains state for the last error that occurred
5. Ensures consistent error messages across the library

## Error Codes

The following error codes are used to categorize different types of errors:

| Error Code | Description |
|------------|-------------|
| `SCHEMA_ERROR` | Schema definition errors |
| `TYPE_ERROR` | Type validation errors |
| `VALUE_ERROR` | Invalid value errors |
| `RANGE_ERROR` | Value out of valid range errors |
| `MISSING_VALUE_ERROR` | Missing required value errors |
| `ENCODING_ERROR` | Data encoding errors |
| `IO_ERROR` | Input/output related errors |
| `THRIFT_ERROR` | Thrift encoding errors |
| `UNSUPPORTED_FEATURE` | Requested feature not supported |
| `INTERNAL_ERROR` | Internal library errors |

## Error Handling Approaches

### Traditional Exceptions

The standard API functions will raise Lua errors when problems occur. These can be caught using Lua's `pcall` mechanism:

```lua
local ok, err = pcall(function()
    parquet.write(schema, rows)
end)

if not ok then
    print("Error occurred:", err)
end
```

The raised errors include the error code, message, and context information.

### Safe API Functions

For applications that prefer error return values over exceptions, we provide "safe" versions of all API functions that return `nil, error_message` on failure:

```lua
local result, err = parquet.write_safe(schema, rows)
if not result then
    print("Error occurred:", err)
end
```

### Try/Catch Pattern

We provide a `try` function that combines `pcall` with standardized error handling:

```lua
local success, result = parquet.try(function()
    return parquet.write(schema, rows)
end)

if success then
    -- Use result
else
    -- Handle error in result
end
```

### Last Error Information

You can access detailed information about the last error that occurred:

```lua
local last_error = parquet.get_last_error()
if last_error.code then
    print("Error code:", last_error.code)
    print("Error message:", last_error.message)
    for k, v in pairs(last_error.context) do
        print(k, "=", v)
    end
end
```

And clear it when needed:

```lua
parquet.clear_last_error()
```

## Error Contexts

Error messages include rich contextual information to help diagnose the problem. Depending on the error, the context might include:

- Row and column information
- Expected and actual types
- Valid ranges and actual values
- Available alternatives for invalid options
- Field names and indices

## Handling Specific Error Types

You can check the error code to handle different types of errors appropriately:

```lua
local result, err = parquet.write_safe(schema, rows)
if not result then
    local last_error = parquet.get_last_error()
    
    if last_error.code == parquet.errors.SCHEMA_ERROR then
        -- Handle schema issues
    elseif last_error.code == parquet.errors.TYPE_ERROR then
        -- Handle type issues
    elseif last_error.code == parquet.errors.RANGE_ERROR then
        -- Handle range issues
    -- etc.
    end
end
```

## Examples

See `examples/error_handling.lua` and `examples/error_handling_extended.lua` for comprehensive examples of error handling techniques. 