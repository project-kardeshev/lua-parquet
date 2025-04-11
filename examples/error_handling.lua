-- error_handling.lua
-- Example demonstrating error handling and diagnostics

local parquet = require("parquet")

print("Parquet Error Handling Example")
print("---------------------------------\n")

-- Example 1: Regular (throwing) API
print("Example 1: Using the regular API (throws errors)")
print("Attempting to create a file with an invalid schema type...\n")

local function example1()
    local schema = "not a table" -- Invalid schema type
    
    -- This will throw an error
    local result = pcall(function()
        parquet.write(schema, {})
    end)
    
    -- This code won't be reached due to the error
    print("This won't be printed")
end

local ok, err = pcall(example1)
if not ok then
    print("Caught error:")
    print(err)
    print()
end

-- Example 2: Safe API
print("Example 2: Using the safe API (returns nil + error message)")
print("Attempting to create a file with an invalid column type...\n")

local schema = {
    { name = "id", type = "UNKNOWN_TYPE" } -- Invalid type
}

local rows = {
    { id = 1 }
}

local result, err = parquet.write_safe(schema, rows)
if not result then
    print("Error returned from safe API:")
    print(err)
    print()
end

-- Example 3: Using try/catch pattern
print("Example 3: Using the try/catch pattern")
print("Attempting to create a file with a value out of range...\n")

local schema = {
    { name = "id", type = "INT32" }
}

local rows = {
    { id = 2147483648 } -- Out of INT32 range
}

local success, result = parquet.try(function()
    return parquet.write(schema, rows)
end)

if success then
    print("Operation succeeded!")
    print("Result length: " .. #result .. " bytes")
else
    print("Operation failed:")
    print(result)
    print()
end

-- Example 4: Accessing the last error
print("Example 4: Accessing the last error")
print("Attempting to create a file with missing required values...\n")

local schema = {
    { name = "id", type = "INT32" },
    { name = "name", type = "BYTE_ARRAY" }
}

local rows = {
    { id = 1 } -- Missing 'name' field
}

-- Clear any previous errors
parquet.clear_last_error()

local ok = pcall(function()
    parquet.write(schema, rows)
end)

if not ok then
    local last_error = parquet.get_last_error()
    
    print("Last error information:")
    print("  Error code: " .. tostring(last_error.code))
    print("  Message: " .. tostring(last_error.message))
    print("  Context:")
    
    if last_error.context then
        for key, value in pairs(last_error.context) do
            print("    " .. key .. ": " .. tostring(value))
        end
    end
end

-- Example 5: Handling specific error codes
print("\nExample 5: Handling specific error codes")
print("Attempting various operations and handling errors by code...\n")

local function handle_error(schema, rows)
    local result, err = parquet.write_safe(schema, rows)
    
    if result then
        print("Operation succeeded!")
        return true
    else
        -- Get the last error
        local last_error = parquet.get_last_error()
        
        -- Handle different error types differently
        if last_error.code == parquet.errors.SCHEMA_ERROR then
            print("Schema error detected. Please fix your schema definition.")
        elseif last_error.code == parquet.errors.TYPE_ERROR then
            print("Type error detected. Check your data types.")
        elseif last_error.code == parquet.errors.RANGE_ERROR then
            print("Range error detected. Value is out of range.")
        elseif last_error.code == parquet.errors.MISSING_VALUE_ERROR then
            print("Missing value error. Fill in required columns.")
        elseif last_error.code == parquet.errors.UNSUPPORTED_FEATURE then
            print("Unsupported feature. This feature is not implemented yet.")
        else
            print("Unknown error: " .. tostring(err))
        end
        
        return false
    end
end

-- Test with different error types
print("Testing with schema error:")
handle_error("not a table", {})

print("\nTesting with type error:")
handle_error({ { name = "id", type = "INT32" } }, { { id = "string" } })

print("\nTesting with range error:")
handle_error({ { name = "id", type = "INT32" } }, { { id = 2147483648 } })

print("\nTesting with missing value error:")
handle_error({ { name = "id", type = "INT32" }, { name = "name", type = "BYTE_ARRAY" } }, { { id = 1 } })

print("\nTesting with unsupported feature error:")
handle_error({ { name = "id", type = "UNKNOWN_TYPE" } }, { { id = 1 } }) 