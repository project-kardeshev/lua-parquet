-- error_handling_extended.lua
-- Advanced example demonstrating error handling for different modules

local parquet = require("parquet")
local utils = require("parquet.utils")
local thrift = require("parquet.thrift")
local schema = require("parquet.schema")
local encoding = require("parquet.encoding")
local errors = require("parquet.errors")

print("Parquet Advanced Error Handling Example")
print("=======================================\n")

-- Helper function to safely run a function and display error information
local function try_run(name, func, ...)
    print("Trying:", name)
    print("----------------------------")
    
    local success, result = parquet.try(func, ...)
    
    if success then
        print("SUCCESS!")
        print("Result:", type(result) == "string" and #result .. " bytes" or tostring(result))
    else
        print("FAILED!")
        print("Error:", result)
        
        -- Show more details from last error
        local last_error = parquet.get_last_error()
        if last_error.code then
            print("\nError details:")
            print("  Code:", last_error.code)
            print("  Message:", last_error.message)
            if last_error.context then
                print("  Context:")
                for k, v in pairs(last_error.context) do
                    print("    " .. k .. ":", tostring(v))
                end
            end
        end
    end
    
    print("\n")
end

-- Example 1: Schema validation error
try_run("Creating a schema with an invalid type", function()
    local schema = {
        { name = "id", type = "DECIMAL" } -- Not a supported type
    }
    
    local rows = {
        { id = 1 }
    }
    
    return parquet.write(schema, rows)
end)

-- Example 2: Value validation error
try_run("Writing data with wrong value type", function()
    local schema = {
        { name = "id", type = "INT32" }
    }
    
    local rows = {
        { id = "not a number" } -- String instead of number
    }
    
    return parquet.write(schema, rows)
end)

-- Example 3: Range error
try_run("Writing data with out-of-range value", function()
    local schema = {
        { name = "id", type = "INT32" }
    }
    
    local rows = {
        { id = 2147483648 } -- Exceeds INT32 max
    }
    
    return parquet.write(schema, rows)
end)

-- Example 4: Missing value error
try_run("Writing data with missing required field", function()
    local schema = {
        { name = "id", type = "INT32" },
        { name = "name", type = "BYTE_ARRAY" }
    }
    
    local rows = {
        { id = 1 } -- Missing name field
    }
    
    return parquet.write(schema, rows)
end)

-- Example 5: Thrift encoding error
try_run("Encoding invalid Thrift data", function()
    -- Try to use an unsupported Thrift type
    return thrift.write_field(255, 1, "test") -- 255 is not a valid type
end)

-- Example 6: Schema element error
try_run("Creating invalid schema element", function()
    -- Try with invalid parameters
    return schema.encode_element(123, "not a number", "not a number", "not a number")
end)

-- Example 7: Encoding error
try_run("Encoding invalid float data", function()
    return encoding.encode_float({"not a number"})
end)

-- Example 8: Multiple row groups with invalid data
try_run("Creating multiple row groups with invalid group", function()
    local schema_def = {
        { name = "id", type = "INT32" }
    }
    
    local row_groups = {
        {
            { id = 1 },
            { id = 2 }
        },
        "not a table", -- This should be a table of rows
        {
            { id = 3 },
            { id = 4 }
        }
    }
    
    return parquet.write_row_groups(schema_def, row_groups)
end)

-- Example 9: Using safe API
print("Using the safe API")
print("----------------------------")

local schema = {
    { name = "id", type = "UNKNOWN_TYPE" } -- Invalid type
}

local rows = {
    { id = 1 }
}

local result, err = parquet.write_safe(schema, rows)

print("Result:", result and "Success" or "Failed")
print("Error:", err or "None")
print("\n")

-- Example 10: Handling specific error codes
print("Handling specific error codes")
print("----------------------------")

local function handle_specific_error()
    local schema = {
        { name = "id", type = "INT32" }
    }
    
    local rows = {
        { id = "string value" } -- Wrong type
    }
    
    local result, err = parquet.write_safe(schema, rows)
    if not result then
        local last_error = parquet.get_last_error()
        
        if last_error.code == parquet.errors.VALUE_ERROR then
            print("Value error detected! You need to provide an integer for INT32 fields.")
            if last_error.context.column_name then
                print("Problem with column:", last_error.context.column_name)
            end
            if last_error.context.row_index then
                print("Problem in row:", last_error.context.row_index)
            end
            if last_error.context.actual_type then
                print("Actual type:", last_error.context.actual_type)
            end
            
            -- Here you could implement automatic error correction based on the detailed context
            print("\nAttempting to fix the error...")
            rows[1].id = 1 -- Fix the type issue
            
            -- Try again with fixed data
            result = parquet.write(schema, rows)
            print("Fixed! Successfully created a " .. #result .. " byte Parquet file.")
        else
            print("Unexpected error:", err)
        end
    end
end

handle_specific_error() 