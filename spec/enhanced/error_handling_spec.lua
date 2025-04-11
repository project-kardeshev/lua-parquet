-- error_handling_spec.lua
-- Tests for the error handling and diagnostics system

describe("error handling and diagnostics", function()
    local parquet = require("parquet")
    local errors = require("parquet.errors")
    
    describe("error codes", function()
        it("should expose error code constants", function()
            assert.is_string(parquet.errors.SCHEMA_ERROR)
            assert.is_string(parquet.errors.TYPE_ERROR)
            assert.is_string(parquet.errors.VALUE_ERROR)
            assert.is_string(parquet.errors.RANGE_ERROR)
            assert.is_string(parquet.errors.MISSING_VALUE_ERROR)
            assert.is_string(parquet.errors.ENCODING_ERROR)
            assert.is_string(parquet.errors.IO_ERROR)
            assert.is_string(parquet.errors.THRIFT_ERROR)
            assert.is_string(parquet.errors.UNSUPPORTED_FEATURE)
            assert.is_string(parquet.errors.INTERNAL_ERROR)
        end)
    end)
    
    describe("error raising", function()
        it("should format error messages with code", function()
            local error_message = errors.format_error({
                code = "TEST_ERROR",
                message = "Test error message",
                context = {}
            })
            
            assert.matches("Parquet Error %[TEST_ERROR%]", error_message)
            assert.matches("Test error message", error_message)
        end)
        
        it("should include context in formatted errors", function()
            local error_message = errors.format_error({
                code = "TEST_ERROR",
                message = "Test error message",
                context = {
                    field_name = "test_field",
                    row_index = 42
                }
            })
            
            assert.matches("field_name: test_field", error_message)
            assert.matches("row_index: 42", error_message)
        end)
        
        it("should raise an error with proper formatting", function()
            local ok, err = pcall(function()
                errors.raise("TEST_ERROR", "Test error message", {
                    detail = "extra info"
                })
            end)
            
            assert.is_false(ok)
            assert.matches("Parquet Error %[TEST_ERROR%]", err)
            assert.matches("Test error message", err)
            assert.matches("detail: extra info", err)
        end)
    end)
    
    describe("error handling with try/catch pattern", function()
        it("should handle successful function calls", function()
            local success, result = parquet.try(function(a, b)
                return a + b
            end, 2, 3)
            
            assert.is_true(success)
            assert.equals(5, result)
        end)
        
        it("should handle functions that raise errors", function()
            local success, error_message = parquet.try(function()
                error("Something went wrong")
            end)
            
            assert.is_false(success)
            assert.matches("Something went wrong", error_message)
        end)
        
        it("should handle structured errors", function()
            local success, error_message = parquet.try(function()
                errors.raise("TEST_ERROR", "Structured error", {
                    important = "context"
                })
            end)
            
            assert.is_false(success)
            assert.matches("Parquet Error %[TEST_ERROR%]", error_message)
            assert.matches("Structured error", error_message)
            assert.matches("important: context", error_message)
        end)
    end)
    
    describe("last error storage", function()
        before_each(function()
            parquet.clear_last_error()
        end)
        
        it("should store the last error", function()
            local ok, _ = pcall(function()
                errors.raise("TEST_ERROR", "Test error message", {
                    key = "value"
                })
            end)
            
            local last_error = parquet.get_last_error()
            
            assert.is_false(ok)
            assert.equals("TEST_ERROR", last_error.code)
            assert.equals("Test error message", last_error.message)
            assert.equals("value", last_error.context.key)
        end)
        
        it("should clear the last error", function()
            local ok, _ = pcall(function()
                errors.raise("TEST_ERROR", "Test error message")
            end)
            
            assert.is_false(ok)
            
            parquet.clear_last_error()
            
            local last_error = parquet.get_last_error()
            assert.is_nil(last_error.code)
            assert.is_nil(last_error.message)
        end)
    end)
    
    describe("safe function versions", function()
        it("should handle successful calls", function()
            local schema = {
                { name = "id", type = "INT32" }
            }
            
            local rows = {
                { id = 1 }
            }
            
            local result = parquet.write_safe(schema, rows)
            
            assert.is_string(result)
            assert.is_true(#result > 0)
        end)
        
        it("should return nil, error_message on failure", function()
            local schema = {
                { name = "id", type = "INVALID_TYPE" } -- Invalid type
            }
            
            local rows = {
                { id = 1 }
            }
            
            local result, error_message = parquet.write_safe(schema, rows)
            
            assert.is_nil(result)
            assert.is_string(error_message)
            assert.matches("Parquet Error", error_message)
            assert.matches("UNSUPPORTED_FEATURE", error_message)
        end)
    end)
    
    describe("specific error cases", function()
        it("should provide detailed diagnostics for schema errors", function()
            local schema = "not a table" -- Invalid schema
            
            local ok, err = pcall(function()
                parquet.write(schema, {})
            end)
            
            assert.is_false(ok)
            assert.matches("SCHEMA_ERROR", err)
            assert.matches("Schema must be a table", err)
            assert.matches("schema_type: string", err)
        end)
        
        it("should provide detailed diagnostics for type errors", function()
            local schema = {
                { name = "id", type = "INT32" }
            }
            
            local rows = {
                { id = "not a number" } -- Wrong type
            }
            
            local ok, err = pcall(function()
                parquet.write(schema, rows)
            end)
            
            assert.is_false(ok)
            assert.matches("VALUE_ERROR", err)
            assert.matches("Column value must be an integer", err)
            assert.matches("actual_type: string", err)
        end)
        
        it("should provide detailed diagnostics for range errors", function()
            local schema = {
                { name = "id", type = "INT32" }
            }
            
            local rows = {
                { id = 2147483648 } -- Out of INT32 range
            }
            
            local ok, err = pcall(function()
                parquet.write(schema, rows)
            end)
            
            assert.is_false(ok)
            assert.matches("RANGE_ERROR", err)
            assert.matches("Column value is out of INT32 range", err)
            assert.matches("actual_value: 2147483648", err)
        end)
        
        it("should provide detailed diagnostics for missing values", function()
            local schema = {
                { name = "id", type = "INT32" },
                { name = "name", type = "BYTE_ARRAY" }
            }
            
            local rows = {
                { id = 1 } -- Missing 'name' field
            }
            
            local ok, err = pcall(function()
                parquet.write(schema, rows)
            end)
            
            assert.is_false(ok)
            assert.matches("MISSING_VALUE_ERROR", err)
            assert.matches("Row is missing required column", err)
            assert.matches("column_name: name", err)
        end)
    end)
end) 