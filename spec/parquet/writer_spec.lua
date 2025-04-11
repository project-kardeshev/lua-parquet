-- writer_spec.lua
-- Tests for parquet.writer module

describe("parquet.writer", function()
    local writer = require("parquet.writer")
    local utils = require("parquet.utils")
    local thrift = require("parquet.thrift")
    
    describe("write_file", function()
        it("should create a valid parquet file", function()
            local schema = {
                { name = "id", type = "INT32" },
                { name = "name", type = "BYTE_ARRAY" }
            }
            
            local rows = {
                { id = 1, name = "Alice" },
                { id = 2, name = "Bob" }
            }
            
            local result = writer.write_file(schema, rows)
            
            -- Check for magic bytes at beginning and end
            assert.equals(utils.PARQUET_MAGIC, result:sub(1, 4))
            assert.equals(utils.PARQUET_MAGIC, result:sub(-4))
            
            -- Check that result is not empty
            assert.is_true(#result > 8) -- At least magic bytes at beginning and end
        end)
        
        it("should handle empty rows", function()
            local schema = {
                { name = "id", type = "INT32" },
                { name = "name", type = "BYTE_ARRAY" }
            }
            
            local rows = {}
            
            local result = writer.write_file(schema, rows)
            
            -- Check for magic bytes at beginning and end
            assert.equals(utils.PARQUET_MAGIC, result:sub(1, 4))
            assert.equals(utils.PARQUET_MAGIC, result:sub(-4))
            
            -- Check that result is not empty
            assert.is_true(#result > 8) -- At least magic bytes at beginning and end
        end)
        
        it("should support all implemented data types", function()
            local schema = {
                { name = "int32_col", type = "INT32" },
                { name = "int64_col", type = "INT64" },
                { name = "double_col", type = "DOUBLE" },
                { name = "boolean_col", type = "BOOLEAN" },
                { name = "string_col", type = "BYTE_ARRAY" }
            }
            
            local rows = {
                {
                    int32_col = 42,
                    int64_col = 9223372036854775807, -- Max INT64
                    double_col = 3.14159,
                    boolean_col = true,
                    string_col = "Hello, World!"
                },
                {
                    int32_col = -42,
                    int64_col = -9223372036854775808, -- Min INT64
                    double_col = -2.71828,
                    boolean_col = false,
                    string_col = "Goodbye, World!"
                }
            }
            
            local result = writer.write_file(schema, rows)
            
            -- Check for magic bytes at beginning and end
            assert.equals(utils.PARQUET_MAGIC, result:sub(1, 4))
            assert.equals(utils.PARQUET_MAGIC, result:sub(-4))
            
            -- Check that result is not empty
            assert.is_true(#result > 8) -- At least magic bytes at beginning and end
        end)
        
        -- Error handling tests
        
        it("should raise an error with empty schema", function()
            local schema = {}
            
            local ok, err = pcall(function() writer.write_file(schema, {}) end)
            assert.is_false(ok)
            assert.matches("Parquet Error %[SCHEMA_ERROR%]", err)
            assert.matches("Schema must have at least one column", err)
        end)
        
        it("should raise an error with unsupported type", function()
            local schema = {
                { name = "id", type = "FIXED_LEN_BYTE_ARRAY" } -- Unsupported type
            }
            
            local rows = {
                { id = "fixed length bytes" }
            }
            
            local ok, err = pcall(function() writer.write_file(schema, rows) end)
            assert.is_false(ok)
            assert.matches("Parquet Error %[UNSUPPORTED_FEATURE%]", err)
            assert.matches("Unsupported type", err)
        end)
        
        it("should validate schema structure", function()
            local schema = {
                { name = 123, type = "INT32" } -- Invalid name type
            }
            
            local rows = {
                { ["123"] = 1 }
            }
            
            assert.has_error(function() writer.write_file(schema, rows) end)
        end)
        
        it("should raise an error with invalid data for column type", function()
            local schema = {
                { name = "id", type = "INT32" }
            }
            
            local rows = {
                { id = "not an integer" } -- String in INT32 column
            }
            
            assert.has_error(function() writer.write_file(schema, rows) end)
        end)
        
        it("should validate boolean column data", function()
            local schema = {
                { name = "flag", type = "BOOLEAN" }
            }
            
            local rows = {
                { flag = "true" } -- String instead of boolean
            }
            
            assert.has_error(function() writer.write_file(schema, rows) end)
        end)
        
        it("should validate double column data", function()
            local schema = {
                { name = "value", type = "DOUBLE" }
            }
            
            local rows = {
                { value = "3.14" } -- String instead of number
            }
            
            assert.has_error(function() writer.write_file(schema, rows) end)
        end)
        
        it("should handle missing columns in rows", function()
            local schema = {
                { name = "id", type = "INT32" },
                { name = "name", type = "BYTE_ARRAY" }
            }
            
            local rows = {
                { id = 1 }, -- missing name
                { name = "Bob" } -- missing id
            }
            
            assert.has_error(function() writer.write_file(schema, rows) end)
        end)
    end)
end) 