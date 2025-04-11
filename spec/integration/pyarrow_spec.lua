-- pyarrow_spec.lua
-- Integration tests between Lua-Parquet and PyArrow

describe("PyArrow integration", function()
    local parquet = require("parquet")
    local utils = require("parquet.utils")
    local file_path = "pyarrow_test.parquet"
    local json = require("cjson")
    
    -- Helper function to run PyArrow verification script
    local function verify_with_pyarrow(file_path, expected_data)
        -- First verify the file exists
        local f = io.open(file_path, "rb")
        if not f then
            print("ERROR: File does not exist: " .. file_path)
            return {success = false, error = "File does not exist"}
        end
        f:close()
        
        -- Print file information for debugging
        local file_stats = "ls -la " .. file_path
        os.execute(file_stats)
        
        local expected_json = ""
        if expected_data then
            expected_json = json.encode(expected_data)
            -- Escape quotes for shell command
            expected_json = expected_json:gsub('"', '\\"')
        end
        
        local command
        if expected_data then
            command = string.format('source "$(pwd)/venv/bin/activate" && python "$(pwd)/tools/pyarrow_verify.py" "%s" "%s" && deactivate', 
                file_path, expected_json)
        else
            command = string.format('source "$(pwd)/venv/bin/activate" && python "$(pwd)/tools/pyarrow_verify.py" "%s" && deactivate', 
                file_path)
        end
        
        print("Executing command: " .. command)
        local handle = io.popen(command)
        local result = handle:read("*a")
        handle:close()
        
        print("Python script result: " .. result)
        -- Parse JSON result
        local status, parsed = pcall(function() return json.decode(result) end)
        if not status then
            print("ERROR: Failed to parse JSON result: " .. tostring(parsed))
            return {success = false, error = "Failed to parse result: " .. tostring(parsed)}
        end
        
        return parsed
    end
    
    -- Helper function to write file to disk
    local function write_test_file(schema, rows)
        local content = parquet.write(schema, rows)
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        return content
    end
    
    after_each(function()
        -- Clean up test file
        os.remove(file_path)
    end)
    
    it("should create a Parquet file readable by PyArrow", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local rows = {
            { id = 1, name = "Alice" },
            { id = 2, name = "Bob" },
            { id = 3, name = "Charlie" }
        }
        
        write_test_file(schema, rows)
        
        -- Verify with PyArrow
        local result = verify_with_pyarrow(file_path)
        assert.is_true(result.success, "PyArrow should be able to read the file")
        assert.equals(3, result.row_count, "File should contain 3 rows")
        assert.same({"id", "name"}, result.columns, "File should have the expected columns")
    end)
    
    it("should have the correct data when read with PyArrow", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" },
            { name = "active", type = "BOOLEAN" }
        }
        
        local rows = {
            { id = 1, name = "Alice", active = true },
            { id = 2, name = "Bob", active = false },
            { id = 3, name = "Charlie", active = true }
        }
        
        write_test_file(schema, rows)
        
        -- Verify with PyArrow, including data comparison
        local result = verify_with_pyarrow(file_path, rows)
        assert.is_true(result.success, "PyArrow should be able to read the file")
        assert.is_true(result.data_matches, "Data should match expected values")
    end)
    
    it("should handle all supported data types correctly", function()
        local schema = {
            { name = "int32_col", type = "INT32" },
            { name = "int64_col", type = "INT64" },
            { name = "float_col", type = "FLOAT" },
            { name = "double_col", type = "DOUBLE" },
            { name = "boolean_col", type = "BOOLEAN" },
            { name = "string_col", type = "BYTE_ARRAY" }
        }
        
        local rows = {
            { 
                int32_col = 123,
                int64_col = 9223372036854775807, -- max int64
                float_col = 1.5,
                double_col = 3.14159265359,
                boolean_col = true,
                string_col = "Test String 1"
            },
            { 
                int32_col = -456,
                int64_col = -9223372036854775808, -- min int64 
                float_col = -2.5,
                double_col = -2.71828182845,
                boolean_col = false,
                string_col = "Test String 2"
            }
        }
        
        write_test_file(schema, rows)
        
        -- Verify with PyArrow
        local result = verify_with_pyarrow(file_path)
        assert.is_true(result.success, "PyArrow should be able to read the file")
        assert.equals(2, result.row_count, "File should contain 2 rows")
        assert.same({"int32_col", "int64_col", "float_col", "double_col", "boolean_col", "string_col"}, 
                    result.columns, "File should have the expected columns")
    end)
    
    it("should handle empty dataset correctly", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local rows = {}
        
        write_test_file(schema, rows)
        
        -- Verify with PyArrow
        local result = verify_with_pyarrow(file_path)
        assert.is_true(result.success, "PyArrow should be able to read empty Parquet file")
        assert.equals(0, result.row_count, "File should contain 0 rows")
        assert.same({"id", "name"}, result.columns, "File should have the expected columns")
    end)
    
    it("should create a minimal valid Parquet file directly using low-level functions", function()
        -- This test uses the lowest level functions to create a minimal valid Parquet file
        -- It should help identify any fundamental issues with the Parquet format we're generating
        
        local thrift = require("parquet.thrift")
        local utils = require("parquet.utils")
        
        -- Create a minimal schema
        local schema_elements = {
            -- Root schema element
            thrift.encode_schema_element(nil, 0, 1, 0),
            -- One INT32 column named "value"
            thrift.encode_schema_element("value", 1, 0, 0)
        }
        
        -- Create a minimal data page with one INT32 value (42)
        local page_data = utils.write_int32(42)
        local page_header = thrift.encode_page_header(#page_data, #page_data, 1)
        local page = page_header .. page_data
        
        -- Create a column chunk
        local column_offset = 8  -- After the magic bytes
        local column_metadata = thrift.encode_column_metadata(
            1,                    -- INT32 type
            {"value"},            -- Column path
            1,                    -- Number of values
            column_offset,        -- Offset to the start of the page
            #page                 -- Total size of the page
        )
        
        -- Create row group with one column
        local row_group = thrift.encode_row_group({column_metadata}, 1)
        
        -- Create file metadata
        local file_metadata = thrift.encode_file_metadata(schema_elements, {row_group}, 1)
        
        -- Assemble the file
        local file_content = utils.PARQUET_MAGIC   -- Start magic
                          .. page                 -- Column data
                          .. file_metadata        -- File metadata
                          .. utils.write_int32(#file_metadata)  -- Metadata length
                          .. utils.PARQUET_MAGIC   -- End magic
        
        -- Write file to disk
        local file = io.open(file_path, "wb")
        file:write(file_content)
        file:close()
        
        -- Verify with PyArrow
        local result = verify_with_pyarrow(file_path)
        assert.is_true(result.success, "PyArrow should be able to read the minimal Parquet file")
        assert.equals(1, result.row_count, "File should contain 1 row")
        assert.same({"value"}, result.columns, "File should have the expected column")
    end)
end) 