-- basic_integration_spec.lua
-- Basic integration tests for the Parquet writer

describe("parquet writer basic integration", function()
    local parquet = require("parquet")
    local file_path = "test_output.parquet"
    
    -- Helper function to write file to disk
    local function write_test_file(schema, rows)
        local content = parquet.write(schema, rows)
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        return content
    end
    
    -- Helper function to read file from disk
    local function read_file_bytes(path)
        local file = io.open(path, "rb")
        local content = file:read("*all")
        file:close()
        return content
    end
    
    after_each(function()
        -- Clean up test file
        os.remove(file_path)
    end)
    
    it("should create a valid Parquet file", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local rows = {
            { id = 1, name = "Alice" },
            { id = 2, name = "Bob" },
            { id = 3, name = "Charlie" }
        }
        
        local content = write_test_file(schema, rows)
        
        -- Read back the file to verify
        local read_content = read_file_bytes(file_path)
        
        -- Verify content matches
        assert.equals(content, read_content)
        
        -- Verify file starts and ends with PAR1 magic
        assert.equals("PAR1", read_content:sub(1, 4))
        assert.equals("PAR1", read_content:sub(-4))
    end)
    
    it("should handle empty data sets", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local rows = {}
        
        local content = write_test_file(schema, rows)
        
        -- Read back the file to verify
        local read_content = read_file_bytes(file_path)
        
        -- Verify content matches
        assert.equals(content, read_content)
        
        -- Verify file starts and ends with PAR1 magic
        assert.equals("PAR1", read_content:sub(1, 4))
        assert.equals("PAR1", read_content:sub(-4))
    end)
end) 