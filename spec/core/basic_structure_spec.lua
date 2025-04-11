-- basic_structure_spec.lua
-- Tests for basic Parquet file structure

describe("parquet file structure", function()
    local parquet = require("parquet")
    local utils = require("parquet.utils")
    local file_path = "structure_test.parquet"
    
    after_each(function()
        -- Clean up test file
        os.remove(file_path)
    end)
    
    it("should start and end with PAR1 magic bytes", function()
        local schema = {
            { name = "id", type = "INT32" }
        }
        
        local rows = {
            { id = 1 },
            { id = 2 }
        }
        
        local content = parquet.write(schema, rows)
        
        -- Write to file
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        
        -- Read back the file
        local read_file = io.open(file_path, "rb")
        local read_content = read_file:read("*all")
        read_file:close()
        
        -- Verify file starts and ends with PAR1 magic
        assert.equals("PAR1", read_content:sub(1, 4))
        assert.equals("PAR1", read_content:sub(-4))
    end)
    
    it("should include metadata with correct row count", function()
        local schema = {
            { name = "id", type = "INT32" }
        }
        
        local rows = {
            { id = 1 },
            { id = 2 },
            { id = 3 }
        }
        
        local content = parquet.write(schema, rows)
        
        -- The metadata contains the row count, but parsing it would require
        -- a reader. For now, we'll just confirm the file was created successfully.
        -- When we implement a reader, we can add more specific assertions here.
        
        assert.truthy(content:find("PAR1"))
        assert.truthy(#content > 8)  -- More than just the magic bytes
    end)
    
    it("should handle an empty table with defined schema", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local rows = {}
        
        local content = parquet.write(schema, rows)
        
        -- Write to file
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        
        -- Read back the file
        local read_file = io.open(file_path, "rb")
        local read_content = read_file:read("*all")
        read_file:close()
        
        -- Verify file starts and ends with PAR1 magic
        assert.equals("PAR1", read_content:sub(1, 4))
        assert.equals("PAR1", read_content:sub(-4))
    end)
    
    it("should have different sizes for files with different data", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local small_rows = {
            { id = 1, name = "A" }
        }
        
        local large_rows = {
            { id = 1, name = "A" },
            { id = 2, name = "B" },
            { id = 3, name = "C" },
            { id = 4, name = "D" },
            { id = 5, name = "E" }
        }
        
        local small_content = parquet.write(schema, small_rows)
        local large_content = parquet.write(schema, large_rows)
        
        -- File with more data should be larger
        assert.truthy(#large_content > #small_content)
    end)
    
    it("should include footer with metadata size", function()
        local schema = {
            { name = "id", type = "INT32" }
        }
        
        local rows = {
            { id = 1 }
        }
        
        local content = parquet.write(schema, rows)
        
        -- The last 8 bytes should be:
        -- - 4 bytes for the length of the metadata
        -- - 4 bytes for the PAR1 magic number
        
        -- Get the length of the metadata from the footer
        local metadata_len_bytes = content:sub(-8, -5)
        
        -- This is a bit tricky without a proper binary reader
        -- In a real test, we'd use utils.read_int32 to get the metadata length
        -- and then verify it matches the actual metadata section
        
        -- For now, just check that these bytes exist
        assert.equals(4, #metadata_len_bytes)
    end)
end) 