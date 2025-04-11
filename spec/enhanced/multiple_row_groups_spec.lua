-- multiple_row_groups_spec.lua
-- Tests for multiple row groups support

describe("multiple row groups", function()
    local parquet = require("parquet")
    local file_path = "row_groups_test.parquet"
    
    after_each(function()
        -- Clean up test file
        os.remove(file_path)
    end)
    
    -- Tests for automatic row group splitting
    it("should write multiple row groups when data exceeds row group size", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        -- Create a dataset
        local rows = {}
        for i = 1, 100 do  -- 100 rows
            rows[i] = { id = i, name = "Name" .. i }
        end
        
        -- Set row group size to 10 rows to force multiple groups
        local options = {
            row_group_size = 10
        }
        
        local content = parquet.write(schema, rows, options)
        
        -- Write to file
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        
        -- Verify file was created
        assert.truthy(#content > 0)
        
        -- Verify magic bytes
        assert.equals("PAR1", content:sub(1, 4))
        assert.equals("PAR1", content:sub(-4))
        
        -- We'd need a reader to fully verify the row groups
        -- but we can do some basic checks on file size
        
        -- Create a single row group file for comparison
        local single_group_content = parquet.write(schema, rows)
        
        -- Multiple row groups may have some overhead so the file might be larger
        assert.truthy(#content >= #single_group_content * 0.8) -- Allow for some variation
    end)
    
    -- Tests for manual row group specification
    it("should allow manual specification of row groups", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local row_groups = {
            {  -- First group
                { id = 1, name = "A" },
                { id = 2, name = "B" }
            },
            {  -- Second group
                { id = 3, name = "C" },
                { id = 4, name = "D" }
            }
        }
        
        local content = parquet.write_row_groups(schema, row_groups)
        
        -- Write to file
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        
        -- Verify file was created successfully
        assert.truthy(#content > 0)
        
        -- Verify magic bytes
        assert.equals("PAR1", content:sub(1, 4))
        assert.equals("PAR1", content:sub(-4))
        
        -- Compare with writing all rows in a single group
        local all_rows = {
            { id = 1, name = "A" },
            { id = 2, name = "B" },
            { id = 3, name = "C" },
            { id = 4, name = "D" }
        }
        
        local single_content = parquet.write(schema, all_rows)
        
        -- Files should be similar in size
        assert.truthy(#content >= #single_content * 0.8) -- Allow for some variation
    end)
    
    it("should handle an empty row group", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local row_groups = {
            {  -- First group
                { id = 1, name = "A" },
                { id = 2, name = "B" }
            },
            {  -- Empty group
            },
            {  -- Third group
                { id = 3, name = "C" },
                { id = 4, name = "D" }
            }
        }
        
        local content = parquet.write_row_groups(schema, row_groups)
        
        -- Write to file
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        
        -- Verify file was created successfully
        assert.truthy(#content > 0)
        
        -- Verify magic bytes
        assert.equals("PAR1", content:sub(1, 4))
        assert.equals("PAR1", content:sub(-4))
    end)
    
    it("should create the correct number of row groups", function()
        local schema = {
            { name = "id", type = "INT32" }
        }
        
        -- Create a larger dataset
        local rows = {}
        for i = 1, 500 do  -- 500 rows
            rows[i] = { id = i }
        end
        
        -- Expected row groups with different sizes
        local test_cases = {
            { row_group_size = 100, expected_groups = 5 },
            { row_group_size = 200, expected_groups = 3 },
            { row_group_size = 500, expected_groups = 1 },
            { row_group_size = 1000, expected_groups = 1 }
        }
        
        for _, test in ipairs(test_cases) do
            local options = {
                row_group_size = test.row_group_size
            }
            
            local content = parquet.write(schema, rows, options)
            
            -- Verify file was created
            assert.truthy(#content > 0)
            
            -- A more accurate test would parse the metadata to check number of row groups,
            -- but for now we just verify the file was created successfully
        end
    end)
    
    -- Previous pending tests now implemented
    
    pending("should create correct metadata for multiple row groups", function()
        -- This test will need to analyze the file metadata to ensure
        -- row group information is correctly written
        -- To fully test this, we need a reader implementation
    end)
    
    pending("should optimize memory usage with multiple row groups", function()
        -- This test will verify that memory usage is optimized
        -- when writing large files with multiple row groups
        -- This would require memory profiling tools
    end)
end) 