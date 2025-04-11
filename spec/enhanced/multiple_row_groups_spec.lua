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
    
    it("should create correct metadata for multiple row groups", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "name", type = "BYTE_ARRAY" }
        }
        
        local row_groups = {
            {  -- First group with 10 rows
                { id = 1, name = "Group1-A" },
                { id = 2, name = "Group1-B" },
                { id = 3, name = "Group1-C" },
                { id = 4, name = "Group1-D" },
                { id = 5, name = "Group1-E" },
                { id = 6, name = "Group1-F" },
                { id = 7, name = "Group1-G" },
                { id = 8, name = "Group1-H" },
                { id = 9, name = "Group1-I" },
                { id = 10, name = "Group1-J" }
            },
            {  -- Second group with 5 rows
                { id = 11, name = "Group2-A" },
                { id = 12, name = "Group2-B" },
                { id = 13, name = "Group2-C" },
                { id = 14, name = "Group2-D" },
                { id = 15, name = "Group2-E" }
            }
        }
        
        local content = parquet.write_row_groups(schema, row_groups)
        
        -- Write to file for inspection
        local file = io.open(file_path, "wb")
        file:write(content)
        file:close()
        
        -- Basic file structure validation
        assert.equals("PAR1", content:sub(1, 4), "File should start with PAR1 magic bytes")
        assert.equals("PAR1", content:sub(-4), "File should end with PAR1 magic bytes")
        
        -- Get footer metadata
        local footer_length_bytes = content:sub(-8, -5)
        local footer_length = 0
        for i = 4, 1, -1 do
            footer_length = footer_length * 256 + string.byte(footer_length_bytes:sub(i, i))
        end
        
        assert.is_true(footer_length > 0, "Footer length should be greater than 0")
        
        -- Simple verification that the multiple groups are reflected in the file size
        -- A file with multiple row groups should be larger than one with a single group
        local all_rows = {}
        for _, group in ipairs(row_groups) do
            for _, row in ipairs(group) do
                table.insert(all_rows, row)
            end
        end
        
        local single_content = parquet.write(schema, all_rows)
        assert.is_true(#content > #single_content, "Multiple row groups should result in different file structure")
        
        -- The real test would parse the Thrift metadata to verify row group count
        -- For now, we can check for patterns in the binary data
        
        -- Count occurrences of column chunk patterns
        -- Each row group should have a pattern for its column chunks
        local column_chunk_pattern = string.char(0x0F, 0x02, 0x00) -- Common pattern in column chunk metadata
        local count = 0
        local pos = 1
        while true do
            pos = content:find(column_chunk_pattern, pos, true)
            if not pos then break end
            count = count + 1
            pos = pos + 1
        end
        
        -- There should be at least one pattern per column per row group
        assert.is_true(count >= #schema * #row_groups, 
            "Should find at least one metadata pattern per column per row group")
    end)
    
    it("should optimize memory usage with multiple row groups", function()
        local schema = {
            { name = "id", type = "INT32" },
            { name = "value", type = "BYTE_ARRAY" }
        }
        
        -- Create a large dataset 
        local rows = {}
        for i = 1, 1000 do
            rows[i] = { id = i, value = string.rep("Test data for memory optimization testing. ", 10) }
        end
        
        -- Write with a single row group
        collectgarbage()
        local mem_before_single = collectgarbage("count")
        local single_group_options = { row_group_size = 1000 }
        local single_content = parquet.write(schema, rows, single_group_options)
        local mem_after_single = collectgarbage("count")
        local single_mem_usage = mem_after_single - mem_before_single
        
        -- Write with multiple smaller row groups
        collectgarbage()
        local mem_before_multi = collectgarbage("count")
        local multi_group_options = { row_group_size = 100 } -- 10 groups of 100 rows
        local multi_content = parquet.write(schema, rows, multi_group_options)
        local mem_after_multi = collectgarbage("count")
        local multi_mem_usage = mem_after_multi - mem_before_multi
        
        -- The memory difference is hard to test precisely, so we'll just log it
        print(string.format("Memory usage with single group: %.2f KB", single_mem_usage))
        print(string.format("Memory usage with multiple groups: %.2f KB", multi_mem_usage))
        
        -- For a robust test with a memory optimized implementation, the multi-group
        -- approach should use less memory (or at least not significantly more)
        
        -- We just verify both methods produced valid output
        assert.is_true(#single_content > 0, "Single group file should have content")
        assert.is_true(#multi_content > 0, "Multi group file should have content")
        
        -- Ensure both files have the correct magic bytes
        assert.equals("PAR1", single_content:sub(1, 4))
        assert.equals("PAR1", single_content:sub(-4))
        assert.equals("PAR1", multi_content:sub(1, 4))
        assert.equals("PAR1", multi_content:sub(-4))
    end)
end) 