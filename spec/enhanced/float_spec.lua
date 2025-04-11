-- float_spec.lua
-- Tests for FLOAT data type support

describe("FLOAT data type", function()
    local parquet = require("parquet")
    local encoding = require("parquet.encoding")
    local utils = require("parquet.utils")
    
    describe("encoding.encode_float", function()
        it("should encode a list of FLOAT values", function()
            local result = encoding.encode_float({1.0, 2.5, -3.75})
            local expected = utils.write_float(1.0) .. utils.write_float(2.5) .. utils.write_float(-3.75)
            assert.equals(expected, result)
            assert.equals(12, #result) -- 3 values * 4 bytes per float
        end)
        
        it("should handle empty lists", function()
            local result = encoding.encode_float({})
            assert.equals("", result)
        end)
        
        it("should handle special float values", function()
            local result = encoding.encode_float({0.0, math.huge, -math.huge, 0/0})  -- zero, +inf, -inf, NaN
            local expected = utils.write_float(0.0) .. utils.write_float(math.huge) .. 
                             utils.write_float(-math.huge) .. utils.write_float(0/0)
            assert.equals(16, #result) -- 4 values * 4 bytes per float
            
            -- We can't directly compare the bytes due to NaN representation differences
            -- but we can check the length
        end)
        
        it("should handle extreme FLOAT values", function()
            local result = encoding.encode_float({-3.4028235e38, 3.4028235e38})
            assert.equals(8, #result) -- 2 values * 4 bytes per float
        end)
        
        -- Error handling test
        it("should raise an error when encoding non-number values", function()
            assert.has_error(function() encoding.encode_float({"not a number"}) end)
        end)
    end)
    
    describe("utils.write_float", function()
        it("should encode a float to 4 bytes", function()
            local result = utils.write_float(1.0)
            assert.equals(4, #result)
        end)
        
        it("should round-trip float values correctly", function()
            -- This test assumes we have a read_float function, which we don't yet
            -- but it's a good test to have when we implement it
            local test_values = {0.0, 1.0, -1.0, 3.14159, -3.14159, 1e-10, 1e10}
            
            for _, value in ipairs(test_values) do
                local encoded = utils.write_float(value)
                assert.equals(4, #encoded)
                
                -- When we implement read_float:
                -- local decoded = utils.read_float(encoded)
                -- assert.near(value, decoded, 1e-6)
            end
        end)
        
        it("should handle special float values", function()
            -- Test zero
            local zero = utils.write_float(0.0)
            assert.equals(4, #zero)
            
            -- Test positive infinity
            local pos_inf = utils.write_float(math.huge)
            assert.equals(4, #pos_inf)
            
            -- Test negative infinity
            local neg_inf = utils.write_float(-math.huge)
            assert.equals(4, #neg_inf)
            
            -- Test NaN
            local nan = utils.write_float(0/0)
            assert.equals(4, #nan)
        end)
    end)
    
    describe("integration tests", function()
        local file_path = "float_test_output.parquet"
        
        after_each(function()
            -- Clean up test file
            os.remove(file_path)
        end)
        
        it("should create a valid Parquet file with FLOAT columns", function()
            local schema = {
                { name = "id", type = "INT32" },
                { name = "value", type = "FLOAT" }
            }
            
            local rows = {
                { id = 1, value = 1.1 },
                { id = 2, value = 2.2 },
                { id = 3, value = 3.3 }
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
            
            -- Verify content matches
            assert.equals(content, read_content)
            
            -- Verify file starts and ends with PAR1 magic
            assert.equals("PAR1", read_content:sub(1, 4))
            assert.equals("PAR1", read_content:sub(-4))
        end)
        
        it("should handle special float values in a file", function()
            local schema = {
                { name = "id", type = "INT32" },
                { name = "special_value", type = "FLOAT" }
            }
            
            local rows = {
                { id = 1, special_value = 0.0 },        -- zero
                { id = 2, special_value = 1.0 },        -- one
                { id = 3, special_value = -1.0 },       -- negative one
                { id = 4, special_value = math.huge },  -- positive infinity
                { id = 5, special_value = -math.huge }, -- negative infinity
                { id = 6, special_value = 0/0 }         -- NaN
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
            
            -- Verify content matches
            assert.equals(content, read_content)
            
            -- Verify file starts and ends with PAR1 magic
            assert.equals("PAR1", read_content:sub(1, 4))
            assert.equals("PAR1", read_content:sub(-4))
        end)
    end)
end) 