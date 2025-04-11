-- encoding_spec.lua
-- Tests for parquet.encoding module

describe("parquet.encoding", function()
    local encoding = require("parquet.encoding")
    local utils = require("parquet.utils")
    
    describe("encode_int32", function()
        it("should encode a list of INT32 values", function()
            local result = encoding.encode_int32({1, 2, 3})
            local expected = utils.write_int32(1) .. utils.write_int32(2) .. utils.write_int32(3)
            assert.equals(expected, result)
        end)
        
        it("should handle empty lists", function()
            local result = encoding.encode_int32({})
            assert.equals("", result)
        end)
        
        it("should handle extreme INT32 values", function()
            local result = encoding.encode_int32({-2147483648, 0, 2147483647})
            local expected = utils.write_int32(-2147483648) .. utils.write_int32(0) .. utils.write_int32(2147483647)
            assert.equals(expected, result)
            assert.equals(12, #result) -- 3 values * 4 bytes per int32
        end)
        
        it("should handle a large number of values", function()
            local values = {}
            for i = 1, 1000 do
                values[i] = i
            end
            local result = encoding.encode_int32(values)
            assert.equals(4000, #result) -- 1000 values * 4 bytes per int32
        end)
        
        -- Error handling test
        it("should raise an error when encoding non-integer values", function()
            assert.has_error(function() encoding.encode_int32({"not an integer"}) end)
        end)
    end)
    
    describe("encode_int64", function()
        it("should encode a list of INT64 values", function()
            local result = encoding.encode_int64({1, 2, 3})
            local expected = utils.write_int64(1) .. utils.write_int64(2) .. utils.write_int64(3)
            assert.equals(expected, result)
            assert.equals(24, #result) -- 3 values * 8 bytes per int64
        end)
        
        it("should handle empty lists", function()
            local result = encoding.encode_int64({})
            assert.equals("", result)
        end)
        
        it("should handle large INT64 values", function()
            local result = encoding.encode_int64({-1000000000, 0, 1000000000})
            local expected = utils.write_int64(-1000000000) .. utils.write_int64(0) .. utils.write_int64(1000000000)
            assert.equals(expected, result)
            assert.equals(24, #result) -- 3 values * 8 bytes per int64
        end)
        
        -- Error handling test
        it("should raise an error when encoding non-integer values", function()
            assert.has_error(function() encoding.encode_int64({"not an integer"}) end)
        end)
    end)
    
    describe("encode_double", function()
        it("should encode a list of DOUBLE values", function()
            local result = encoding.encode_double({1.0, 2.5, -3.75})
            local expected = utils.write_double(1.0) .. utils.write_double(2.5) .. utils.write_double(-3.75)
            assert.equals(expected, result)
            assert.equals(24, #result) -- 3 values * 8 bytes per double
        end)
        
        it("should handle empty lists", function()
            local result = encoding.encode_double({})
            assert.equals("", result)
        end)
        
        it("should handle extreme DOUBLE values", function()
            local result = encoding.encode_double({-1e38, 0, 1e38})
            local expected = utils.write_double(-1e38) .. utils.write_double(0) .. utils.write_double(1e38)
            assert.equals(expected, result)
            assert.equals(24, #result) -- 3 values * 8 bytes per double
        end)
        
        -- Error handling test
        it("should raise an error when encoding non-number values", function()
            assert.has_error(function() encoding.encode_double({"not a number"}) end)
        end)
    end)
    
    describe("encode_boolean", function()
        it("should encode a list of BOOLEAN values", function()
            local result = encoding.encode_boolean({true, false, true})
            local expected = string.char(1) .. string.char(0) .. string.char(1)
            assert.equals(expected, result)
            assert.equals(3, #result) -- 3 values * 1 byte per boolean
        end)
        
        it("should handle empty lists", function()
            local result = encoding.encode_boolean({})
            assert.equals("", result)
        end)
        
        -- Error handling test
        it("should raise an error when encoding non-boolean values", function()
            assert.has_error(function() encoding.encode_boolean({"not a boolean"}) end)
        end)
    end)
    
    describe("encode_byte_array", function()
        it("should encode a list of BYTE_ARRAY values", function()
            local result = encoding.encode_byte_array({"hello", "world"})
            local expected = utils.write_int32(5) .. "hello" .. utils.write_int32(5) .. "world"
            assert.equals(expected, result)
        end)
        
        it("should handle empty lists", function()
            local result = encoding.encode_byte_array({})
            assert.equals("", result)
        end)
        
        it("should handle empty strings", function()
            local result = encoding.encode_byte_array({"", ""})
            local expected = utils.write_int32(0) .. "" .. utils.write_int32(0) .. ""
            assert.equals(expected, result)
        end)
        
        it("should handle binary data with null bytes", function()
            local binary_data = string.char(0, 1, 2, 3, 0)
            local result = encoding.encode_byte_array({binary_data})
            local expected = utils.write_int32(5) .. binary_data
            assert.equals(expected, result)
        end)
        
        it("should handle large strings", function()
            local large_string = string.rep("a", 10000)
            local result = encoding.encode_byte_array({large_string})
            local expected = utils.write_int32(10000) .. large_string
            assert.equals(expected, result)
            assert.equals(10004, #result) -- 4 bytes for length + 10000 bytes for string
        end)
        
        it("should handle multiple large strings", function()
            local str1 = string.rep("a", 1000)
            local str2 = string.rep("b", 2000)
            local str3 = string.rep("c", 3000)
            
            local result = encoding.encode_byte_array({str1, str2, str3})
            local expected = utils.write_int32(1000) .. str1 .. 
                             utils.write_int32(2000) .. str2 .. 
                             utils.write_int32(3000) .. str3
            assert.equals(expected, result)
            assert.equals(6012, #result) -- 3*4 bytes for lengths + 1000+2000+3000 bytes for strings
        end)
        
        -- Error handling test for non-string value
        it("should raise an error when encoding non-string values", function()
            assert.has_error(function() encoding.encode_byte_array({123}) end)
        end)
    end)
    
    describe("extract_column_values", function()
        it("should extract values for a column", function()
            local rows = {
                {id = 1, name = "Alice"},
                {id = 2, name = "Bob"},
                {id = 3, name = "Charlie"}
            }
            
            local result = encoding.extract_column_values(rows, 1, "id")
            assert.same({1, 2, 3}, result)
            
            local result2 = encoding.extract_column_values(rows, 2, "name")
            assert.same({"Alice", "Bob", "Charlie"}, result2)
        end)
        
        it("should handle empty rows", function()
            local result = encoding.extract_column_values({}, 1, "id")
            assert.same({}, result)
        end)
        
        it("should handle missing values", function()
            local rows = {
                {id = 1, name = "Alice"},
                {name = "Bob"},           -- missing id
                {id = 3}                  -- missing name
            }
            
            local result = encoding.extract_column_values(rows, 1, "id")
            assert.same({1, nil, 3}, result)
            
            local result2 = encoding.extract_column_values(rows, 2, "name")
            assert.same({"Alice", "Bob", nil}, result2)
        end)
        
        it("should handle uniformly missing values", function()
            local rows = {
                {other = 1},
                {other = 2},
                {other = 3}
            }
            
            local result = encoding.extract_column_values(rows, 1, "id")
            assert.same({nil, nil, nil}, result)
        end)
        
        it("should handle mixed data types", function()
            local rows = {
                {id = 1, value = "string"},
                {id = 2, value = 42},
                {id = 3, value = true}
            }
            
            local result = encoding.extract_column_values(rows, 2, "value")
            assert.same({"string", 42, true}, result)
        end)
    end)
end) 