-- thrift_spec.lua
-- Tests for parquet.thrift module

describe("parquet.thrift", function()
    local thrift = require("parquet.thrift")
    local utils = require("parquet.utils")
    
    describe("write_field", function()
        it("should encode I32 fields correctly", function()
            local result = thrift.write_field(thrift.TType.I32, 1, 42)
            local expected = string.char(thrift.TType.I32) .. string.char(0x01, 0x00) .. string.char(0x2A, 0x00, 0x00, 0x00)
            assert.equals(expected, result)
        end)
        
        it("should encode STRING fields correctly", function()
            local result = thrift.write_field(thrift.TType.STRING, 2, "test")
            local expected = string.char(thrift.TType.STRING) .. string.char(0x02, 0x00) .. string.char(0x04, 0x00, 0x00, 0x00) .. "test"
            assert.equals(expected, result)
        end)
        
        it("should encode BOOL fields correctly", function()
            local result = thrift.write_field(thrift.TType.BOOL, 3, true)
            local expected = string.char(thrift.TType.BOOL) .. string.char(0x03, 0x00) .. string.char(0x01)
            assert.equals(expected, result)
        end)
        
        it("should encode I16 fields correctly", function()
            local result = thrift.write_field(thrift.TType.I16, 4, 12345)
            local expected = string.char(thrift.TType.I16) .. string.char(0x04, 0x00) .. string.char(0x39, 0x30)
            assert.equals(expected, result)
        end)
        
        it("should encode I64 fields correctly", function()
            local result = thrift.write_field(thrift.TType.I64, 5, 1234567890)
            local expected = string.char(thrift.TType.I64) .. string.char(0x05, 0x00) .. 
                            string.char(0xD2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00)
            assert.equals(expected, result)
        end)
        
        it("should encode BINARY fields correctly", function()
            local result = thrift.write_field(thrift.TType.BINARY, 6, "\x01\x02\x03")
            local expected = string.char(thrift.TType.BINARY) .. string.char(0x06, 0x00) .. 
                            string.char(0x03, 0x00, 0x00, 0x00) .. "\x01\x02\x03"
            assert.equals(expected, result)
        end)
    end)
    
    describe("write_list", function()
        it("should encode I32 lists correctly", function()
            local result = thrift.write_list(5, thrift.TType.I32, {10, 20, 30})
            local expected = string.char(thrift.TType.LIST) .. string.char(0x05, 0x00) .. 
                            string.char(thrift.TType.I32) .. string.char(0x03, 0x00, 0x00, 0x00) ..
                            string.char(0x0A, 0x00, 0x00, 0x00) ..
                            string.char(0x14, 0x00, 0x00, 0x00) ..
                            string.char(0x1E, 0x00, 0x00, 0x00)
            assert.equals(expected, result)
        end)
        
        it("should encode STRING lists correctly", function()
            local result = thrift.write_list(6, thrift.TType.STRING, {"ab", "cd"})
            local expected = string.char(thrift.TType.LIST) .. string.char(0x06, 0x00) .. 
                            string.char(thrift.TType.STRING) .. string.char(0x02, 0x00, 0x00, 0x00) ..
                            string.char(0x02, 0x00, 0x00, 0x00) .. "ab" ..
                            string.char(0x02, 0x00, 0x00, 0x00) .. "cd"
            assert.equals(expected, result)
        end)
        
        it("should encode list with custom encoder correctly", function()
            local encoder = function(value)
                return string.char(value)
            end
            local result = thrift.write_list(7, thrift.TType.BYTE, {65, 66, 67}, encoder)
            local expected = string.char(thrift.TType.LIST) .. string.char(0x07, 0x00) .. 
                            string.char(thrift.TType.BYTE) .. string.char(0x03, 0x00, 0x00, 0x00) ..
                            "ABC"
            assert.equals(expected, result)
        end)
    end)
    
    describe("write_map", function()
        it("should encode string-to-int maps correctly", function()
            local map_data = { ["a"] = 1, ["b"] = 2 }
            local result = thrift.write_map(8, thrift.TType.STRING, thrift.TType.I32, map_data)
            
            -- Check the header (map type + field ID + key type + value type + size)
            local header = string.char(thrift.TType.MAP) .. string.char(0x08, 0x00) .. 
                           string.char(thrift.TType.STRING) .. string.char(thrift.TType.I32) .. 
                           string.char(0x02, 0x00, 0x00, 0x00)
            
            assert.equals(#header, 9)
            assert.equals(header, result:sub(1, 9))
            
            -- The rest is the key-value pairs, but order is not guaranteed in maps
            assert.equals(#result, 
                          9 +                                -- header
                          (4 + 1 + 4) +                      -- "a" key (length + content) + 1 value
                          (4 + 1 + 4))                       -- "b" key (length + content) + 1 value
        end)
    end)
    
    describe("write_set", function()
        it("should encode sets as lists correctly", function()
            local result = thrift.write_set(9, thrift.TType.I32, {100, 200, 300})
            local expected = string.char(thrift.TType.LIST) .. string.char(0x09, 0x00) .. 
                            string.char(thrift.TType.I32) .. string.char(0x03, 0x00, 0x00, 0x00) ..
                            string.char(0x64, 0x00, 0x00, 0x00) ..
                            string.char(0xC8, 0x00, 0x00, 0x00) ..
                            string.char(0x2C, 0x01, 0x00, 0x00)
            assert.equals(expected, result)
        end)
    end)
    
    describe("message encoding", function()
        it("should encode message begin correctly", function()
            local result = thrift.write_message_begin("testMethod", 1, 12345)
            local expected = utils.write_int32(thrift.VERSION_1 | 1) .. 
                            utils.write_int32(10) .. "testMethod" .. 
                            utils.write_int32(12345)
            assert.equals(expected, result)
        end)
    end)
    
    describe("encode_struct", function()
        it("should encode simple structs correctly", function()
            local fields = {
                { type_id = thrift.TType.I32, id = 1, value = 42 },
                { type_id = thrift.TType.STRING, id = 2, value = "test" }
            }
            
            local result = thrift.encode_struct(fields)
            
            local expected = thrift.write_field(thrift.TType.I32, 1, 42) ..
                            thrift.write_field(thrift.TType.STRING, 2, "test") ..
                            thrift.write_stop()
            
            assert.equals(expected, result)
        end)
    end)
    
    describe("write_stop", function()
        it("should write a STOP byte", function()
            local result = thrift.write_stop()
            assert.equals(string.char(0), result)
        end)
    end)
end) 