-- utils_spec.lua
-- Tests for parquet.utils module

describe("parquet.utils", function()
    local utils = require("parquet.utils")
    
    describe("write_int16", function()
        it("should encode 16-bit integers in little-endian format", function()
            local result = utils.write_int16(0x1234)
            assert.equals(string.char(0x34, 0x12), result)
        end)
        
        it("should handle zero correctly", function()
            local result = utils.write_int16(0)
            assert.equals(string.char(0x00, 0x00), result)
        end)
        
        it("should handle max 16-bit value correctly", function()
            local result = utils.write_int16(0xFFFF)
            assert.equals(string.char(0xFF, 0xFF), result)
        end)
    end)
    
    describe("write_int32", function()
        it("should encode 32-bit integers in little-endian format", function()
            local result = utils.write_int32(0x12345678)
            assert.equals(string.char(0x78, 0x56, 0x34, 0x12), result)
        end)
        
        it("should handle zero correctly", function()
            local result = utils.write_int32(0)
            assert.equals(string.char(0x00, 0x00, 0x00, 0x00), result)
        end)
        
        it("should handle max 32-bit value correctly", function()
            local result = utils.write_int32(0xFFFFFFFF)
            assert.equals(string.char(0xFF, 0xFF, 0xFF, 0xFF), result)
        end)
    end)
    
    describe("write_int64", function()
        it("should encode 64-bit integers in little-endian format", function()
            local result = utils.write_int64(0x1234567890ABCDEF)
            assert.equals(string.char(0xEF, 0xCD, 0xAB, 0x90, 0x78, 0x56, 0x34, 0x12), result)
        end)
        
        it("should handle zero correctly", function()
            local result = utils.write_int64(0)
            assert.equals(string.char(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00), result)
        end)
    end)
end) 