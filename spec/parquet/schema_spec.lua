-- schema_spec.lua
-- Tests for parquet.schema module

describe("parquet.schema", function()
    local schema = require("parquet.schema")
    local thrift = require("parquet.thrift")
    local utils = require("parquet.utils")
    
    -- Debug helper function to print binary data
    local function hex_dump(bytes)
        local result = {}
        for i = 1, #bytes do
            result[i] = string.format("%02X", bytes:byte(i))
        end
        return table.concat(result, " ")
    end
    
    describe("encode_element", function()
        it("should encode schema elements correctly", function()
            local result = schema.encode_element("test_column", thrift.Type.INT32, 0, thrift.FieldRepetitionType.REQUIRED)
            
            print("\nSchema element result:", hex_dump(result))
            
            -- Create manual struct for comparison
            local expected = thrift.write_field(thrift.TType.I32, 1, thrift.Type.INT32) ..
                             thrift.write_field(thrift.TType.I32, 3, thrift.FieldRepetitionType.REQUIRED) ..
                             thrift.write_field(thrift.TType.STRING, 4, "test_column") ..
                             thrift.write_field(thrift.TType.I32, 5, 0) ..
                             thrift.write_stop()
            
            print("Expected:", hex_dump(expected))
            
            assert.equals(#expected, #result)
            
            -- Test type and values, but don't test exact byte positions
            -- Check if it contains the INT32 type field with correct value
            local contains_type_field = false
            for i = 1, #result - 7 do
                if result:byte(i) == thrift.TType.I32 and 
                   result:byte(i+1) == 0x01 and result:byte(i+2) == 0x00 and
                   result:byte(i+3) == 0x01 and result:byte(i+4) == 0x00 and
                   result:byte(i+5) == 0x00 and result:byte(i+6) == 0x00 then
                    contains_type_field = true
                    break
                end
            end
            assert.is_true(contains_type_field, "Should contain INT32 type field with correct ID and value")
        end)
    end)
    
    describe("encode_page_header", function()
        it("should encode page headers correctly", function()
            local result = schema.encode_page_header(100, 100, 5)
            
            print("\nPage header result:", hex_dump(result))
            
            -- Check that result has reasonable length
            assert.is_true(#result > 0)
            
            -- Look for page type field with DATA_PAGE value (0)
            local contains_page_type = false
            for i = 1, #result - 7 do
                if result:byte(i) == thrift.TType.I32 and 
                   result:byte(i+1) == 0x01 and result:byte(i+2) == 0x00 and
                   result:byte(i+3) == 0x00 and result:byte(i+4) == 0x00 and
                   result:byte(i+5) == 0x00 and result:byte(i+6) == 0x00 then
                    contains_page_type = true
                    break
                end
            end
            assert.is_true(contains_page_type, "Should contain page type field with DATA_PAGE value")
        end)
    end)
    
    describe("encode_column_metadata", function()
        it("should encode column metadata correctly", function()
            local result = schema.encode_column_metadata(
                thrift.Type.INT32,           -- type
                {"test_column"},             -- path in schema
                10,                          -- num values
                1000,                        -- data page offset
                200                          -- page size
            )
            
            print("\nColumn metadata result:", hex_dump(result))
            
            -- Check that result has reasonable length
            assert.is_true(#result > 0)
            
            -- Look for type field with INT32 value
            local contains_type_field = false
            for i = 1, #result - 7 do
                if result:byte(i) == thrift.TType.I32 and 
                   result:byte(i+1) == 0x01 and result:byte(i+2) == 0x00 and
                   result:byte(i+3) == 0x01 and result:byte(i+4) == 0x00 and
                   result:byte(i+5) == 0x00 and result:byte(i+6) == 0x00 then
                    contains_type_field = true
                    break
                end
            end
            assert.is_true(contains_type_field, "Should contain type field with INT32 value")
            
            -- Check for path in schema field
            local contains_path = false
            for i = 1, #result - 10 do
                if result:byte(i) == thrift.TType.LIST and 
                   result:byte(i+1) == 0x03 and result:byte(i+2) == 0x00 then
                    contains_path = true
                    break
                end
            end
            assert.is_true(contains_path, "Should contain path in schema list")
        end)
    end)
    
    describe("encode_file_metadata", function()
        it("should encode file metadata correctly", function()
            -- Create a minimal schema
            local schema_element = schema.encode_element("test_column", thrift.Type.INT32, 0, thrift.FieldRepetitionType.REQUIRED)
            local root_element = schema.encode_element(nil, 0, 1, thrift.FieldRepetitionType.REQUIRED)
            
            -- Create column metadata
            local col_meta = schema.encode_column_metadata(
                thrift.Type.INT32,
                {"test_column"},
                10,
                1000,
                200
            )
            
            -- Create column chunk
            local col_chunk = schema.encode_column_chunk(1000, col_meta)
            
            -- Create row group
            local row_group = schema.encode_row_group(200, 10, {col_chunk})
            
            -- Now create the file metadata
            local result = schema.encode_file_metadata(
                {root_element, schema_element},  -- schema elements
                1,                               -- schema version
                10,                              -- num rows
                {row_group}                      -- row groups
            )
            
            print("\nFile metadata result:", hex_dump(result))
            
            -- Check that result has reasonable length
            assert.is_true(#result > 0)
            
            -- Look for version field with value 1
            local contains_version = false
            for i = 1, #result - 7 do
                if result:byte(i) == thrift.TType.I32 and 
                   result:byte(i+1) == 0x01 and result:byte(i+2) == 0x00 and
                   result:byte(i+3) == 0x01 and result:byte(i+4) == 0x00 and
                   result:byte(i+5) == 0x00 and result:byte(i+6) == 0x00 then
                    contains_version = true
                    break
                end
            end
            assert.is_true(contains_version, "Should contain version field with value 1")
            
            -- Check for creator field
            local contains_creator = false
            for i = 1, #result - 10 do
                if result:byte(i) == thrift.TType.STRING and 
                   result:byte(i+1) == 0x06 and result:byte(i+2) == 0x00 then
                    contains_creator = true
                    break
                end
            end
            assert.is_true(contains_creator, "Should contain creator field")
        end)
    end)
end) 