-- parquet/writer.lua
-- Main Parquet writer implementation

local utils = require("parquet.utils")
local thrift = require("parquet.thrift")
local schema = require("parquet.schema")
local encoding = require("parquet.encoding")
local errors = require("parquet.errors")

local writer = {}

-- Validate schema definition
local function validate_schema(schema_def)
    if type(schema_def) ~= "table" then
        errors.raise(errors.SCHEMA_ERROR, "Schema must be a table", {
            schema_type = type(schema_def)
        })
    end
    
    if #schema_def < 1 then
        errors.raise(errors.SCHEMA_ERROR, "Schema must have at least one column", {
            schema_length = #schema_def
        })
    end
    
    for i, field in ipairs(schema_def) do
        if type(field) ~= "table" then
            errors.raise(errors.SCHEMA_ERROR, "Schema field must be a table", {
                field_index = i,
                field_type = type(field)
            })
        end
        
        if type(field.name) ~= "string" then
            errors.raise(errors.SCHEMA_ERROR, "Schema field must have a name property of type string", {
                field_index = i,
                field_name_type = type(field.name)
            })
        end
        
        if type(field.type) ~= "string" then
            errors.raise(errors.SCHEMA_ERROR, "Schema field must have a type property of type string", {
                field_index = i,
                field_name = field.name,
                field_type_type = type(field.type)
            })
        end
        
        -- Check for supported types
        local supported_types = {
            ["INT32"] = true,
            ["INT64"] = true,
            ["DOUBLE"] = true,
            ["BOOLEAN"] = true,
            ["BYTE_ARRAY"] = true,
            ["FLOAT"] = true
        }
        
        if not supported_types[field.type] then
            errors.raise(errors.UNSUPPORTED_FEATURE, "Unsupported type", {
                field_index = i,
                field_name = field.name,
                unsupported_type = field.type,
                supported_types = table.concat(utils.get_table_keys(supported_types), ", ")
            })
        end
    end
end

-- Validate that row data matches schema
local function validate_row_data(schema_def, rows)
    if type(rows) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Rows must be a table", {
            rows_type = type(rows)
        })
    end
    
    -- Get required column names
    local required_columns = {}
    for _, field in ipairs(schema_def) do
        required_columns[field.name] = field.type
    end
    
    -- Validate each row
    for i, row in ipairs(rows) do
        if type(row) ~= "table" then
            errors.raise(errors.TYPE_ERROR, "Row must be a table", {
                row_index = i,
                row_type = type(row)
            })
        end
        
        -- Check for missing columns and validate data types
        for col_name, col_type in pairs(required_columns) do
            local value = row[col_name]
            
            -- Always require values for simplicity
            if value == nil then
                errors.raise(errors.MISSING_VALUE_ERROR, "Row is missing required column", {
                    row_index = i,
                    column_name = col_name,
                    column_type = col_type
                })
            end
            
            -- Validate data type
            if col_type == "INT32" then
                if not utils.is_integer(value) then
                    errors.raise(errors.VALUE_ERROR, "Column value must be an integer", {
                        row_index = i,
                        column_name = col_name,
                        actual_value = value,
                        actual_type = type(value)
                    })
                end
                if value < -2147483648 or value > 2147483647 then
                    errors.raise(errors.RANGE_ERROR, "Column value is out of INT32 range", {
                        row_index = i,
                        column_name = col_name,
                        actual_value = value,
                        min_value = -2147483648,
                        max_value = 2147483647
                    })
                end
            elseif col_type == "INT64" then
                if not utils.is_integer(value) then
                    errors.raise(errors.VALUE_ERROR, "Column value must be an integer", {
                        row_index = i,
                        column_name = col_name,
                        actual_value = value,
                        actual_type = type(value)
                    })
                end
                -- Simplified INT64 range check for Lua
                if value < -9223372036854775808 or value > 9223372036854775807 then
                    errors.raise(errors.RANGE_ERROR, "Column value is out of INT64 range", {
                        row_index = i,
                        column_name = col_name,
                        actual_value = value,
                        min_value = "-9223372036854775808",
                        max_value = "9223372036854775807"
                    })
                end
            elseif col_type == "DOUBLE" or col_type == "FLOAT" then
                if type(value) ~= "number" then
                    errors.raise(errors.TYPE_ERROR, "Column value has wrong type", {
                        row_index = i,
                        column_name = col_name,
                        expected_type = "number",
                        actual_type = type(value)
                    })
                end
            elseif col_type == "BOOLEAN" then
                if type(value) ~= "boolean" then
                    errors.raise(errors.TYPE_ERROR, "Column value has wrong type", {
                        row_index = i,
                        column_name = col_name,
                        expected_type = "boolean",
                        actual_type = type(value)
                    })
                end
            elseif col_type == "BYTE_ARRAY" then
                if type(value) ~= "string" then
                    errors.raise(errors.TYPE_ERROR, "Column value has wrong type", {
                        row_index = i,
                        column_name = col_name,
                        expected_type = "string",
                        actual_type = type(value)
                    })
                end
            end
        end
    end
end

-- Get Parquet type value from type name
local function get_type_value(type_name)
    if type_name == "INT32" then
        return thrift.Type.INT32
    elseif type_name == "INT64" then
        return thrift.Type.INT64
    elseif type_name == "DOUBLE" then
        return thrift.Type.DOUBLE
    elseif type_name == "FLOAT" then
        return thrift.Type.FLOAT
    elseif type_name == "BOOLEAN" then
        return thrift.Type.BOOLEAN
    elseif type_name == "BYTE_ARRAY" then
        return thrift.Type.BYTE_ARRAY
    else
        errors.raise(errors.UNSUPPORTED_FEATURE, "Unsupported type", {
            type_name = type_name
        })
    end
end

-- Encode values based on type
local function encode_values(values, type_name)
    local status, result = errors.try(encoding.encode_values, values, type_name)
    
    if not status then
        errors.raise(errors.ENCODING_ERROR, "Failed to encode values", {
            type_name = type_name,
            error_message = result
        })
    end
    
    return result
end

-- Create row groups from rows based on row_group_size
local function process_row_groups(schema_def, rows, options)
    local row_groups_metadata = {}
    local file_buffer = utils.string_buffer()
    
    -- Start with magic bytes
    file_buffer.add(utils.PARQUET_MAGIC)
    local current_offset = #utils.PARQUET_MAGIC
    local total_rows = #rows
    
    -- Default options
    options = options or {}
    local row_group_size = options.row_group_size or total_rows
    
    -- Split rows into groups
    local num_groups = math.ceil(total_rows / row_group_size)
    
    for group_idx = 1, num_groups do
        -- Calculate start and end index for this group
        local start_idx = (group_idx - 1) * row_group_size + 1
        local end_idx = math.min(group_idx * row_group_size, total_rows)
        
        -- Extract rows for this group
        local group_rows = {}
        for i = start_idx, end_idx do
            group_rows[#group_rows + 1] = rows[i]
        end
        
        -- Column data and metadata for this group
        local column_metadata = {}
        local column_offsets = {}
        
        -- Process each column
        for i, field in ipairs(schema_def) do
            -- Convert type name to type value
            local type_value = get_type_value(field.type)
            
            -- Get column values
            local values = encoding.extract_column_values(group_rows, i, field.name)
            
            -- Encode values
            local encoded_values = encode_values(values, field.type)
            
            -- Create page header
            local page_header = schema.encode_page_header(#encoded_values, #encoded_values, #values)
            
            -- Record offset before writing
            table.insert(column_offsets, current_offset)
            
            -- Update current offset
            current_offset = current_offset + #page_header + #encoded_values
            
            -- Add to file buffer
            file_buffer.add(page_header)
            file_buffer.add(encoded_values)
            
            -- Create column metadata
            local col_meta = schema.encode_column_metadata(
                type_value, 
                {field.name}, 
                #values, 
                column_offsets[i], 
                #encoded_values
            )
            
            table.insert(column_metadata, col_meta)
        end
        
        -- Create row group metadata
        local row_group_buffer = utils.string_buffer()
        row_group_buffer.add(thrift.write_list(1, thrift.TType.STRUCT, column_metadata))
        row_group_buffer.add(thrift.write_field(thrift.TType.I64, 2, current_offset - column_offsets[1])) -- Total byte size
        row_group_buffer.add(thrift.write_field(thrift.TType.I64, 3, #group_rows)) -- Num rows
        row_group_buffer.add(thrift.write_stop())
        
        table.insert(row_groups_metadata, row_group_buffer.get())
    end
    
    return file_buffer.get(), row_groups_metadata, total_rows
end

-- Create a Parquet file from rows and schema with options
function writer.write_file(schema_def, rows, options)
    -- Validate inputs
    validate_schema(schema_def)
    
    -- Only validate row data if there are any rows
    if #rows > 0 then
        validate_row_data(schema_def, rows)
    end
    
    -- Process row groups
    local file, row_groups_metadata, total_rows = process_row_groups(schema_def, rows, options)
    
    -- Create schema elements
    local schema_elements = {}
    
    -- Root schema element
    table.insert(schema_elements, schema.encode_element(nil, 0, #schema_def, 0))
    
    -- Column schema elements
    for _, field in ipairs(schema_def) do
        local type_value = get_type_value(field.type)
        
        table.insert(
            schema_elements, 
            schema.encode_element(field.name, type_value, 0, thrift.FieldRepetitionType.REQUIRED)
        )
    end
    
    -- Create file metadata
    local metadata_buffer = utils.string_buffer()
    metadata_buffer.add(thrift.write_field(thrift.TType.I32, 1, 1)) -- Version
    metadata_buffer.add(thrift.write_list(2, thrift.TType.STRUCT, schema_elements)) -- Schema
    metadata_buffer.add(thrift.write_field(thrift.TType.I64, 3, total_rows)) -- Num rows
    metadata_buffer.add(thrift.write_list(4, thrift.TType.STRUCT, row_groups_metadata)) -- Row groups
    metadata_buffer.add(thrift.write_field(thrift.TType.STRING, 5, "lua-parquet-writer")) -- Created by
    metadata_buffer.add(thrift.write_stop())
    
    local file_metadata = metadata_buffer.get()
    
    -- Create final file buffer
    local final_buffer = utils.string_buffer()
    final_buffer.add(file)
    final_buffer.add(file_metadata)
    final_buffer.add(utils.write_int32(#file_metadata))
    final_buffer.add(utils.PARQUET_MAGIC)
    
    return final_buffer.get()
end

-- Create a Parquet file with manually specified row groups
function writer.write_row_groups(schema_def, row_groups, options)
    -- Validate inputs
    local status, result
    
    status, result = errors.try(validate_schema, schema_def)
    if not status then
        errors.raise(errors.SCHEMA_ERROR, "Schema validation failed", {
            error_message = result
        })
    end
    
    -- Validate row groups
    if type(row_groups) ~= "table" then
        errors.raise(errors.TYPE_ERROR, "Row groups must be a table", {
            row_groups_type = type(row_groups)
        })
    end
    
    if #row_groups < 1 then
        errors.raise(errors.VALUE_ERROR, "At least one row group must be provided", {
            row_groups_count = #row_groups
        })
    end
    
    -- Count total rows
    local total_rows = 0
    for g, group in ipairs(row_groups) do
        if type(group) ~= "table" then
            errors.raise(errors.TYPE_ERROR, "Each row group must be a table of rows", {
                group_index = g,
                group_type = type(group)
            })
        end
        
        -- Validate row data in this group
        if #group > 0 then
            status, result = errors.try(validate_row_data, schema_def, group)
            if not status then
                errors.raise(errors.VALUE_ERROR, "Row validation failed in group", {
                    group_index = g,
                    error_message = result
                })
            end
        end
        
        total_rows = total_rows + #group
    end
    
    -- Options
    options = options or {}
    
    -- Start with magic bytes
    local file = utils.PARQUET_MAGIC
    local current_offset = #file
    local row_groups_metadata = {}
    
    -- Process each row group
    for _, group_rows in ipairs(row_groups) do
        -- Column data and metadata for this group
        local column_metadata = {}
        local column_offsets = {}
        
        -- Process each column
        for i, field in ipairs(schema_def) do
            -- Convert type name to type value
            local type_value = get_type_value(field.type)
            
            -- Get column values
            local values = encoding.extract_column_values(group_rows, i, field.name)
            
            -- Encode values
            local encoded_values = encode_values(values, field.type)
            
            -- Create page header
            local page_header = schema.encode_page_header(#encoded_values, #encoded_values, #values)
            
            -- Record offset before writing
            table.insert(column_offsets, current_offset)
            
            -- Update current offset
            current_offset = current_offset + #page_header + #encoded_values
            
            -- Add to file
            file = file .. page_header .. encoded_values
            
            -- Create column metadata
            local col_meta = schema.encode_column_metadata(
                type_value, 
                {field.name}, 
                #values, 
                column_offsets[i], 
                #encoded_values
            )
            
            table.insert(column_metadata, col_meta)
        end
        
        -- Create row group metadata
        local row_group = ""
        row_group = row_group .. thrift.write_list(1, thrift.TType.STRUCT, column_metadata)
        row_group = row_group .. thrift.write_field(thrift.TType.I64, 2, current_offset - column_offsets[1]) -- Total byte size
        row_group = row_group .. thrift.write_field(thrift.TType.I64, 3, #group_rows) -- Num rows
        row_group = row_group .. thrift.write_stop()
        
        table.insert(row_groups_metadata, row_group)
    end
    
    -- Create schema elements
    local schema_elements = {}
    
    -- Root schema element
    table.insert(schema_elements, schema.encode_element(nil, 0, #schema_def, 0))
    
    -- Column schema elements
    for _, field in ipairs(schema_def) do
        local type_value = get_type_value(field.type)
        
        table.insert(
            schema_elements, 
            schema.encode_element(field.name, type_value, 0, thrift.FieldRepetitionType.REQUIRED)
        )
    end
    
    -- Create file metadata
    local file_metadata = ""
    file_metadata = file_metadata .. thrift.write_field(thrift.TType.I32, 1, 1) -- Version
    file_metadata = file_metadata .. thrift.write_list(2, thrift.TType.STRUCT, schema_elements) -- Schema
    file_metadata = file_metadata .. thrift.write_field(thrift.TType.I64, 3, total_rows) -- Num rows
    file_metadata = file_metadata .. thrift.write_list(4, thrift.TType.STRUCT, row_groups_metadata) -- Row groups
    file_metadata = file_metadata .. thrift.write_field(thrift.TType.STRING, 5, "lua-parquet-writer") -- Created by
    file_metadata = file_metadata .. thrift.write_stop()
    
    -- Add metadata length and closing magic
    file = file .. file_metadata .. utils.write_int32(#file_metadata) .. utils.PARQUET_MAGIC
    
    return file
end

return writer 