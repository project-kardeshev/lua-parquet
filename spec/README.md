# Test Organization

This directory contains tests for the lua-parquet library, organized to align with our feature requirements.

## Structure

- `core/`: Tests for core functionality (Tier 1 features)
  - `basic_structure_spec.lua`: Tests for file format, magic bytes, footer
  - `thrift_encoding_spec.lua`: Tests for Thrift binary encoding
  - `plain_encoding_spec.lua`: Tests for plain encoding of all supported data types
  - `schema_spec.lua`: Tests for schema handling
  - `validation_spec.lua`: Tests for input validation
  - `basic_types_spec.lua`: Tests for INT32, BYTE_ARRAY, BOOLEAN, etc.

- `enhanced/`: Tests for Tier 2 and 3 features
  - `float_spec.lua`: Tests for FLOAT type support
  - `multiple_row_groups_spec.lua`: Tests for multiple row groups
  - `error_handling_spec.lua`: Tests for error diagnostics
  - `column_statistics_spec.lua`: Tests for min/max/null count
  - `optional_fields_spec.lua`: Tests for NULL values
  - `dictionary_encoding_spec.lua`: Tests for dictionary encoding

- `advanced/`: Tests for Tier 4 and 5 features
  - `compression_spec.lua`: Tests for compression codecs
  - `complex_types_spec.lua`: Tests for nested data structures
  - `performance_spec.lua`: Tests for performance benchmarks

- `compatibility/`: Tests for compatibility with other Parquet implementations
  - `pyarrow_spec.lua`: Tests for compatibility with PyArrow

- `integration/`: End-to-end tests
  - `basic_integration_spec.lua`: Basic integration tests
  - `roundtrip_spec.lua`: Tests that write and read back files

## Running Tests

```bash
busted -p '_spec.lua$'       # Run all tests
busted spec/core             # Run core tests only
busted spec/enhanced         # Run enhanced feature tests
busted spec/advanced         # Run advanced feature tests
busted spec/compatibility    # Run compatibility tests
busted spec/integration      # Run integration tests
``` 