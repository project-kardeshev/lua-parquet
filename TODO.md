# lua-parquet TODO List

This document tracks the implementation progress for the lua-parquet library, prioritized from essential functionality to advanced features.

## Tier 1: Must Have (Core Functionality)
- [x] Basic file structure (PAR1 magic number, footer, etc.)
- [x] Thrift encoding for metadata
- [x] Plain encoding for basic types
- [x] Single row group support
- [x] Schema definition
- [x] Basic validation
- [x] INT32 data type
- [x] BYTE_ARRAY (strings) data type
- [x] Basic file metadata
- [x] Simple row-based API

## Tier 2: Important Features
- [x] INT64 data type
- [x] BOOLEAN data type
- [x] DOUBLE data type
- [x] FLOAT data type
- [x] Multiple row groups (for larger files)
- [x] Simple error handling with diagnostics
- [ ] Basic compatibility testing with pyarrow reader
- [ ] Memory-efficient processing

## Tier 3: Enhanced Functionality
- [ ] Column statistics (min, max, null count)
- [ ] Optional fields (nullability)
- [ ] Dictionary encoding (for better compression)
- [ ] SNAPPY compression (widely used)
- [ ] GZIP compression (common alternative)
- [ ] Extended file metadata
- [ ] Round-trip testing
- [ ] Performance benchmarks

## Tier 4: Advanced Features
- [ ] RLE (Run Length Encoding)
- [ ] Delta encoding
- [ ] INT96 (timestamp) support
- [ ] FIXED_LEN_BYTE_ARRAY
- [ ] Decimal types
- [ ] Column-based API
- [ ] Schema inference
- [ ] Chunked data writing

## Tier 5: Specialized Features
- [ ] Nested data structures (lists, maps)
- [ ] Streaming writes
- [ ] Parallel column processing
- [ ] LZ4/ZSTD compression (newer alternatives)
- [ ] Page-level checksums
- [ ] Bloom filters
- [ ] Column projection
- [ ] Custom memory pools
- [ ] Predicate pushdown hints
- [ ] Configuration options (compression level, encoding strategy) 