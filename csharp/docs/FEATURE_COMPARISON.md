# C# TsFile Feature Comparison with Java

This document provides a detailed comparison of the C# TsFile library features against the reference Java implementation.

## Overview

| Component | Java | C# | Status |
|-----------|------|-----|--------|
| Data Types | 12 types | 12 types | ✅ Full parity |
| Encoding Types | 15 types | 15 types | ✅ Full parity |
| Compression Types | 6 types | 6 types | ✅ Full parity |
| V4 Table Model | ✅ Complete | ⚠️ Basic | Partial |
| Query Engine | ✅ Complete | ❌ Missing | Not implemented |
| Encryption | ✅ Complete | ❌ Missing | Not implemented |

## Data Types

| Data Type | Java | C# | Notes |
|-----------|------|-----|-------|
| Boolean | ✅ | ✅ | Full support |
| Int32 | ✅ | ✅ | Full support |
| Int64 | ✅ | ✅ | Full support |
| Float | ✅ | ✅ | Full support |
| Double | ✅ | ✅ | Full support |
| Text | ✅ | ✅ | Full support |
| String | ✅ | ✅ | Full support |
| Blob | ✅ | ✅ | Full support |
| Timestamp | ✅ | ✅ | Full support |
| Date | ✅ | ✅ | Full support |
| Vector | ✅ | ✅ | Basic support |
| Object | ✅ | ✅ | Basic support |

## Encoding Types

| Encoding | Java | C# | Supported Types | Notes |
|----------|------|-----|-----------------|-------|
| Plain | ✅ | ✅ | All | Default encoding |
| Dictionary | ✅ | ✅ | Text/String | For repeated values |
| RLE | ✅ | ✅ | Bool, Int32, Int64 | Run-length encoding |
| Diff | ✅ | ✅ | Int32, Int64 | Differential |
| Ts2Diff | ✅ | ✅ | Int32, Int64, Float, Double | Time series 2nd diff |
| Bitmap | ✅ | ✅ | Boolean | Bit packing |
| GorillaV1 | ✅ | ✅ | Float, Double | Legacy Gorilla |
| Gorilla | ✅ | ✅ | Int32, Int64, Float, Double | Improved Gorilla |
| Regular | ✅ | ✅ | Int64 | Regular intervals |
| ZigZag | ✅ | ✅ | Int32, Int64 | ZigZag + VarInt |
| Chimp | ✅ | ✅ | Float, Double | CHIMP compression |
| Sprintz | ✅ | ✅ | Int32, Int64, Float, Double | Sprintz encoding |
| RLBE | ✅ | ✅ | Int32, Int64 | RLE + Binary |
| Camel | ✅ | ✅ | Float, Double | Camel encoding |
| Freq | ⚠️ Deprecated | ⚠️ Deprecated | - | Not recommended |

## Compression Types

| Compression | Java | C# | Notes |
|-------------|------|-----|-------|
| Uncompressed | ✅ | ✅ | No compression |
| Snappy | ✅ | ✅ | Fast compression |
| Gzip | ✅ | ✅ | High ratio |
| LZ4 | ✅ | ✅ | Fast decompression |
| Zstd | ✅ | ✅ | Balanced performance |
| LZMA2 | ✅ | ✅ | Maximum compression |

## V4 Table Model

| Feature | Java | C# | Notes |
|---------|------|-----|-------|
| TableSchema | ✅ | ✅ | Table definition |
| ColumnSchema | ✅ | ✅ | Column metadata |
| ColumnCategory (TAG/FIELD) | ✅ | ✅ | Column classification |
| TsFileWriterV4 | ✅ | ✅ | Basic writing |
| TsFileReaderV4 | ✅ | ✅ | Basic reading |
| DeviceID | ✅ | ✅ | StringArrayDeviceID |
| MetadataIndexNode | ✅ | ✅ | Index tree |
| TimeseriesMetadata | ✅ | ✅ | Series metadata |
| Query Execution | ✅ | ❌ | Not implemented |
| Filters | ✅ | ❌ | Not implemented |
| Result Sets | ✅ | ❌ | Not implemented |

## Reader/Writer Components

| Component | Java | C# | Status |
|-----------|------|-----|--------|
| TsFileWriter | ✅ | ✅ | Implemented |
| TsFileReader | ✅ | ✅ | Basic implementation |
| TsFileWriterV4 | ✅ | ✅ | Implemented |
| TsFileReaderV4 | ✅ | ✅ | Implemented |
| TsFileWriterBuilder | ✅ | ❌ | Missing |
| TsFileReaderBuilder | ✅ | ❌ | Missing |
| PageReader | ✅ | ❌ | Missing |
| ChunkReader | ✅ | ❌ | Missing |
| SeriesReader | ✅ | ❌ | Missing |
| BatchData/TsBlock | ✅ | ❌ | Missing |

## Test Coverage

### Encoding + Compression Combinations Tested

| Data Type | Tested Encodings | Tested Compressions |
|-----------|------------------|---------------------|
| Int32 | Plain, RLE, Ts2Diff, Gorilla, ZigZag | Uncompressed, Snappy, Gzip, LZ4, Zstd |
| Int64 | Plain, RLE, Ts2Diff, Gorilla, ZigZag | Uncompressed, Snappy, Gzip, LZ4, Zstd |
| Float | Plain, Gorilla, GorillaV1 | Uncompressed, Snappy, Gzip, LZ4, Zstd |
| Double | Plain, Gorilla, GorillaV1 | Uncompressed, Snappy, Gzip, LZ4, Zstd |
| Boolean | Plain, RLE | Uncompressed, Snappy, Gzip |
| String | Plain, Dictionary | Uncompressed, Snappy, Gzip, LZ4, Zstd |

### Additional Test Scenarios

- Mixed data types in single file
- Mixed compression types per column
- Various data patterns (sequential, repeated, alternating, random)
- Different row counts (1, 10, 100, 1000)
- Large string values
- Multiple devices (multi-tag combinations)

## Roadmap for C# Implementation

### Phase 1 (Current) ✅
- Basic V4 read/write support
- All data types
- All encoding types
- All compression types
- Comprehensive interop tests

### Phase 2 (Planned)
- Query execution engine
- Filter expressions
- Result set implementation

### Phase 3 (Planned)
- Page/Chunk readers
- Series readers
- BatchData/TsBlock support

### Phase 4 (Future)
- Encryption support
- Builder pattern APIs
- Performance optimizations
