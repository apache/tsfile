# TSFile Interoperability Test Generator

This Java application generates TSFile test files for validating interoperability with other TSFile implementations (C#, Python, etc.).

## Purpose

Generate TSFile files with known, predictable data patterns across all supported combinations of:
- Data types
- Encodings
- Compressions
- Data patterns

These files serve as test fixtures for validating that different language implementations can correctly read Java-generated TSFiles.

## Building

```bash
mvn clean install
```

## Running

```bash
mvn exec:java
```

Or directly:

```bash
java -cp target/classes:$(find ~/.m2/repository -name '*.jar' -printf '%p:') \
  org.apache.tsfile.interop.TsFileInteropGenerator
```

## Output

The generator creates:
- **Test files**: `/tmp/interop-test-files/*.tsfile` (360 files)
- **Metadata**: `/tmp/interop-test-files/test-metadata.json`

## Test Configurations

### Data Types (6)
- INT32
- INT64
- FLOAT
- DOUBLE
- BOOLEAN
- TEXT

### Encodings (varies by data type)
- PLAIN (all types)
- RLE (INT32, INT64, FLOAT, DOUBLE, BOOLEAN)
- TS_2DIFF (INT32, INT64, FLOAT, DOUBLE)
- GORILLA (INT32, INT64, FLOAT, DOUBLE)
- GORILLA_V1 (FLOAT, DOUBLE)
- ZIGZAG (INT32, INT64)
- DICTIONARY (TEXT)

### Compressions (5)
- UNCOMPRESSED
- GZIP
- LZ4
- SNAPPY
- ZSTD

### Data Patterns (3)
- **Sequential**: Values from 0 to 99
- **Repeated**: Values repeated in groups of 10
- **Alternating**: Two values alternating

## Metadata Format

The `test-metadata.json` file contains an array of objects:

```json
[
  {
    "fileName": "int32_plain_uncompressed_sequential.tsfile",
    "dataType": "INT32",
    "encoding": "PLAIN",
    "compression": "UNCOMPRESSED",
    "pattern": "sequential",
    "valueCount": 100,
    "expectedValues": [0, 1, 2, ..., 99]
  },
  ...
]
```

## File Verification

Each generated file is automatically verified by reading it back and comparing values to ensure correctness before being included in the test suite.

## Device and Measurement

All test files use:
- **Device**: `root.test.d0`
- **Measurement**: `s0`
- **Timestamp range**: 0 to 99

## Adding New Configurations

To add new test configurations, modify:

1. `getCompatibleEncodings(TSDataType dataType)` - Add encoding support
2. `getTestCompressions()` - Add compression types
3. `generateValue()` methods - Add data patterns

Then rebuild and run to regenerate all test files.

## Troubleshooting

### OutOfMemoryError
Increase Java heap size:
```bash
export MAVEN_OPTS="-Xmx2g"
mvn exec:java
```

### Slow Generation
The generator creates and verifies 360 files. On slower systems this may take several minutes.

### File Permission Issues
Ensure `/tmp` is writable, or modify `OUTPUT_DIR` constant in `TsFileInteropGenerator.java`.
