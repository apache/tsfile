# TSFile Interoperability Test Results

## Executive Summary

This document describes the Java-C# interoperability test suite created for TSFile and the initial findings from running these tests.

## Test Suite Overview

### Generated Test Files
- **Total Files**: 360 test files
- **File Size**: Ranges from ~280 bytes to ~1.9MB
- **Location**: `/tmp/interop-test-files/`
- **Metadata**: `test-metadata.json` with complete configuration for each file

### Test Matrix

| Component | Count | Values |
|-----------|-------|--------|
| Data Types | 6 | INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT |
| Encodings | 7 | PLAIN, RLE, TS_2DIFF, GORILLA, GORILLA_V1, ZIGZAG, DICTIONARY |
| Compressions | 5 | UNCOMPRESSED, GZIP, LZ4, SNAPPY, ZSTD |
| Patterns | 3 | sequential, repeated, alternating |
| Values per file | 100 | Fixed for consistency |

### Encoding Compatibility Matrix

| Data Type | PLAIN | RLE | TS_2DIFF | GORILLA | GORILLA_V1 | ZIGZAG | DICTIONARY |
|-----------|-------|-----|----------|---------|------------|--------|------------|
| INT32 | ✓ | ✓ | ✓ | ✓ | - | ✓ | - |
| INT64 | ✓ | ✓ | ✓ | ✓ | - | ✓ | - |
| FLOAT | ✓ | ✓ | ✓ | ✓ | ✓ | - | - |
| DOUBLE | ✓ | ✓ | ✓ | ✓ | ✓ | - | - |
| BOOLEAN | ✓ | ✓ | - | - | - | - | - |
| TEXT | ✓ | - | - | - | - | - | ✓ |

## Key Findings

### 1. Version Incompatibility (Critical)

**Issue**: Java TSFile library generates version 4 files, while C# implementation expects version 3.

**Evidence**:
```
✗ All 360 test files: Unsupported TSFile version: 4
```

**Impact**: 
- C# cannot read any Java-generated files without modification
- Indicates format changes between v3 and v4 that need to be documented
- Requires C# implementation to be updated to support v4

**Temporary Fix Applied**: Modified `TsFileReader.cs` to accept version 4:
```csharp
var version = _reader.ReadByte();
if (version != TsFileConstants.Version && version != 4)
    throw new InvalidDataException($"Unsupported TSFile version: {version}");
```

### 2. Metadata Format Changes

**Issue**: After version check fix, metadata reading fails with negative file position.

**Error**:
```
System.ArgumentOutOfRangeException: Non-negative number required. (Parameter 'value')
  at System.IO.FileStream.set_Position(Int64 value)
  at Apache.TsFile.IO.TsFileReader.ReadMetadata()
```

**Impact**:
- Indicates structural changes in v4 file format
- Metadata offset calculation differs between v3 and v4
- C# reader needs comprehensive updates for v4 support

**Root Cause**: The C# implementation was developed based on v3 format specification. Version 4 likely includes:
- Different metadata footer structure
- Changed offset calculation method
- Possibly different magic string at file end
- Modified chunk group format

### 3. Format Specification Gap

**Finding**: No formal specification document exists for TSFile v4 format changes.

**Recommendation**: Create a detailed format specification documenting:
- Version 4 file structure
- Differences from version 3
- Migration guide for implementations
- Binary format for all components (header, chunks, metadata footer)

## Test Implementation

### Java Generator (`java/interop-tests/`)

**Components**:
- `TsFileInteropGenerator.java`: Main generator creating all test files
- `TestFileMetadata.java`: Metadata structure for test files
- `pom.xml`: Maven build configuration

**Key Features**:
- Automatic verification of generated files
- JSON metadata export for cross-language testing
- Pattern-based data generation (sequential, repeated, alternating)
- Compatible encoding selection per data type

**Generation Statistics**:
```
Total test files: 360
- INT32: 75 files (5 encodings × 5 compressions × 3 patterns)
- INT64: 75 files (5 encodings × 5 compressions × 3 patterns)
- FLOAT: 75 files (5 encodings × 5 compressions × 3 patterns)
- DOUBLE: 75 files (5 encodings × 5 compressions × 3 patterns)
- BOOLEAN: 30 files (2 encodings × 5 compressions × 3 patterns)
- TEXT: 30 files (2 encodings × 5 compressions × 3 patterns)
```

### C# Validator (`csharp/tests/Apache.TsFile.InteropTests/`)

**Components**:
- `JavaToCSharpInteropTests.cs`: xUnit test class
- `TestFileMetadata.cs`: C# metadata deserialization
- `Apache.TsFile.InteropTests.csproj`: .NET project file

**Test Methods**:
1. `TestFilesDirectoryExists()`: Verifies test files are available
2. `MetadataFileExists()`: Confirms metadata file is present
3. `ReadAllJavaGeneratedFiles()`: Attempts to read all 360 files
4. `ReadSpecificConfiguration()`: Tests specific data type/encoding combinations

**Current Status**: 
- ✓ Infrastructure working (project builds, tests run)
- ✓ Version check updated
- ✗ Cannot read v4 files (metadata parsing fails)

## Next Steps

### Immediate (High Priority)

1. **Document Version 4 Format**
   - Create specification for v4 file structure
   - Document all changes from v3
   - Include binary format diagrams

2. **Update C# Reader for v4**
   - Implement v4 metadata reading
   - Update chunk reading for any format changes
   - Add version detection and adaptive reading

3. **Complete Interop Tests**
   - Verify all 360 files can be read
   - Validate data integrity
   - Document any encoding-specific issues

### Future (Medium Priority)

4. **Bidirectional Testing**
   - Create C# generator (writes v3 files)
   - Add Java validator for C# files
   - Test both directions

5. **CI Integration**
   - Add interop tests to CI pipeline
   - Automate test file generation
   - Report compatibility status

6. **Extended Testing**
   - Add larger datasets (1000s of values)
   - Test edge cases (NaN, Infinity, nulls)
   - Performance benchmarks for cross-language reading

## Recommendations

### For C# Implementation

1. **Version Support Strategy**:
   - Support both v3 (writing) and v4 (reading) initially
   - Add `TsFileVersion` enum with `V3` and `V4`
   - Implement `IVersionHandler` interface for version-specific logic
   - Phase out v3 writing support once v4 is stable

2. **Reader Architecture**:
   ```csharp
   public interface IVersionHandler
   {
       void ReadMetadata(BinaryReader reader);
       ChunkGroup ReadChunkGroup(BinaryReader reader);
   }
   
   public class TsFileV3Handler : IVersionHandler { }
   public class TsFileV4Handler : IVersionHandler { }
   ```

3. **Testing Strategy**:
   - Unit tests for each version handler
   - Integration tests with Java-generated files
   - Regression tests to ensure v3 still works

### For Documentation

1. Create `TSFILE_FORMAT_V4.md` with:
   - Complete binary structure
   - Field-by-field breakdown
   - Comparison with v3
   - Migration guide

2. Update user documentation:
   - Version compatibility matrix
   - Best practices for cross-language usage
   - Troubleshooting guide

## Conclusion

The interoperability test suite successfully:
- ✓ Generated 360 comprehensive test files
- ✓ Created metadata for validation
- ✓ Identified critical version incompatibility
- ✓ Established testing infrastructure
- ✓ Documented findings

However, achieving full interoperability requires:
- Formal v4 format specification
- C# implementation updates for v4 support
- Additional validation once reading works

This test suite provides a solid foundation for ongoing interoperability validation and will be valuable for:
- Catching regressions
- Validating new encodings
- Ensuring cross-language compatibility
- Quality assurance in releases
