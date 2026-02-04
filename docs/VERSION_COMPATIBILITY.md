<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# TsFile Version Compatibility Matrix

## Overview

This document provides a comprehensive compatibility matrix for different TsFile versions across multiple implementations (Java, C#, Python, C++) to help users understand interoperability constraints and make informed decisions when working with TsFile across different platforms.

## Version Summary

| Version | Version Number | Magic String | Release | Status |
|---------|---------------|--------------|---------|--------|
| v4 | `0x04` (byte) | `TsFile` (6 bytes) | Current | ✅ Active |
| v3 | `0x03` (byte) | `TsFile` (6 bytes) | Legacy | ✅ Supported |
| v2 | `"000002"` (string) | `TsFile` (6 bytes) | Legacy | ⚠️ Deprecated |
| v1 | `"000001"` (string) | `TsFile` (6 bytes) | Legacy | ⚠️ Deprecated |

## Implementation Version Support

### Write Capabilities

Which version each implementation writes by default:

| Implementation | Default Write Version | Configurable |
|----------------|----------------------|--------------|
| **Java** | v4 | ❌ No (v4 only) |
| **C#** | v3 | ❌ No (v3 only) |
| **Python** | v3 | ❌ No (v3 only) |
| **C++** | v3 | ❌ No (v3 only) |

### Read Capabilities

Which versions each implementation can read:

| Implementation | v1 | v2 | v3 | v4 | Notes |
|----------------|----|----|----|----|-------|
| **Java (current)** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | Full backward compatibility |
| **C# (current)** | ⚠️ Limited | ⚠️ Limited | ✅ Yes | ❌ No | v4 support required for Java interop |
| **Python (current)** | ⚠️ Limited | ⚠️ Limited | ✅ Yes | ❌ No | v4 support not implemented |
| **C++ (current)** | ⚠️ Limited | ⚠️ Limited | ✅ Yes | ❌ No | v4 support not implemented |

**Legend:**
- ✅ Full support with all features
- ⚠️ Partial support or deprecated
- ❌ Not supported

## Feature Comparison by Version

### Data Model

| Feature | v1/v2 | v3 | v4 |
|---------|-------|----|----|
| Data Model | Tree (Device → Measurement) | Tree (Device → Measurement) | **Table (TAG + FIELD columns)** |
| Device ID | String path | String path | Composite TAG values |
| Schema Definition | Implicit | Implicit | **Explicit TableSchema** |
| Column Types | Not categorized | Not categorized | **TAG, FIELD, TIMESTAMP** |

### File Structure

| Feature | v1/v2 | v3 | v4 |
|---------|-------|----|----|
| Version Format | String (6 bytes) | **Byte (1 byte)** | Byte (1 byte) |
| Metadata Location | File tail | File tail | **Inside metadata** |
| File Tail | `[offset:8][magic:6]` | `[offset:8][magic:6]` | **`[size:4][magic:6]`** |
| Metadata Index | Basic | Enhanced | **Hierarchical tree** |

### Metadata Structure

| Component | v1/v2 | v3 | v4 |
|-----------|-------|----|----|
| TsFileMetadata | Basic | Standard | **Enhanced with TableSchema** |
| MetadataIndexNode | Simple | Tree structure | **Multi-level with table support** |
| Device Index | Linear | Tree | **Tree with table context** |
| Measurement Index | Linear | Tree | Tree |
| TableSchema | ❌ None | ❌ None | ✅ Present |
| Bloom Filter | Optional | Optional | Optional |

## Implementation Feature Matrix

### Data Types Support

All implementations support these data types (as of latest version):

| Data Type | Java | C# | Python | C++ |
|-----------|------|----|----|-----|
| BOOLEAN | ✅ | ✅ | ✅ | ✅ |
| INT32 | ✅ | ✅ | ✅ | ✅ |
| INT64 | ✅ | ✅ | ✅ | ✅ |
| FLOAT | ✅ | ✅ | ✅ | ✅ |
| DOUBLE | ✅ | ✅ | ✅ | ✅ |
| TEXT/STRING | ✅ | ✅ | ✅ | ✅ |
| BLOB | ✅ | ✅ | ✅ | ✅ |
| DATE | ✅ | ✅ | ✅ | ✅ |
| TIMESTAMP | ✅ | ✅ | ✅ | ✅ |

**Status:** 100% parity across all implementations ✅

### Encoding Support

| Encoding | Java | C# | Python | C++ | Notes |
|----------|------|----|--------|-----|-------|
| PLAIN | ✅ | ✅ | ✅ | ✅ | Universal support |
| RLE | ✅ | ✅ | ✅ | ✅ | Universal support |
| TS_2DIFF | ✅ | ✅ | ✅ | ✅ | Universal support |
| GORILLA | ✅ | ✅ | ✅ | ✅ | Universal support |
| GORILLA_V1 | ✅ | ✅ | ✅ | ✅ | Universal support |
| DICTIONARY | ✅ | ✅ | ✅ | ✅ | Universal support |
| ZIGZAG | ✅ | ✅ | ✅ | ✅ | Universal support |
| BITMAP | ✅ | ✅ | ✅ | ✅ | Universal support |
| REGULAR | ✅ | ✅ | ✅ | ✅ | Universal support |
| DIFF | ✅ | ✅ | ✅ | ✅ | Universal support |
| **CHIMP** | ✅ | ❌ → PLAIN | ⚠️ | ⚠️ | C# falls back to PLAIN |
| **SPRINTZ** | ✅ | ❌ → PLAIN | ⚠️ | ⚠️ | C# falls back to PLAIN |
| **RLBE** | ✅ | ❌ → PLAIN | ⚠️ | ⚠️ | C# falls back to PLAIN |
| FREQ (deprecated) | ⚠️ | ⚠️ → PLAIN | ⚠️ | ⚠️ | Maps to PLAIN |

**Summary:**
- Core encodings (11/14): 100% support across Java/C# ✅
- Advanced encodings (3/14): Java only, others fallback to PLAIN ⚠️

### Compression Support

| Compression | Java | C# | Python | C++ | Notes |
|-------------|------|----|--------|-----|-------|
| UNCOMPRESSED | ✅ | ✅ | ✅ | ✅ | Universal |
| SNAPPY | ✅ | ✅ | ✅ | ✅ | Universal |
| GZIP | ✅ | ✅ | ✅ | ✅ | Universal |
| LZ4 | ✅ | ✅ | ✅ | ✅ | Universal |
| ZSTD | ✅ | ✅ | ✅ | ✅ | Universal |
| **LZMA2** | ✅ | ❌ | ⚠️ | ⚠️ | Not available in .NET, C# fallback |

**Summary:**
- Standard compression (5/6): 100% support ✅
- LZMA2: Java only, not available in .NET Standard ⚠️

## Cross-Implementation Interoperability

### Java ↔ C# Interoperability

#### Current Status (⚠️ Limited)

```
Java (writes v4) → TsFile → C# (reads v3 only) ❌ INCOMPATIBLE
C# (writes v3) → TsFile → Java (reads v3/v4) ✅ COMPATIBLE
```

**Issue:** Java generates v4 files by default, but C# only reads v3.

**Impact:**
- ❌ Java → C# data transfer requires workaround
- ✅ C# → Java data transfer works without issues

#### Workarounds

**Option 1: Upgrade C# to v4 support** (Recommended)
- Implement v4 reader in C#
- Parse table-based metadata
- Handle new metadata structure
- Status: 📋 Documented in this PR

**Option 2: Configure Java to write v3** (Not available)
- Java API does not currently support configuring v3 output
- Would require code modification

**Option 3: Convert files externally**
- Use Java tool to convert v4 → v3
- Not ideal for production workflows

### Java ↔ Python Interoperability

```
Java (writes v4) → TsFile → Python (reads v3 only) ❌ INCOMPATIBLE
Python (writes v3) → TsFile → Java (reads v3/v4) ✅ COMPATIBLE
```

**Status:** Same limitation as Java ↔ C#

### Java ↔ C++ Interoperability

```
Java (writes v4) → TsFile → C++ (reads v3 only) ❌ INCOMPATIBLE
C++ (writes v3) → TsFile → Java (reads v3/v4) ✅ COMPATIBLE
```

**Status:** Same limitation as Java ↔ C#

### C# ↔ Python ↔ C++ Interoperability

```
All write v3, all read v3: ✅ FULLY COMPATIBLE
```

**Status:** Perfect interoperability between non-Java implementations ✅

## Detailed v3 vs v4 Differences

### 1. File Structure Changes

**v3 File Tail:**
```
┌─────────────────────────────────────┐
│   Metadata (TsFileMetadata)         │
├─────────────────────────────────────┤
│   Metadata Offset (8 bytes, long)   │
├─────────────────────────────────────┤
│   Magic String "TsFile" (6 bytes)   │
└─────────────────────────────────────┘
```

**v4 File Tail:**
```
┌─────────────────────────────────────┐
│   Metadata (TsFileMetadata)         │
│   - Contains offset internally      │
├─────────────────────────────────────┤
│   Metadata Size (4 bytes, int32)    │
├─────────────────────────────────────┤
│   Magic String "TsFile" (6 bytes)   │
└─────────────────────────────────────┘
```

**Key Difference:** Metadata offset moved from file tail into metadata structure.

### 2. Metadata Content Changes

**v3 TsFileMetadata:**
```
- MetadataIndexNode (device/measurement tree)
- Bloom filter (optional)
- File statistics
```

**v4 TsFileMetadata:**
```
- TableSchema map (NEW in v4) ✨
- MetadataIndexNode (enhanced device/measurement tree)
- Bloom filter (optional)
- File statistics
```

**Key Addition:** TableSchema provides explicit schema definition for table-based data model.

### 3. API Changes

**v3 API (Tree Model):**
```java
// Write
TsFileWriter writer = new TsFileWriter(file);
writer.registerTimeseries(path, schema);
writer.write(record);

// Read
TsFileSequenceReader reader = new TsFileSequenceReader(file);
QueryExpression query = QueryExpression.create(paths);
```

**v4 API (Table Model):**
```java
// Write
TableSchema schema = new TableSchema("table", tags, fields);
ITsFileWriter writer = new TsFileWriterBuilder()
    .tableSchema(schema)
    .build();
writer.write(tablet);

// Read  
TsFileReader reader = new TsFileReader(file);
// Can use table-based or tree-based API
```

**Key Difference:** v4 introduces explicit table schema while maintaining tree API compatibility.

## Encoding Fallback Behavior

When an implementation encounters an unsupported encoding:

### C# Behavior (for CHIMP, SPRINTZ, RLBE)

```
1. Detect unsupported encoding during read
2. Log warning message
3. Fallback to PLAIN encoding
4. Decompress and return raw values
5. Application continues normally
```

**Impact:**
- ✅ Files remain readable
- ⚠️ May lose some compression efficiency
- ✅ Data integrity maintained

### When Writing

```
1. If encoding not implemented
2. Throw NotSupportedException  
3. User must choose supported encoding
4. No automatic fallback during write
```

## Compression Fallback Behavior

### C# Behavior (for LZMA2)

```
1. LZMA2 not available in .NET Standard
2. If encountered during read: throw exception
3. Recommendation: Avoid LZMA2 for cross-platform files
4. Use ZSTD or GZIP instead for high compression
```

**Workaround:** Re-compress files with supported algorithm before transferring to C#.

## Testing Interoperability

### Test File Generation

**Java v4 Test Generation:**
```bash
cd java/interop-tests
mvn clean compile exec:java
# Generates 360 test files in testdata/
# 6 data types × 7 encodings × 5 compressions × 3 patterns
```

**Test File Naming Convention:**
```
{datatype}_{encoding}_{compression}_{pattern}.tsfile

Examples:
- INT32_TS_2DIFF_LZ4_CONSTANT.tsfile
- DOUBLE_GORILLA_SNAPPY_INCREASING.tsfile
- TEXT_DICTIONARY_GZIP_RANDOM.tsfile
```

### Validation Process

**C# Validation (currently blocked):**
```bash
cd csharp/tests/Apache.TsFile.InteropTests
dotnet test
# Status: ⚠️ Fails due to v4 format incompatibility
```

**Expected after C# v4 support:**
```bash
dotnet test
# Status: ✅ 360/360 files validated successfully
```

## Migration Strategies

### For Existing v3 Systems

**If using Java only:**
- ✅ Upgrade to latest version (v4 support included)
- ✅ Benefit from table model features
- ✅ Backward compatibility with old v3 files

**If using C# only:**
- ✅ Continue using v3 (fully supported)
- ⏳ Upgrade to v4 when C# support is added
- ✅ Files remain compatible with Java readers

**If using Java ↔ C# interoperability:**
- **Option A (Recommended):** Wait for C# v4 support, then upgrade both
- **Option B (Current):** Use C# → Java direction only
- **Option C (Workaround):** Keep Java files in v3 format (requires code modification)

### For New Projects

**Java projects:**
- ✅ Use v4 (default and recommended)
- ✅ Leverage table model for better organization
- ⚠️ Consider interop requirements with other languages

**C# projects:**
- ✅ Use v3 (current version)
- ⚠️ Plan for v4 migration when available
- ✅ Maintain compatibility with Java readers

**Multi-language projects:**
- ⚠️ Use v3 for maximum compatibility (all languages)
- 📋 Plan migration to v4 after all implementations support it
- ✅ Test interoperability thoroughly

## Compatibility Checklist

### Before Choosing TsFile Version

- [ ] Identify all languages/implementations in your system
- [ ] Check version support matrix for each implementation
- [ ] Verify encoding requirements (especially CHIMP, SPRINTZ, RLBE)
- [ ] Verify compression requirements (especially LZMA2)
- [ ] Test with sample files if cross-implementation transfer needed
- [ ] Plan migration strategy for future version upgrades
- [ ] Document version requirements for your project

### For Cross-Language Projects

- [ ] Current Java version supports v4 ✅
- [ ] Current C# version supports v3 only ⚠️
- [ ] Need Java → C#? Wait for C# v4 support 📋
- [ ] Need C# → Java? Works today ✅
- [ ] Alternative implementations (Python/C++)? Use v3 ✅
- [ ] Future-proof? Plan for v4 upgrade across all implementations 📋

## Recommendations

### For Maximum Compatibility (Current)

```
Write: v3 format (use C#, Python, or C++)
Read: Any implementation
Status: ✅ Works everywhere today
```

### For Future-Proof (Planned)

```
Write: v4 format (Java)
Read: Java now, C#/Python/C++ after upgrade
Status: 📋 Requires implementation upgrades
```

### For Production Systems

1. **Single-language systems:** Use latest version of your implementation
2. **Multi-language systems:** Use v3 until all implementations support v4
3. **Java-only systems:** Use v4 for best features and performance
4. **Gradual migration:** Start with C# v4 support, then migrate data

## Version Support Timeline

| Version | Released | End of Support | Recommendation |
|---------|----------|----------------|----------------|
| v4 | Current | Active | ✅ Use for Java-only |
| v3 | Legacy | ✅ Indefinite | ✅ Use for interop |
| v2 | Legacy | ⚠️ Deprecated | ⚠️ Migrate to v3/v4 |
| v1 | Legacy | ⚠️ Deprecated | ⚠️ Migrate to v3/v4 |

## Future Roadmap

### Planned Enhancements

1. **C# v4 Support** 📋 (Documented in this PR)
   - Implement v4 metadata reader
   - Add TableSchema support
   - Enable Java ↔ C# interoperability

2. **Python v4 Support** 📋
   - Follow C# implementation patterns
   - Add table model API
   - Test interoperability

3. **C++ v4 Support** 📋
   - Implement v4 reader
   - Add table model structures
   - Validate with test files

4. **Advanced Encodings** 📋
   - CHIMP, SPRINTZ, RLBE for C#/Python/C++
   - Unified encoding test suite
   - Performance benchmarks

## References

- [TsFile Format v4 Specification](./TSFILE_FORMAT_V4.md)
- [Migration Guide v3 to v4](./MIGRATION_GUIDE_V3_TO_V4.md)
- [Format Changelist](../java/tsfile/format-changelist.md)
- [Implementation Progress](../IMPLEMENTATION_PROGRESS.md)

## Getting Help

### Documentation
- Apache TsFile: https://iotdb.apache.org/
- GitHub Issues: https://github.com/apache/tsfile/issues

### Community
- Mailing List: dev@iotdb.apache.org
- Slack: Apache IoTDB Community

### Reporting Compatibility Issues
When reporting compatibility issues, please include:
- Source implementation and version
- Target implementation and version
- File version (v3 or v4)
- Encodings and compressions used
- Error messages or unexpected behavior
- Sample file (if possible)
