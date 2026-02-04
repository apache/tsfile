# TsFile v4 Support Status in C#

## Current Status

The C# implementation of Apache TsFile now has **partial v4 support** with infrastructure in place for full implementation.

### What's Implemented ✅

1. **V4 Format Recognition**
   - Correctly identifies v4 files by reading version byte (0x04)
   - Parses v4 footer structure (metadata_size prefix instead of offset suffix)
   - Validates magic strings at header and footer

2. **Foundational Classes**
   - `MetadataIndexNodeType` enum (4 types: INTERNAL_DEVICE, INTERNAL_MEASUREMENT, LEAF_DEVICE, LEAF_MEASUREMENT)
   - `ColumnCategory` enum (TAG, FIELD, TIMESTAMP)
   - `ColumnSchema` class for v4 column definitions
   - `MetadataIndexNode` class for metadata index tree structure
   - `TableSchema.DeserializeV4()` method for v4 table schema deserialization

3. **Core Infrastructure**
   - BigEndian integer reading (Java compatibility)
   - VarInt and VarIntString reading utilities
   - V4 metadata section location and size calculation

### What's Not Yet Implemented ❌

1. **Complex Metadata Deserialization**
   - Full `MetadataIndexNode` tree parsing with nested structures
   - `IMetadataIndexEntry` interface implementations:
     - `DeviceMetadataIndexEntry` - for device-level index entries
     - `MeasurementMetadataIndexEntry` - for measurement-level index entries
   - `IDeviceID` deserialization for device identification
   - Recursive metadata index tree traversal

2. **Data Reading**
   - Chunk reading for v4 format
   - `ChunkMetadata` v4 deserialization
   - `TimeseriesMetadata` v4 deserialization
   - Query operations on v4 files

3. **Optional Features**
   - Bloom filter deserialization
   - Properties map parsing (encryption settings)

## Current Behavior

When attempting to read a v4 file, the C# reader will:
1. Successfully recognize it as a v4 file
2. Throw a `NotSupportedException` with a clear message explaining that v4 support is partial
3. Suggest using v3 format files for C# interoperability

```csharp
try 
{
    using var reader = new TsFileReader("v4file.tsfile");
    // ...
}
catch (NotSupportedException ex)
{
    // "TSFile v4 format reading is partially supported..."
    Console.WriteLine(ex.Message);
}
```

## Roadmap to Full V4 Support

### Phase 1: Complete Metadata Deserialization
**Effort:** Medium-High  
**Dependencies:** Understanding of Java TsFile metadata structures

- Implement `IMetadataIndexEntry` and its implementations
- Implement `IDeviceID` for device identification
- Complete `MetadataIndexNode` deserialization with entry parsing
- Implement `TableSchema` v4 with proper column category handling

### Phase 2: Data Reading
**Effort:** Medium  
**Dependencies:** Phase 1 completion

- Update chunk reading logic for v4 format
- Implement v4 chunk and page metadata deserialization
- Support v4 query operations

### Phase 3: Feature Completeness
**Effort:** Low-Medium  
**Dependencies:** Phase 2 completion

- Bloom filter support
- Encryption properties parsing
- Full compatibility testing with Java v4 files

## Why Partial Support?

The v4 format introduces a significantly more complex metadata structure:

1. **Table-Based Model**: Replaces the simpler device→measurement hierarchy with a table-based model using TAG, FIELD, and TIMESTAMP columns

2. **Hierarchical Metadata Index**: Uses a recursive tree structure (`MetadataIndexNode`) with multiple types of index entries at different levels

3. **Complex Serialization**: The metadata includes polymorphic structures (`IMetadataIndexEntry`) that require context-aware deserialization

4. **Device Identification**: Devices are identified by composite TAG column values rather than simple string paths

Implementing full v4 support requires careful handling of these complex structures, which is beyond the scope of a minimal implementation.

## Interoperability Options

Until full v4 support is implemented, consider these options for Java-C# interoperability:

### Option 1: Use V3 Format (Recommended)
Configure Java to write v3 format files that C# can read:

```java
// Java v4 can write v3 format for compatibility
TsFileConfig config = new TsFileConfig();
config.setTsFileVersion((byte) 3);
```

### Option 2: Contribute V4 Implementation
The foundational classes are in place. Contributors can:
1. Study the Java implementation in `org.apache.tsfile.file.metadata`
2. Port the metadata deserialization logic to C#
3. Add comprehensive tests for v4 reading
4. Submit a pull request

## Testing

The current implementation includes:
- `TsFileV4Tests.cs` - Tests v4 format recognition
- All existing v3 tests pass (103 tests)
- No regression in v3 functionality

## References

- [TsFile V4 Format Specification](../docs/TSFILE_FORMAT_V4.md)
- [Migration Guide V3 to V4](../docs/MIGRATION_GUIDE_V3_TO_V4.md)
- [Version Compatibility Matrix](../docs/VERSION_COMPATIBILITY.md)
- Java implementation: `org.apache.tsfile.file.metadata.*`
