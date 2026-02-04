# Roadmap to Complete V4 Reading and Writing

## Current Status (Phase 2 Complete)

✅ **What Works:**
- V4 file format recognition
- Schema reading from v4 files
- Column introspection (TAG/FIELD/TIMESTAMP categories)

❌ **What Doesn't Work:**
- Query() operations on v4 files (returns empty results)
- Data reading from v4 chunks
- Writing v4 files

## Why Data Reading is Complex

### V3 vs V4 Architecture Differences

**V3 Format (Simple):**
```
File: [Header][Chunks with device names][Metadata][Offset][Footer]
Query: Scan chunks sequentially, match device name
```

**V4 Format (Complex):**
```
File: [Header][Chunks][Metadata Index Tree][Footer]
Query: Navigate metadata tree → Find TimeseriesMetadata → Get chunk positions → Read chunks
```

### Key V4 Challenges

1. **Metadata Index Tree Navigation**
   - Root node → Table nodes → Device nodes → Measurement nodes
   - Each level uses different entry types (IMetadataIndexEntry)
   - Requires recursive tree traversal

2. **Device Identification**
   - V3: Simple string deviceName
   - V4: Composite TAG values (e.g., Region="Beijing", Device="D1")
   - Requires IDeviceID deserialization

3. **Chunk Location**
   - V3: Sequential scan with device name matching
   - V4: TimeseriesMetadata contains chunk positions
   - Must parse TimeseriesMetadata to get chunk offsets

## Phase 3: Data Reading Implementation

### Estimated Effort: 3-4 days

### Step 1: Implement IMetadataIndexEntry (1 day)

**Files to Create:**
- `IMetadataIndexEntry.cs` - Interface
- `DeviceMetadataIndexEntry.cs` - Device-level entries
- `MeasurementMetadataIndexEntry.cs` - Measurement-level entries

**Key Methods:**
```csharp
interface IMetadataIndexEntry
{
    long GetOffset();
    string GetName();
    static IMetadataIndexEntry Deserialize(BinaryReader reader, bool isDeviceLevel);
}
```

### Step 2: Implement IDeviceID (0.5 days)

**Files to Create:**
- `IDeviceID.cs` - Interface
- `StringDeviceID.cs` - String-based device IDs (most common)

**Key Methods:**
```csharp
interface IDeviceID
{
    string ToString();
    bool Matches(Dictionary<string, string> tagValues);
    static IDeviceID Deserialize(BinaryReader reader);
}
```

### Step 3: Update MetadataIndexNode (0.5 days)

**Current:** Simplified deserialization that skips children
**Needed:** Full deserialization with IMetadataIndexEntry children

```csharp
class MetadataIndexNode
{
    List<IMetadataIndexEntry> Children;
    
    MetadataIndexNode GetChildNode(string name);
    IMetadataIndexEntry FindEntry(string name);
}
```

### Step 4: Implement TimeseriesMetadata (1 day)

**File to Create:**
- `TimeseriesMetadata.cs`

**Contains:**
- Measurement ID
- Data type
- Statistics (min/max/count)
- List of ChunkMetadata (positions and sizes)

```csharp
class TimeseriesMetadata
{
    string MeasurementId;
    TsDataType DataType;
    List<ChunkMetadata> ChunkMetadataList;
    
    static TimeseriesMetadata Deserialize(BinaryReader reader);
}
```

### Step 5: Update Query() for V4 (1 day)

**Algorithm:**
```csharp
QueryResult Query(string tableName, Dictionary<string, string>? tagFilters)
{
    if (_fileVersion == v4) {
        // 1. Get table's MetadataIndexNode from metadata
        var tableNode = _tableIndexNodes[tableName];
        
        // 2. Navigate to device nodes
        foreach (var deviceEntry in tableNode.Children)
        {
            if (tagFilters == null || MatchesTags(deviceEntry, tagFilters))
            {
                // 3. Get measurement nodes
                var deviceNode = ReadMetadataIndexNode(deviceEntry.Offset);
                
                foreach (var measurementEntry in deviceNode.Children)
                {
                    // 4. Read TimeseriesMetadata
                    var tsMetadata = ReadTimeseriesMetadata(measurementEntry.Offset);
                    
                    // 5. Read chunks at specified positions
                    foreach (var chunkMeta in tsMetadata.ChunkMetadataList)
                    {
                        ReadChunkAtPosition(result, chunkMeta.Offset, chunkMeta.Size);
                    }
                }
            }
        }
    }
    return result;
}
```

### Step 6: Testing (0.5 days)

- Test with Java v4 example file
- Verify data correctness
- Test different TAG filter combinations
- Performance testing

## Phase 4: Writing Implementation

### Estimated Effort: 4-5 days

### Step 1: Design V4 Writer API (0.5 days)

**New API Design:**
```csharp
// V4 Table-based API
var schema = new TableSchema("sensor_data");
schema.AddTag("region", TsDataType.String);
schema.AddTag("device", TsDataType.String);
schema.AddField("temperature", TsDataType.Double, TsEncoding.Gorilla);

using var writer = new TsFileWriterV4("file.tsfile");
writer.RegisterTable(schema);

// Write with TAG values identifying device
var tablet = new TabletV4(schema);
tablet.AddRow(1000, tags: new[] {"Beijing", "D1"}, values: new object[] {25.5});
writer.Write(tablet);
```

### Step 2: Implement V4 Metadata Writing (2 days)

- Build MetadataIndexNode tree in memory
- Track chunk positions
- Write TimeseriesMetadata
- Write metadata index tree
- Write v4 footer (metadata_size prefix)

### Step 3: Implement V4 Chunk Writing (1.5 days)

- V4 chunk headers
- Device ID serialization
- Page structure
- Encoding/compression (reuse v3)

### Step 4: Java Interop Testing (1 day)

- Write v4 from C#
- Read in Java
- Verify correctness
- Round-trip testing

## Total Estimated Effort

- **Phase 3 (Reading):** 3-4 days
- **Phase 4 (Writing):** 4-5 days
- **Total:** 7-9 days for full implementation

## Recommended Approach

Given the complexity:

### Option 1: Incremental (Recommended)
1. ✅ Phase 1-2: Complete (schema reading)
2. 🔄 Phase 3: Implement reading first (higher priority)
3. 📋 Phase 4: Implement writing later (when needed)

### Option 2: Simplified
- Document limitations clearly
- Provide schema reading (current state)
- Suggest Java for v4 writing
- Implement reading only when use case demands it

### Option 3: Full Implementation
- Commit 7-9 days for complete implementation
- High quality, full Java parity
- Complete test coverage

## Current Recommendation

**Start Phase 3 with a minimal but functional approach:**

1. Implement IMetadataIndexEntry basics (2 days)
2. Add simple Query() support for v4 (1 day)
3. Test with Java v4 files (0.5 days)
4. Document remaining limitations

This provides **functional v4 reading in ~3-4 days** while keeping quality high.

Phase 4 (writing) can be deferred until there's a clear use case.
