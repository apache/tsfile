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

# TsFile Format Specification v4

## Overview

TsFile v4 is the latest version of the TsFile columnar storage format for time series data. This version introduces a **table-based data model** as a significant evolution from the tree-based device model used in v3, providing more flexible data organization and improved query performance.

**Version Number:** `0x04` (4 in byte format)

**Magic String:** `TsFile` (6 bytes)

## Key Changes from v3

### 1. Data Model Evolution

| Aspect | v3 (Tree Model) | v4 (Table Model) |
|--------|----------------|------------------|
| **Data Organization** | Device → Measurement hierarchy | Table-based with explicit column types |
| **Schema Definition** | Implicit hierarchy | Explicit `TableSchema` with typed columns |
| **Column Types** | Not categorized | TAG, FIELD, TIMESTAMP columns |
| **Device Identification** | Device path string | Composite of TAG column values |
| **Flexibility** | Fixed hierarchy | Flexible schema per table |

### 2. Metadata Structure Changes

**v3 Format Tail:**
```
[metadata_offset: 8 bytes][MAGIC_STRING: 6 bytes]
```

**v4 Format Tail:**
```
[TsFileMetadata_size: 4 bytes][MAGIC_STRING: 6 bytes]
```

The metadata offset is now stored **inside** the TsFileMetadata structure instead of at the file tail, allowing for more efficient metadata parsing.

### 3. File Structure

Both versions maintain the columnar storage design with these levels:

```
┌─────────────────────────┐
│      Magic String       │  6 bytes: "TsFile"
├─────────────────────────┤
│     Version Number      │  1 byte: 0x04 for v4
├─────────────────────────┤
│                         │
│      Chunk Groups       │  Multiple chunk groups
│    (Device Data)        │  containing time series data
│                         │
├─────────────────────────┤
│                         │
│    Metadata Section     │  Index tree and statistics
│                         │
├─────────────────────────┤
│  TsFileMetadata Size    │  4 bytes (v4 only)
├─────────────────────────┤
│     Magic String        │  6 bytes: "TsFile"
└─────────────────────────┘
```

## Table-Based Data Model

### Schema Definition

A `TableSchema` defines the structure of a table with three types of columns:

#### 1. TAG Columns
- **Purpose:** Unique identification of devices/entities
- **Data Type:** Currently only `STRING`
- **Characteristics:**
  - Can have 0 to multiple TAG columns
  - Composite values form the device identifier
  - Values can be null/empty
  - Used for indexing and filtering
  - All TAG columns must be specified when writing (unspecified filled with null)

**Example:**
```
TAG columns: [Region, Factory, Equipment]
Device ID: ("Beijing", "Factory_A", "Device_001")
```

#### 2. FIELD Columns
- **Purpose:** Measurement values (actual time series data)
- **Data Types:** All TsFile data types supported
  - INT32, INT64
  - FLOAT, DOUBLE
  - BOOLEAN
  - TEXT
  - BLOB
  - DATE
  - TIMESTAMP
  - STRING
- **Characteristics:**
  - Define measurement point names and types
  - Can have multiple FIELD columns per table
  - Support various encoding and compression methods

#### 3. TIMESTAMP Column
- **Purpose:** Time dimension for all measurements
- **Data Type:** INT64 (milliseconds since epoch)
- **Characteristics:**
  - Automatically included in every table
  - Cannot be null
  - Must be in ascending order for same device
  - Built-in indexing

### Table Model Example

**Schema: Industrial Equipment Monitoring**

```
Table: equipment_data
  TAG columns:
    - Region: STRING
    - Factory: STRING  
    - Equipment: STRING
  FIELD columns:
    - Temperature: DOUBLE
    - Humidity: DOUBLE
    - Status: BOOLEAN
    - PowerConsumption: FLOAT
  TIMESTAMP column: (implicit)
```

**Data Example:**

| Timestamp | Region  | Factory    | Equipment  | Temperature | Humidity | Status | PowerConsumption |
|-----------|---------|------------|------------|-------------|----------|--------|------------------|
| 1000      | Beijing | Factory_A  | Device_001 | 25.5        | 60.2     | true   | 120.5            |
| 2000      | Beijing | Factory_A  | Device_001 | 26.1        | 61.0     | true   | 125.3            |
| 3000      | Shanghai| Factory_B  | Device_002 | 24.8        | 58.5     | true   | 115.2            |

Each unique combination of TAG values represents a different device.

## File Format Details

### Magic String and Version

**File Header:**
```
Offset 0-5:  "TsFile" (6 bytes, ASCII)
Offset 6:    Version number (1 byte: 0x04)
```

**File Tail:**
```
Offset N-9:   TsFileMetadata size (4 bytes, int32)
Offset N-5:   "TsFile" (6 bytes, ASCII)
```

### Chunk Structure

Each chunk represents data for one time series (device + measurement):

```
┌──────────────────────────────┐
│      Chunk Header            │
│  - Measurement ID            │
│  - Data size                 │
│  - Data type                 │
│  - Compression type          │
│  - Encoding type             │
│  - Number of pages           │
├──────────────────────────────┤
│         Page 1               │
│  - Page Header               │
│  - Compressed Time Column    │
│  - Compressed Value Column   │
├──────────────────────────────┤
│         Page 2               │
│          ...                 │
├──────────────────────────────┤
│         Page N               │
└──────────────────────────────┘
```

### Chunk Group Structure

Multiple chunks for the same device in the same time period:

```
┌──────────────────────────────┐
│   Chunk Group Header         │
│  - Device ID                 │
│  - Number of chunks          │
├──────────────────────────────┤
│   Chunk 1 (Measurement 1)    │
├──────────────────────────────┤
│   Chunk 2 (Measurement 2)    │
├──────────────────────────────┤
│          ...                 │
├──────────────────────────────┤
│   Chunk N (Measurement N)    │
└──────────────────────────────┘
```

### Metadata Structure (v4)

The v4 metadata structure uses a hierarchical index tree:

```
TsFileMetadata
├── TableSchema Map
│   ├── Table 1 Schema
│   │   ├── TAG columns
│   │   └── FIELD columns
│   └── Table 2 Schema
│       ├── TAG columns
│       └── FIELD columns
├── MetadataIndexNode (Root)
│   ├── Device Index Level
│   │   ├── Device A → MetadataIndexNode
│   │   │   ├── Measurement Index Level
│   │   │   │   ├── Measurement 1 → TimeseriesMetadata
│   │   │   │   └── Measurement 2 → TimeseriesMetadata
│   │   └── Device B → MetadataIndexNode
│   │       └── Measurement Index Level
│   │           └── Measurement 1 → TimeseriesMetadata
├── Bloom Filter (optional)
└── File-level Statistics
```

**MetadataIndexNode Types:**
1. **INTERNAL_DEVICE** - Device-level index nodes
2. **INTERNAL_MEASUREMENT** - Measurement-level index nodes  
3. **LEAF_DEVICE** - Leaf nodes pointing to device metadata
4. **LEAF_MEASUREMENT** - Leaf nodes pointing to time series metadata

### TimeseriesMetadata

Stores metadata for a single time series:

```
TimeseriesMetadata
├── Measurement ID
├── Data Type
├── Statistics
│   ├── Start time
│   ├── End time
│   ├── Count
│   ├── Min value
│   ├── Max value
│   ├── Sum (numeric types)
│   └── First/Last values
├── Chunk Metadata List
│   └── For each chunk:
│       ├── Offset in file
│       ├── Data size
│       ├── Statistics
│       ├── Encoding type
│       └── Compression type
└── Modified indicator
```

## Encoding Methods

TsFile v4 supports the following encoding methods:

| Encoding | Data Types | Description |
|----------|-----------|-------------|
| **PLAIN** | All | No encoding, raw values |
| **RLE** | All | Run-Length Encoding |
| **TS_2DIFF** | INT32, INT64 | Two-level difference encoding |
| **GORILLA** | FLOAT, DOUBLE | Gorilla encoding for floating-point |
| **GORILLA_V1** | FLOAT, DOUBLE | Gorilla v1 variant |
| **DICTIONARY** | TEXT, STRING | Dictionary encoding |
| **ZIGZAG** | INT32, INT64 | ZigZag encoding |
| **CHIMP** | FLOAT, DOUBLE | CHIMP encoding |
| **SPRINTZ** | INT32, INT64, FLOAT, DOUBLE | SPRINTZ encoding |
| **RLBE** | INT32, INT64 | Run-Length Bit-packed Encoding |
| **BITMAP** | BOOLEAN | Bitmap encoding |
| **REGULAR** | INT64 | Regular timestamp encoding |
| **DIFF** | INT32, INT64 | Difference encoding |

**Recommended Encodings:**
- INT32/INT64: `TS_2DIFF`
- FLOAT/DOUBLE: `GORILLA`
- BOOLEAN: `RLE` or `BITMAP`
- TEXT/STRING: `DICTIONARY`
- Regular timestamps: `REGULAR`

## Compression Methods

TsFile v4 supports the following compression algorithms:

| Compression | Description | Best For |
|-------------|-------------|----------|
| **UNCOMPRESSED** | No compression | Already compressed data |
| **SNAPPY** | Fast compression/decompression | General purpose |
| **LZ4** | Very fast, moderate compression | General purpose, real-time |
| **GZIP** | Good compression ratio, slower | Storage optimization |
| **ZSTD** | Best compression ratio | Storage optimization |
| **LZMA2** | Highest compression, slowest | Archival |

**Recommended Compressions:**
- General use: `LZ4` (best balance)
- High throughput: `SNAPPY` or `LZ4`
- Storage optimization: `ZSTD` or `GZIP`

## Reading v4 Files

### Reader Implementation Steps

1. **Read and validate file header**
   ```
   - Read bytes 0-5: Verify "TsFile" magic string
   - Read byte 6: Get version number (0x04)
   ```

2. **Read metadata from file tail**
   ```
   - Seek to position (file_size - 10)
   - Read last 6 bytes: Verify "TsFile" magic string
   - Read bytes at (file_size - 10) to (file_size - 6): Get metadata size (4 bytes)
   ```

3. **Read and deserialize TsFileMetadata**
   ```
   - Seek to position (file_size - 10 - metadata_size)
   - Read metadata_size bytes
   - Deserialize TsFileMetadata structure:
     * TableSchema map
     * MetadataIndexNode tree
     * Bloom filter (optional)
     * File statistics
   ```

4. **Navigate the metadata index tree**
   ```
   - Start from root MetadataIndexNode
   - Traverse device index level
   - Traverse measurement index level
   - Locate TimeseriesMetadata for desired series
   ```

5. **Read chunk data**
   ```
   - Use TimeseriesMetadata to locate chunks
   - For each chunk:
     * Read chunk header
     * Read and decompress pages
     * Decode timestamp and value columns
     * Return data to user
   ```

## Writing v4 Files

### Writer Implementation Steps

1. **Initialize writer with TableSchema**
   ```java
   TableSchema schema = new TableSchema("table_name",
       Arrays.asList(
           new MeasurementSchema("tag1", TSDataType.STRING, TSEncoding.PLAIN),
           new MeasurementSchema("tag2", TSDataType.STRING, TSEncoding.PLAIN)
       ),
       Arrays.asList(
           new MeasurementSchema("field1", TSDataType.DOUBLE, TSEncoding.GORILLA),
           new MeasurementSchema("field2", TSDataType.INT32, TSEncoding.TS_2DIFF)
       )
   );
   ```

2. **Write file header**
   ```
   - Write "TsFile" (6 bytes)
   - Write version 0x04 (1 byte)
   ```

3. **Write data in tablets (batches)**
   ```
   - Group data by device (TAG combination)
   - For each device:
     * Create chunk group
     * For each measurement:
       - Create chunk
       - Write pages with encoded/compressed data
   ```

4. **Build metadata index tree**
   ```
   - Create MetadataIndexNode hierarchy
   - Store device index nodes
   - Store measurement index nodes
   - Store TimeseriesMetadata for each series
   ```

5. **Write file tail**
   ```
   - Serialize TsFileMetadata
   - Write metadata bytes
   - Write metadata size (4 bytes)
   - Write "TsFile" magic string (6 bytes)
   ```

## Compatibility Notes

### Backward Compatibility

- **v4 readers CAN read v3 files** with compatibility layer
- **v3 readers CANNOT read v4 files** due to metadata format changes

### Forward Compatibility  

- v4 introduces breaking changes to metadata structure
- Metadata offset location changed (file tail vs. inside metadata)
- TableSchema is new concept not present in v3
- MetadataIndexNode structure enhanced with table support

### Cross-Implementation Compatibility

| Implementation | Write Version | Read v3 | Read v4 |
|----------------|---------------|---------|---------|
| **Java (current)** | v4 | ✅ Yes | ✅ Yes |
| **C# (current)** | v3 | ✅ Yes | ❌ No |
| **Python (current)** | v3 | ✅ Yes | ❌ No |
| **C++ (current)** | v3 | ✅ Yes | ❌ No |

**Interoperability Requirement:** For Java-C# interoperability, either:
- Upgrade C# to support v4 reading
- Configure Java to write v3 format files

## API Examples

### Java v4 API (Table Model)

```java
// Create table schema
TableSchema schema = new TableSchema("sensor_data");
schema.addTag("region", TSDataType.STRING);
schema.addTag("device", TSDataType.STRING);
schema.addField("temperature", TSDataType.DOUBLE);
schema.addField("humidity", TSDataType.DOUBLE);

// Create writer
try (ITsFileWriter writer = new TsFileWriterBuilder()
    .file(new File("data.tsfile"))
    .tableSchema(schema)
    .build()) {
    
    // Write data
    Tablet tablet = new Tablet(schema);
    tablet.addRow(1000L, "Beijing", "Device_01", 25.5, 60.2);
    tablet.addRow(2000L, "Beijing", "Device_01", 26.1, 61.0);
    
    writer.write(tablet);
}

// Read data
try (TsFileReader reader = new TsFileReader("data.tsfile")) {
    // Query specific device
    QueryExpression query = QueryExpression.create()
        .addFilter("region", "Beijing")
        .addFilter("device", "Device_01")
        .setTimeRange(1000L, 3000L);
    
    QueryDataSet dataSet = reader.query(query);
    while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        // Process record
    }
}
```

### Java v4 API (Tree Model Compatibility)

```java
// For backward compatibility, tree model interface still works
try (TsFileWriter writer = new TsFileWriter(new File("data.tsfile"))) {
    // Register device
    writer.registerTimeseries(
        new Path("root.sg.device1"), 
        new MeasurementSchema("sensor1", TSDataType.DOUBLE, TSEncoding.GORILLA)
    );
    
    // Write records
    TSRecord record = new TSRecord(1000L, "root.sg.device1");
    record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "sensor1", 25.5));
    writer.write(record);
}
```

## Performance Characteristics

### v4 Improvements

1. **Metadata Access**
   - Faster metadata parsing with size-prefixed structure
   - Reduced seeks with integrated offset information
   - Better cache locality for index traversal

2. **Query Performance**
   - Efficient TAG-based filtering
   - Hierarchical index reduces search space
   - Better statistics for query optimization

3. **Schema Flexibility**
   - Dynamic table schemas
   - Explicit column typing
   - Better support for evolving data models

### Best Practices

1. **Schema Design**
   - Use appropriate TAG columns for device identification
   - Choose optimal encodings per data type
   - Consider query patterns when designing schema

2. **Write Optimization**
   - Write data in batches (tablets) for efficiency
   - Group related devices in same chunk groups
   - Use appropriate page sizes (default: 64KB)

3. **Read Optimization**
   - Use filters to reduce data scanning
   - Leverage statistics for query planning
   - Enable caching for frequently accessed metadata

## Migration from v3 to v4

See [MIGRATION_GUIDE_V3_TO_V4.md](./MIGRATION_GUIDE_V3_TO_V4.md) for detailed migration instructions.

## Version History

- **v4 (0x04):** Current version with table-based model
- **v3 (0x03):** Tree-based model, byte version number
- **v2 (000002):** Tree-based model, string version number
- **v1 (000001):** Original format

## References

- [TsFile Format Changelist](../java/tsfile/format-changelist.md)
- [Version Compatibility Matrix](./VERSION_COMPATIBILITY.md)
- [Migration Guide v3 to v4](./MIGRATION_GUIDE_V3_TO_V4.md)
- [Apache TsFile Documentation](https://iotdb.apache.org/)

## Contributors

This specification is maintained by the Apache TsFile community. For questions or contributions, please visit:
- GitHub: https://github.com/apache/tsfile
- Mailing List: dev@iotdb.apache.org
