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

# Migration Guide: TsFile v3 to v4

## Overview

This guide helps you migrate from TsFile v3 to v4, covering the key changes, migration strategies, code examples, and troubleshooting steps. The migration primarily affects **Java implementations** since other implementations (C#, Python, C++) currently use v3.

**Target Audience:**
- Java developers upgrading to v4 APIs
- System architects planning cross-language interoperability
- C# developers preparing for v4 support (future)

## Executive Summary

### What's Changing

| Aspect | v3 | v4 |
|--------|----|----|
| **Data Model** | Tree (Device → Measurement) | **Table (TAG + FIELD)** |
| **API** | TsFileWriter/Reader | **ITsFileWriter + Builder pattern** |
| **Schema** | Implicit registration | **Explicit TableSchema** |
| **Metadata** | Device-based indexing | **Table-based indexing** |
| **Compatibility** | Forward compatible | **Backward compatible** |

### Who Should Migrate

✅ **Migrate to v4 if:**
- Starting new Java projects
- Need table-based data organization
- Want improved query performance
- Can use Java-only ecosystem

⚠️ **Stay on v3 if:**
- Need C#/Python/C++ interoperability (until they support v4)
- Have stable v3 systems
- Cannot test migration thoroughly

## Migration Checklist

### Pre-Migration

- [ ] **Audit current usage**
  - List all TsFile write operations
  - List all TsFile read operations
  - Document current device/measurement structure
  - Identify all dependencies

- [ ] **Assess compatibility**
  - Check if consumers support v4 (Java only currently)
  - Review encoding/compression requirements
  - Plan for backward compatibility if needed

- [ ] **Backup data**
  - Backup all existing v3 TsFile files
  - Test backup restoration
  - Document backup locations

- [ ] **Set up testing**
  - Create test environment
  - Prepare test data sets
  - Define success criteria

### Migration Process

- [ ] **Update dependencies**
  - Upgrade to TsFile v4 library
  - Update Maven/Gradle dependencies
  - Resolve dependency conflicts

- [ ] **Convert code**
  - Update writer code to v4 API
  - Update reader code to v4 API
  - Define TableSchema for datasets

- [ ] **Test thoroughly**
  - Unit tests for new code
  - Integration tests with real data
  - Performance comparison v3 vs v4

- [ ] **Deploy gradually**
  - Deploy to development environment
  - Deploy to staging environment
  - Monitor for issues
  - Deploy to production

### Post-Migration

- [ ] **Verify data integrity**
  - Compare v3 and v4 file contents
  - Validate all data points
  - Check statistics and metadata

- [ ] **Monitor performance**
  - Measure write throughput
  - Measure read throughput
  - Compare with v3 baseline

- [ ] **Document changes**
  - Update internal documentation
  - Train team on new APIs
  - Update deployment procedures

## Code Migration Examples

### Example 1: Basic Write Operation

#### v3 Code (Tree Model)

```java
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;

import java.io.File;

public class V3Writer {
    public void writeData() throws Exception {
        File file = new File("data_v3.tsfile");
        
        try (TsFileWriter writer = new TsFileWriter(file)) {
            // Register measurements for each device
            writer.registerTimeseries(
                new Path("root.sensor.device1"),
                new MeasurementSchema("temperature", TSDataType.DOUBLE, TSEncoding.GORILLA)
            );
            writer.registerTimeseries(
                new Path("root.sensor.device1"),
                new MeasurementSchema("humidity", TSDataType.DOUBLE, TSEncoding.GORILLA)
            );
            
            // Write data points individually
            TSRecord record = new TSRecord(1000L, "root.sensor.device1");
            record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "temperature", 25.5));
            record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "humidity", 60.2));
            writer.write(record);
            
            record = new TSRecord(2000L, "root.sensor.device1");
            record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "temperature", 26.1));
            record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "humidity", 61.0));
            writer.write(record);
        }
    }
}
```

#### v4 Code (Table Model) - Recommended

```java
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;

import java.io.File;
import java.util.Arrays;

public class V4Writer {
    public void writeData() throws Exception {
        // Define table schema with explicit TAG and FIELD columns
        TableSchema schema = new TableSchema("sensor_data");
        
        // TAG columns for device identification (optional, can be empty list)
        schema.addTag("device", TSDataType.STRING);
        
        // FIELD columns for measurements
        schema.addField("temperature", TSDataType.DOUBLE, TSEncoding.GORILLA);
        schema.addField("humidity", TSDataType.DOUBLE, TSEncoding.GORILLA);
        
        // Create writer with builder pattern
        try (ITsFileWriter writer = new TsFileWriterBuilder()
                .file(new File("data_v4.tsfile"))
                .tableSchema(schema)
                .build()) {
            
            // Write data in batches (more efficient)
            Tablet tablet = new Tablet(schema);
            
            // Add rows (timestamp, tag values, field values)
            tablet.addRow(1000L, "device1", 25.5, 60.2);
            tablet.addRow(2000L, "device1", 26.1, 61.0);
            
            // Write the batch
            writer.write(tablet);
        }
    }
}
```

#### v4 Code (Tree Model Compatibility) - If Needed

```java
import org.apache.tsfile.read.v4.TsFileTreeReader;
import org.apache.tsfile.write.v4.TsFileTreeWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;

import java.io.File;

public class V4WriterTreeCompatibility {
    public void writeData() throws Exception {
        // v4 still supports tree model API for backward compatibility
        try (TsFileTreeWriter writer = new TsFileTreeWriter(new File("data_v4_tree.tsfile"))) {
            // Same API as v3
            writer.registerTimeseries(
                new Path("root.sensor.device1"),
                new MeasurementSchema("temperature", TSDataType.DOUBLE, TSEncoding.GORILLA)
            );
            
            TSRecord record = new TSRecord(1000L, "root.sensor.device1");
            record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "temperature", 25.5));
            writer.write(record);
        }
    }
}
```

### Example 2: Multi-Device Write with Tags

#### v3 Code

```java
public class V3MultiDeviceWriter {
    public void writeMultiDeviceData() throws Exception {
        try (TsFileWriter writer = new TsFileWriter(new File("devices_v3.tsfile"))) {
            // Register measurements for each device separately
            String[] devices = {"device1", "device2", "device3"};
            
            for (String device : devices) {
                writer.registerTimeseries(
                    new Path("root.factory.beijing." + device),
                    new MeasurementSchema("temperature", TSDataType.DOUBLE, TSEncoding.GORILLA)
                );
                writer.registerTimeseries(
                    new Path("root.factory.beijing." + device),
                    new MeasurementSchema("status", TSDataType.BOOLEAN, TSEncoding.RLE)
                );
            }
            
            // Write data for each device
            for (int i = 0; i < 1000; i++) {
                long timestamp = i * 1000L;
                for (String device : devices) {
                    TSRecord record = new TSRecord(timestamp, "root.factory.beijing." + device);
                    record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "temperature", 20.0 + i * 0.1));
                    record.addTuple(DataPoint.getDataPoint(TSDataType.BOOLEAN, "status", i % 2 == 0));
                    writer.write(record);
                }
            }
        }
    }
}
```

#### v4 Code with Multiple TAG Columns

```java
public class V4MultiDeviceWriter {
    public void writeMultiDeviceData() throws Exception {
        // Define schema with multiple TAG columns for richer device identification
        TableSchema schema = new TableSchema("factory_equipment");
        
        // TAG columns (composite device ID)
        schema.addTag("region", TSDataType.STRING);
        schema.addTag("factory", TSDataType.STRING);
        schema.addTag("device", TSDataType.STRING);
        
        // FIELD columns
        schema.addField("temperature", TSDataType.DOUBLE, TSEncoding.GORILLA);
        schema.addField("status", TSDataType.BOOLEAN, TSEncoding.RLE);
        
        try (ITsFileWriter writer = new TsFileWriterBuilder()
                .file(new File("devices_v4.tsfile"))
                .tableSchema(schema)
                .build()) {
            
            String[] devices = {"device1", "device2", "device3"};
            Tablet tablet = new Tablet(schema);
            
            // Write all data efficiently in batches
            for (int i = 0; i < 1000; i++) {
                long timestamp = i * 1000L;
                for (String device : devices) {
                    tablet.addRow(
                        timestamp,
                        "beijing",  // region TAG
                        "factory1", // factory TAG  
                        device,     // device TAG
                        20.0 + i * 0.1, // temperature FIELD
                        i % 2 == 0      // status FIELD
                    );
                }
            }
            
            writer.write(tablet);
        }
    }
}
```

### Example 3: Reading Data

#### v3 Code

```java
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class V3Reader {
    public void readData() throws Exception {
        try (TsFileSequenceReader reader = new TsFileSequenceReader("data_v3.tsfile")) {
            // Build query with paths
            List<Path> paths = new ArrayList<>();
            paths.add(new Path("root.sensor.device1.temperature"));
            paths.add(new Path("root.sensor.device1.humidity"));
            
            QueryExpression queryExpression = QueryExpression.create(paths, null);
            
            // Execute query (lower-level API)
            // Note: Full query execution requires more setup in v3
            System.out.println("Reading v3 file with device-based paths");
        }
    }
}
```

#### v4 Code (Table Model)

```java
import org.apache.tsfile.read.v4.TsFileReader;
import org.apache.tsfile.read.v4.query.QueryExpression;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.QueryDataSet;

import java.io.File;

public class V4Reader {
    public void readData() throws Exception {
        try (TsFileReader reader = new TsFileReader("data_v4.tsfile")) {
            // Query with table-based filters
            QueryExpression query = QueryExpression.create()
                .setTable("sensor_data")
                .addTagFilter("device", "device1")  // Filter by TAG
                .setTimeRange(1000L, 3000L);        // Time range
            
            QueryDataSet dataSet = reader.query(query);
            
            // Iterate results
            while (dataSet.hasNext()) {
                RowRecord record = dataSet.next();
                long timestamp = record.getTimestamp();
                
                // Access fields by name or index
                double temperature = record.getFields().get(0).getDoubleV();
                double humidity = record.getFields().get(1).getDoubleV();
                
                System.out.printf("Time=%d, Temp=%.2f, Humidity=%.2f%n", 
                    timestamp, temperature, humidity);
            }
        }
    }
}
```

#### v4 Code (Tree Compatibility)

```java
import org.apache.tsfile.read.v4.TsFileTreeReader;
import org.apache.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;

public class V4TreeReader {
    public void readData() throws Exception {
        // v4 can still read using tree-based API
        try (TsFileTreeReader reader = new TsFileTreeReader("data_v4.tsfile")) {
            // Same paths as v3
            List<Path> paths = new ArrayList<>();
            paths.add(new Path("root.sensor.device1.temperature"));
            
            // Use tree-based query (for backward compatibility)
            System.out.println("Reading v4 file with tree-based API");
        }
    }
}
```

## Schema Design Migration

### Mapping Device Hierarchy to Table Schema

#### v3 Device Hierarchy
```
root.factory.beijing.workshop1.device001
     └── temperature
     └── humidity
     └── pressure

root.factory.shanghai.workshop2.device002
     └── temperature
     └── humidity
     └── pressure
```

#### v4 Table Schema Equivalent
```java
TableSchema schema = new TableSchema("factory_sensors");

// Extract hierarchy levels as TAG columns
schema.addTag("factory", TSDataType.STRING);    // "beijing", "shanghai"
schema.addTag("workshop", TSDataType.STRING);   // "workshop1", "workshop2"
schema.addTag("device", TSDataType.STRING);     // "device001", "device002"

// Measurements become FIELD columns
schema.addField("temperature", TSDataType.DOUBLE, TSEncoding.GORILLA);
schema.addField("humidity", TSDataType.DOUBLE, TSEncoding.GORILLA);
schema.addField("pressure", TSDataType.DOUBLE, TSEncoding.GORILLA);
```

**Benefits:**
- Explicit schema definition
- Better query optimization with TAG filters
- Clearer data organization
- Easier to extend with new TAG dimensions

### Design Patterns

#### Pattern 1: Flat Device ID (Simple)

**v3:**
```
root.sensors.device123
```

**v4:**
```java
schema.addTag("device_id", TSDataType.STRING); // "device123"
```

#### Pattern 2: Hierarchical Device ID (Recommended)

**v3:**
```
root.region.city.building.floor.room.device
```

**v4:**
```java
schema.addTag("region", TSDataType.STRING);
schema.addTag("city", TSDataType.STRING);
schema.addTag("building", TSDataType.STRING);
schema.addTag("floor", TSDataType.STRING);
schema.addTag("room", TSDataType.STRING);
schema.addTag("device", TSDataType.STRING);
```

#### Pattern 3: No Device ID (Minimal)

If all data belongs to a single logical device:

**v4:**
```java
// No TAG columns needed!
TableSchema schema = new TableSchema("single_device_data");
schema.addField("measurement1", TSDataType.DOUBLE, TSEncoding.GORILLA);
schema.addField("measurement2", TSDataType.INT32, TSEncoding.TS_2DIFF);
```

## Data Migration Strategies

### Strategy 1: Direct Conversion (Recommended for Small Datasets)

```java
public class DataMigration {
    public void migrateV3ToV4(String v3File, String v4File) throws Exception {
        // Read all data from v3
        List<DataPoint> allData = readV3File(v3File);
        
        // Define v4 schema based on v3 structure
        TableSchema schema = buildSchemaFromV3(allData);
        
        // Write to v4
        try (ITsFileWriter writer = new TsFileWriterBuilder()
                .file(new File(v4File))
                .tableSchema(schema)
                .build()) {
            
            Tablet tablet = new Tablet(schema);
            for (DataPoint dp : allData) {
                tablet.addRow(dp.timestamp, dp.deviceId, dp.value);
            }
            writer.write(tablet);
        }
    }
}
```

### Strategy 2: Streaming Conversion (For Large Datasets)

```java
public class StreamingMigration {
    private static final int BATCH_SIZE = 10000;
    
    public void migrateV3ToV4Streaming(String v3File, String v4File) throws Exception {
        TableSchema schema = defineSchema();
        
        try (TsFileSequenceReader v3Reader = new TsFileSequenceReader(v3File);
             ITsFileWriter v4Writer = new TsFileWriterBuilder()
                 .file(new File(v4File))
                 .tableSchema(schema)
                 .build()) {
            
            Tablet tablet = new Tablet(schema);
            int count = 0;
            
            // Read v3 data in chunks
            Iterator<RowRecord> iterator = readV3Iterator(v3Reader);
            while (iterator.hasNext()) {
                RowRecord record = iterator.next();
                
                // Convert and add to tablet
                addRecordToTablet(tablet, record);
                count++;
                
                // Write batch when full
                if (count >= BATCH_SIZE) {
                    v4Writer.write(tablet);
                    tablet.clear();
                    count = 0;
                }
            }
            
            // Write remaining data
            if (count > 0) {
                v4Writer.write(tablet);
            }
        }
    }
}
```

### Strategy 3: Dual-Write During Transition

```java
public class DualWriter {
    private TsFileWriter v3Writer;
    private ITsFileWriter v4Writer;
    
    public void writeBoth(long timestamp, String device, double value) throws Exception {
        // Write to v3 (for backward compatibility)
        TSRecord v3Record = new TSRecord(timestamp, device);
        v3Record.addTuple(DataPoint.getDataPoint(TSDataType.DOUBLE, "measurement", value));
        v3Writer.write(v3Record);
        
        // Write to v4 (for new consumers)
        Tablet tablet = new Tablet(v4Schema);
        tablet.addRow(timestamp, device, value);
        v4Writer.write(tablet);
    }
}
```

## Maven/Gradle Dependency Updates

### Maven

**v3 (Old):**
```xml
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>1.x.x</version> <!-- v3 version -->
</dependency>
```

**v4 (New):**
```xml
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>2.x.x</version> <!-- v4 version -->
</dependency>
```

### Gradle

**v3 (Old):**
```gradle
implementation 'org.apache.tsfile:tsfile:1.x.x'
```

**v4 (New):**
```gradle
implementation 'org.apache.tsfile:tsfile:2.x.x'
```

## Testing Your Migration

### Unit Test Template

```java
import org.junit.Test;
import static org.junit.Assert.*;

public class MigrationTest {
    @Test
    public void testV3ToV4Migration() throws Exception {
        String v3File = "test_v3.tsfile";
        String v4File = "test_v4.tsfile";
        
        // 1. Create v3 file with test data
        createV3TestFile(v3File);
        
        // 2. Migrate to v4
        migrateV3ToV4(v3File, v4File);
        
        // 3. Verify data integrity
        verifyDataIntegrity(v3File, v4File);
        
        // 4. Verify statistics
        verifyStatistics(v3File, v4File);
        
        // 5. Performance comparison
        comparePerformance(v3File, v4File);
    }
    
    private void verifyDataIntegrity(String v3File, String v4File) throws Exception {
        List<DataPoint> v3Data = readAllData(v3File, 3);
        List<DataPoint> v4Data = readAllData(v4File, 4);
        
        assertEquals("Data count mismatch", v3Data.size(), v4Data.size());
        
        for (int i = 0; i < v3Data.size(); i++) {
            assertEquals("Timestamp mismatch at index " + i, 
                v3Data.get(i).timestamp, v4Data.get(i).timestamp);
            assertEquals("Value mismatch at index " + i,
                v3Data.get(i).value, v4Data.get(i).value, 0.0001);
        }
    }
}
```

### Integration Test Checklist

- [ ] Verify all data points migrated correctly
- [ ] Check timestamp ordering
- [ ] Validate data types
- [ ] Verify statistics (min, max, count, sum)
- [ ] Test query functionality
- [ ] Compare file sizes
- [ ] Measure read performance
- [ ] Measure write performance
- [ ] Test with production-like data volumes

## Troubleshooting

### Common Issues

#### Issue 1: NotCompatibleTsFileException

**Error:**
```
org.apache.tsfile.exception.NotCompatibleTsFileException: 
TsFile version 4 is not compatible with this reader
```

**Cause:** C# (or other) implementation trying to read v4 file.

**Solution:**
```
Option A: Wait for C# v4 support implementation
Option B: Use Java for v4 file reading
Option C: Convert v4 files back to v3 for C# consumption (not recommended)
```

#### Issue 2: TableSchema Not Found

**Error:**
```
java.lang.NullPointerException: TableSchema not found in metadata
```

**Cause:** Trying to read v4 file with v3 API, or schema not registered.

**Solution:**
```java
// Always define schema before writing v4
TableSchema schema = new TableSchema("my_table");
schema.addTag("device", TSDataType.STRING);
schema.addField("measurement", TSDataType.DOUBLE, TSEncoding.GORILLA);

// Register schema with writer
ITsFileWriter writer = new TsFileWriterBuilder()
    .file(file)
    .tableSchema(schema)  // Must provide schema
    .build();
```

#### Issue 3: Metadata Size Too Large

**Error:**
```
java.io.IOException: Metadata size exceeds maximum limit
```

**Cause:** Too many devices/measurements in single file.

**Solution:**
```java
// Split large files by time range or device groups
// Example: One file per day or per 1000 devices
if (deviceCount > 1000 || timeRange > ONE_DAY) {
    writer.close();
    writer = new TsFileWriterBuilder()
        .file(new File("data_" + fileIndex + ".tsfile"))
        .tableSchema(schema)
        .build();
    fileIndex++;
}
```

#### Issue 4: Performance Degradation

**Symptom:** v4 writes slower than v3.

**Cause:** Not using batched writes (tablets).

**Solution:**
```java
// BAD: Writing individual records
for (DataPoint dp : data) {
    tablet.addRow(dp.timestamp, dp.device, dp.value);
    writer.write(tablet);  // Writing after each row!
    tablet.clear();
}

// GOOD: Batching writes
Tablet tablet = new Tablet(schema);
for (DataPoint dp : data) {
    tablet.addRow(dp.timestamp, dp.device, dp.value);
    
    if (tablet.rowCount() >= 1000) {  // Batch size
        writer.write(tablet);
        tablet.clear();
    }
}
// Don't forget remaining rows
if (tablet.rowCount() > 0) {
    writer.write(tablet);
}
```

### Debug Checklist

When migration doesn't work as expected:

- [ ] Check TsFile library version (must be v4-compatible)
- [ ] Verify schema definition matches data structure
- [ ] Confirm TAG vs FIELD column classification
- [ ] Check encoding compatibility with data types
- [ ] Verify compression is supported
- [ ] Test with small dataset first
- [ ] Enable debug logging
- [ ] Check file permissions
- [ ] Verify disk space
- [ ] Monitor memory usage

### Getting Help

1. **Check documentation:**
   - [TsFile Format v4 Specification](./TSFILE_FORMAT_V4.md)
   - [Version Compatibility Matrix](./VERSION_COMPATIBILITY.md)

2. **Review examples:**
   - Java v4 examples: `/java/examples/src/main/java/org/apache/tsfile/v4/`

3. **Community support:**
   - Mailing list: dev@iotdb.apache.org
   - GitHub issues: https://github.com/apache/tsfile/issues

4. **Report bugs:**
   Include:
   - TsFile library version
   - Java version
   - Error messages
   - Minimal reproducible example
   - Expected vs actual behavior

## Performance Optimization Tips

### Write Performance

1. **Use batched writes (tablets)**
   ```java
   // Aim for 1000-10000 rows per batch
   Tablet tablet = new Tablet(schema);
   for (int i = 0; i < 10000; i++) {
       tablet.addRow(/* data */);
   }
   writer.write(tablet);
   ```

2. **Configure appropriate page size**
   ```java
   TsFileConfig config = new TsFileConfig();
   config.setPageSizeInByte(64 * 1024);  // 64KB default
   config.setMaxNumberOfPointsInPage(10000);
   ```

3. **Choose efficient encodings**
   ```java
   // INT32/INT64: Use TS_2DIFF
   schema.addField("counter", TSDataType.INT64, TSEncoding.TS_2DIFF);
   
   // FLOAT/DOUBLE: Use GORILLA
   schema.addField("temperature", TSDataType.DOUBLE, TSEncoding.GORILLA);
   
   // BOOLEAN: Use RLE
   schema.addField("status", TSDataType.BOOLEAN, TSEncoding.RLE);
   ```

### Read Performance

1. **Use filters to reduce data scanning**
   ```java
   QueryExpression query = QueryExpression.create()
       .addTagFilter("region", "beijing")  // Filter early
       .setTimeRange(startTime, endTime);   // Limit time range
   ```

2. **Enable metadata caching**
   ```java
   TsFileReader reader = new TsFileReader(file, true);  // Enable cache
   ```

3. **Read only required fields**
   ```java
   QueryExpression query = QueryExpression.create()
       .selectFields("temperature", "pressure")  // Not all fields
       .addTagFilter("device", "device1");
   ```

## Rollback Plan

If migration fails or causes issues:

### Step 1: Stop New Writes
```java
// Immediately stop writing v4 files
v4Writer.close();
```

### Step 2: Restore from Backup
```bash
# Restore v3 files from backup
cp -r /backup/tsfiles/* /data/tsfiles/
```

### Step 3: Revert Code Changes
```bash
# Revert to v3 code
git revert <migration-commit-hash>
git push
```

### Step 4: Revert Dependencies
```xml
<!-- Maven: Back to v3 -->
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>1.x.x</version>
</dependency>
```

### Step 5: Verify System
- [ ] Confirm v3 files are readable
- [ ] Test write operations
- [ ] Check data integrity
- [ ] Monitor for errors

## Summary

### Key Takeaways

1. **v4 introduces table model** - More flexible than tree model
2. **API changes required** - Use builders and explicit schemas
3. **Backward compatible** - v4 can read v3 files
4. **Not forward compatible** - v3 cannot read v4 files
5. **Java only currently** - C#/Python/C++ support pending
6. **Batch writes crucial** - Use tablets for best performance
7. **Test thoroughly** - Verify data integrity before production

### Migration Timeline

**Phase 1: Preparation (1-2 weeks)**
- Review documentation
- Set up test environment
- Create migration scripts

**Phase 2: Development (2-4 weeks)**
- Update code to v4 APIs
- Implement schema definitions
- Create test cases

**Phase 3: Testing (2-3 weeks)**
- Unit testing
- Integration testing
- Performance testing
- User acceptance testing

**Phase 4: Deployment (1-2 weeks)**
- Deploy to development
- Deploy to staging
- Monitor and validate
- Deploy to production

**Total: 6-11 weeks for complete migration**

## Next Steps

1. **Read the format specification:** [TSFILE_FORMAT_V4.md](./TSFILE_FORMAT_V4.md)
2. **Check compatibility:** [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md)
3. **Review examples:** `/java/examples/src/main/java/org/apache/tsfile/v4/`
4. **Start small:** Migrate a test dataset first
5. **Get help:** Contact the community if needed

Good luck with your migration! 🚀
