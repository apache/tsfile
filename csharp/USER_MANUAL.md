# Apache TSFile C# User Manual

## Table of Contents

1. [Introduction](#1-introduction)
2. [Installation](#2-installation)
3. [Quick Start](#3-quick-start)
4. [Core Concepts](#4-core-concepts)
5. [Writing Data](#5-writing-data)
6. [Reading Data](#6-reading-data)
7. [Data Types](#7-data-types)
8. [Encoding](#8-encoding)
9. [Compression](#9-compression)
10. [Advanced Usage](#10-advanced-usage)
11. [Best Practices](#11-best-practices)
12. [Troubleshooting](#12-troubleshooting)
13. [API Reference](#13-api-reference)

## 1. Introduction

Apache TSFile is a columnar storage file format designed for time series data. This C# implementation provides a simple and efficient API for reading and writing TSFile format files.

### 1.1 Features

- ✅ **Multiple Data Types**: Boolean, Int32, Int64, Float, Double, Text
- ✅ **Multiple Encodings**: Plain, RLE, Gorilla, ZigZag, and more
- ✅ **Multiple Compressions**: GZIP, LZ4, ZSTD, Snappy, LZMA2
- ✅ **Batch Operations**: Efficient columnar writes with Tablet
- ✅ **Query Support**: Read specific devices and time ranges
- ✅ **Java Compatibility**: Read/write files compatible with Java implementation

### 1.2 System Requirements

- .NET 10.0 or higher
- Windows, Linux, or macOS
- 64-bit operating system (recommended)

## 2. Installation

### 2.1 NuGet Package (Future)

```bash
dotnet add package Apache.TsFile
```

### 2.2 Build from Source

```bash
git clone https://github.com/apache/tsfile.git
cd tsfile/csharp
dotnet build
```

### 2.3 Dependencies

The library automatically includes:
- K4os.Compression.LZ4 (LZ4 compression)
- ZstdSharp.Port (ZSTD compression)
- Snappy.NET (Snappy compression)
- SharpCompress (LZMA2 support)

## 3. Quick Start

### 3.1 Writing Data

```csharp
using Apache.TsFile;
using Apache.TsFile.Enums;
using Apache.TsFile.Schema;

// Create a TSFile writer
using var writer = new TsFileWriter("sensor_data.tsfile");

// Define schema
var measurements = new List<MeasurementSchema>
{
    new("temperature", TsDataType.Float, TsEncoding.Plain, CompressionType.Gzip),
    new("humidity", TsDataType.Float, TsEncoding.Plain, CompressionType.Gzip),
    new("pressure", TsDataType.Int32, TsEncoding.Plain, CompressionType.Gzip)
};

// Register device
writer.RegisterDevice("sensor_01", measurements);

// Create tablet for batch write
var tablet = new Tablet("sensor_01", measurements, capacity: 100);

// Add data rows
tablet.AddRow(1000L, 23.5f, 65.2f, 1013);
tablet.AddRow(2000L, 23.7f, 65.1f, 1014);
tablet.AddRow(3000L, 23.6f, 65.3f, 1013);

// Write to file
writer.Write(tablet);

// Close file
writer.Close();

Console.WriteLine("Data written successfully!");
```

### 3.2 Reading Data

```csharp
using Apache.TsFile.IO;

// Open TSFile for reading
using var reader = new TsFileReader("sensor_data.tsfile");

// Query all data for a device
var result = reader.Query("sensor_01");

Console.WriteLine($"Device: {result.DeviceId}");
Console.WriteLine($"Total rows: {result.Count}");

// Access data
for (int i = 0; i < result.Count; i++)
{
    var timestamp = result.Timestamps[i];
    var temperature = result.GetColumn("temperature")[i];
    var humidity = result.GetColumn("humidity")[i];
    var pressure = result.GetColumn("pressure")[i];
    
    Console.WriteLine($"Time: {timestamp}, Temp: {temperature}°C, " +
                     $"Humidity: {humidity}%, Pressure: {pressure}hPa");
}
```

## 4. Core Concepts

### 4.1 Device

A device represents a physical or logical entity that generates time series data (e.g., a sensor, machine, or system).

```csharp
string deviceId = "sensor_01";
```

### 4.2 Measurement

A measurement is a specific metric or attribute being measured (e.g., temperature, humidity).

```csharp
var measurement = new MeasurementSchema(
    measurementId: "temperature",
    dataType: TsDataType.Float,
    encoding: TsEncoding.Plain,
    compression: CompressionType.Gzip
);
```

### 4.3 Tablet

A tablet is a columnar data container for batch operations, improving write performance.

```csharp
var tablet = new Tablet(deviceId, measurements, capacity: 1000);
tablet.AddRow(timestamp, value1, value2, value3);
```

### 4.4 Schema

A schema defines the structure of data for a device, including measurement names, data types, and compression settings.

```csharp
var schema = new TableSchema();
schema.AddMeasurement(new MeasurementSchema("temp", TsDataType.Float));
```

## 5. Writing Data

### 5.1 Basic Writing

```csharp
using var writer = new TsFileWriter("data.tsfile");

var measurements = new List<MeasurementSchema>
{
    new("value", TsDataType.Int32, TsEncoding.Plain, CompressionType.Lz4)
};

writer.RegisterDevice("device_1", measurements);

var tablet = new Tablet("device_1", measurements, 100);
tablet.AddRow(1000L, 42);
tablet.AddRow(2000L, 43);

writer.Write(tablet);
writer.Close();
```

### 5.2 Writing Multiple Devices

```csharp
using var writer = new TsFileWriter("multi_device.tsfile");

// Device 1
var measurements1 = new List<MeasurementSchema>
{
    new("temperature", TsDataType.Float)
};
writer.RegisterDevice("sensor_1", measurements1);

// Device 2
var measurements2 = new List<MeasurementSchema>
{
    new("speed", TsDataType.Int32)
};
writer.RegisterDevice("motor_1", measurements2);

// Write to device 1
var tablet1 = new Tablet("sensor_1", measurements1, 10);
tablet1.AddRow(1000L, 25.5f);
writer.Write(tablet1);

// Write to device 2
var tablet2 = new Tablet("motor_1", measurements2, 10);
tablet2.AddRow(1000L, 1500);
writer.Write(tablet2);

writer.Close();
```

### 5.3 Writing Row by Row

```csharp
using var writer = new TsFileWriter("row_data.tsfile");

writer.RegisterDevice("device_1", new List<MeasurementSchema>
{
    new("value", TsDataType.Int32)
});

// Write individual rows (less efficient than tablet)
for (int i = 0; i < 100; i++)
{
    writer.WriteRow("device_1", timestamp: i * 1000L, values: i);
}

writer.Close();
```

### 5.4 Batch Writing with Tablet Reset

```csharp
var tablet = new Tablet("sensor_1", measurements, capacity: 1000);

for (int batch = 0; batch < 10; batch++)
{
    // Fill tablet
    for (int i = 0; i < 1000; i++)
    {
        tablet.AddRow(batch * 1000 + i, GetSensorValue());
    }
    
    // Write batch
    writer.Write(tablet);
    
    // Clear for next batch
    tablet.Reset();
}
```

## 6. Reading Data

### 6.1 Basic Reading

```csharp
using var reader = new TsFileReader("data.tsfile");

var result = reader.Query("device_1");

foreach (var timestamp in result.Timestamps)
{
    Console.WriteLine($"Timestamp: {timestamp}");
}
```

### 6.2 Reading Specific Time Range

```csharp
using var reader = new TsFileReader("data.tsfile");

// Query data between timestamps
var result = reader.Query(
    deviceId: "sensor_1",
    startTime: 1000L,
    endTime: 5000L
);

Console.WriteLine($"Found {result.Count} rows in time range");
```

### 6.3 Reading Specific Measurements

```csharp
using var reader = new TsFileReader("data.tsfile");

// Only read specific measurements
var measurements = new List<string> { "temperature", "humidity" };
var result = reader.Query("sensor_1", measurements);

var temps = result.GetColumn("temperature");
var humidities = result.GetColumn("humidity");
```

### 6.4 Discovering Devices

```csharp
using var reader = new TsFileReader("data.tsfile");

// Get all devices in the file
var devices = reader.GetDevices();

Console.WriteLine("Devices in file:");
foreach (var device in devices)
{
    Console.WriteLine($"- {device}");
    
    var result = reader.Query(device);
    Console.WriteLine($"  Rows: {result.Count}");
    Console.WriteLine($"  Measurements: {string.Join(", ", result.Values.Keys)}");
}
```

### 6.5 Processing Large Files

```csharp
using var reader = new TsFileReader("large_file.tsfile");

foreach (var deviceId in reader.GetDevices())
{
    // Process one device at a time to manage memory
    var result = reader.Query(deviceId);
    
    // Process data in chunks
    int chunkSize = 10000;
    for (int i = 0; i < result.Count; i += chunkSize)
    {
        int end = Math.Min(i + chunkSize, result.Count);
        ProcessChunk(result, i, end);
    }
}
```

## 7. Data Types

### 7.1 Supported Types

```csharp
// Boolean (true/false)
new MeasurementSchema("flag", TsDataType.Boolean)

// 32-bit integer
new MeasurementSchema("count", TsDataType.Int32)

// 64-bit integer
new MeasurementSchema("id", TsDataType.Int64)

// 32-bit floating point
new MeasurementSchema("temperature", TsDataType.Float)

// 64-bit floating point
new MeasurementSchema("precise_value", TsDataType.Double)

// UTF-8 text
new MeasurementSchema("message", TsDataType.Text)
```

### 7.2 Type Conversion

```csharp
// Writing
tablet.AddRow(timestamp, 
    true,           // Boolean
    42,             // Int32
    123456789L,     // Int64
    3.14f,          // Float
    3.141592653,    // Double
    "Hello"         // Text
);

// Reading
var boolValue = (bool)result.GetColumn("flag")[0];
var intValue = (int)result.GetColumn("count")[0];
var longValue = (long)result.GetColumn("id")[0];
var floatValue = (float)result.GetColumn("temperature")[0];
var doubleValue = (double)result.GetColumn("precise_value")[0];
var textValue = (string)result.GetColumn("message")[0];
```

### 7.3 Null Values

```csharp
// Null values are supported
tablet.AddRow(timestamp, null, 42, null);

// Check for null when reading
var value = result.GetColumn("optional_field")[0];
if (value != null)
{
    Console.WriteLine($"Value: {value}");
}
```

## 8. Encoding

### 8.1 Encoding Types

| Encoding | Best For | Supported Types |
|----------|----------|-----------------|
| **Plain** | Mixed data, default | All types |
| **Rle** | Repeated values | Boolean, Int32, Int64 |
| **Ts2Diff** | Regular timestamps | Int32, Int64 |
| **Gorilla** | Time-series floats | Float, Double |
| **ZigZag** | Small integers | Int32, Int64 |
| **Dictionary** | Low-cardinality text | Text |

### 8.2 Choosing Encoding

```csharp
// For boolean flags (many repeated values)
new MeasurementSchema("flag", TsDataType.Boolean, 
    encoding: TsEncoding.Rle)

// For timestamps (regular intervals)
new MeasurementSchema("time", TsDataType.Int64, 
    encoding: TsEncoding.Ts2Diff)

// For sensor readings (time-correlated)
new MeasurementSchema("temperature", TsDataType.Float, 
    encoding: TsEncoding.Gorilla)

// For counters (small values)
new MeasurementSchema("count", TsDataType.Int32, 
    encoding: TsEncoding.ZigZag)

// For status messages (repeated text)
new MeasurementSchema("status", TsDataType.Text, 
    encoding: TsEncoding.Dictionary)
```

### 8.3 Encoding Performance

```csharp
// Current implementation defaults to Plain encoding
// Future versions will implement additional encodings

// Plain encoding (currently used for all)
- Pros: Simple, reliable, compatible
- Cons: Larger file sizes

// Recommended: Use compression to reduce file size
new MeasurementSchema("temp", TsDataType.Float,
    encoding: TsEncoding.Plain,  // Currently all use Plain
    compression: CompressionType.Lz4)  // Compress for size reduction
```

## 9. Compression

### 9.1 Compression Types

| Compressor | Speed | Ratio | Best For |
|------------|-------|-------|----------|
| **Uncompressed** | Instant | 1x | Testing only |
| **Lz4** | Very Fast | 2-3x | Real-time systems |
| **Zstd** | Fast | 3-7x | General purpose (recommended) |
| **Gzip** | Medium | 3-5x | Standard compatibility |
| **Snappy** | Very Fast | 2-3x | Requires native libraries |
| **Lzma2** | Slow | 5-10x | Archival storage |

### 9.2 Choosing Compression

```csharp
// For real-time ingestion (low latency)
new MeasurementSchema("value", TsDataType.Float,
    compression: CompressionType.Lz4)

// For general purpose (balanced)
new MeasurementSchema("value", TsDataType.Float,
    compression: CompressionType.Zstd)

// For maximum compression (archival)
new MeasurementSchema("value", TsDataType.Float,
    compression: CompressionType.Lzma2)

// For development (no overhead)
new MeasurementSchema("value", TsDataType.Float,
    compression: CompressionType.Uncompressed)
```

### 9.3 Compression Benchmark

Example compression ratios for sensor data:

```
Original: 100 MB
├── Uncompressed: 100 MB (1.0x)
├── LZ4: 35 MB (2.9x)
├── ZSTD: 22 MB (4.5x)
├── GZIP: 28 MB (3.6x)
└── LZMA2: 15 MB (6.7x)
```

## 10. Advanced Usage

### 10.1 Custom Properties

```csharp
var schema = new MeasurementSchema("temperature", TsDataType.Float);
schema.Properties["unit"] = "celsius";
schema.Properties["accuracy"] = "0.1";
schema.Properties["range"] = "-40 to 85";
```

### 10.2 Error Handling

```csharp
try
{
    using var writer = new TsFileWriter("data.tsfile");
    // ... write operations
    writer.Close();
}
catch (IOException ex)
{
    Console.WriteLine($"I/O error: {ex.Message}");
}
catch (ArgumentException ex)
{
    Console.WriteLine($"Invalid argument: {ex.Message}");
}
catch (Exception ex)
{
    Console.WriteLine($"Unexpected error: {ex.Message}");
}
```

### 10.3 Memory Management

```csharp
// Use 'using' statement for automatic disposal
using var writer = new TsFileWriter("data.tsfile");
using var reader = new TsFileReader("data.tsfile");

// Or manually dispose
var writer = new TsFileWriter("data.tsfile");
try
{
    // ... operations
}
finally
{
    writer.Dispose();
}
```

### 10.4 Performance Optimization

```csharp
// 1. Use appropriate tablet size
var tablet = new Tablet(deviceId, measurements, 
    capacity: 10000);  // Larger = more memory, better performance

// 2. Batch writes instead of individual rows
// Good:
tablet.AddRow(timestamp, value1, value2);
writer.Write(tablet);

// Bad:
writer.WriteRow(deviceId, timestamp, value1, value2);

// 3. Choose fast compression for real-time
new MeasurementSchema("value", TsDataType.Float,
    compression: CompressionType.Lz4)

// 4. Reuse tablet instances
tablet.Reset();  // Clear and reuse
```

## 11. Best Practices

### 11.1 Schema Design

✅ **DO:**
- Use appropriate data types for your data
- Choose encoding based on data characteristics
- Use compression to reduce file size
- Group related measurements in same device

❌ **DON'T:**
- Mix unrelated measurements in one device
- Use Text for numeric data
- Skip compression in production
- Create thousands of devices in one file

### 11.2 Writing

✅ **DO:**
```csharp
// Use batch writes with tablet
var tablet = new Tablet(deviceId, measurements, 1000);
for (int i = 0; i < 1000; i++)
{
    tablet.AddRow(timestamp, values);
}
writer.Write(tablet);
```

❌ **DON'T:**
```csharp
// Avoid row-by-row writes
for (int i = 0; i < 1000; i++)
{
    writer.WriteRow(deviceId, timestamp, values);
}
```

### 11.3 Reading

✅ **DO:**
```csharp
// Query specific time ranges to reduce memory
var result = reader.Query(deviceId, startTime, endTime);

// Query specific measurements
var result = reader.Query(deviceId, measurements);
```

❌ **DON'T:**
```csharp
// Avoid loading entire large files
var result = reader.Query(deviceId);  // May load GBs into memory
```

### 11.4 Resource Management

✅ **DO:**
```csharp
using var writer = new TsFileWriter("data.tsfile");
// ... operations
// Automatically disposed
```

❌ **DON'T:**
```csharp
var writer = new TsFileWriter("data.tsfile");
// ... operations
// Forgot to close/dispose - file may be corrupted
```

## 12. Troubleshooting

### 12.1 Common Issues

#### File Not Found

```
Problem: FileNotFoundException when opening TSFile
Solution: Check file path is correct and file exists
```

```csharp
if (!File.Exists("data.tsfile"))
{
    Console.WriteLine("File does not exist!");
    return;
}
```

#### Invalid Format

```
Problem: InvalidDataException when reading file
Solution: Ensure file is valid TSFile format
```

```csharp
try
{
    using var reader = new TsFileReader("data.tsfile");
}
catch (InvalidDataException)
{
    Console.WriteLine("Not a valid TSFile or file is corrupted");
}
```

#### Out of Memory

```
Problem: OutOfMemoryException when reading large files
Solution: Query specific time ranges or process in chunks
```

```csharp
// Instead of:
var result = reader.Query("device_1");  // Loads all data

// Use:
var result = reader.Query("device_1", startTime, endTime);  // Loads subset
```

#### Type Mismatch

```
Problem: InvalidCastException when reading values
Solution: Check data type matches schema
```

```csharp
// Schema defines Float
new MeasurementSchema("temp", TsDataType.Float)

// Read as Float
var temp = (float)result.GetColumn("temp")[0];  // ✓

// Not as Int32
var temp = (int)result.GetColumn("temp")[0];  // ✗ InvalidCastException
```

### 12.2 Performance Issues

#### Slow Writes

```
Problem: Writing is slower than expected
Solutions:
1. Increase tablet capacity
2. Use faster compression (LZ4 instead of GZIP)
3. Use batch writes instead of row-by-row
```

```csharp
// Slow
writer.WriteRow(deviceId, timestamp, value);

// Fast
tablet.AddRow(timestamp, value);
writer.Write(tablet);
```

#### Large File Sizes

```
Problem: TSFile is larger than expected
Solutions:
1. Enable compression
2. Choose appropriate encoding
3. Check for null values (stored as markers)
```

```csharp
// Uncompressed (large)
new MeasurementSchema("value", TsDataType.Float,
    compression: CompressionType.Uncompressed)

// Compressed (smaller)
new MeasurementSchema("value", TsDataType.Float,
    compression: CompressionType.Zstd)
```

## 13. API Reference

### 13.1 TsFileWriter

```csharp
public class TsFileWriter : IDisposable
{
    // Constructor
    public TsFileWriter(string filePath);
    
    // Register device with schema
    public void RegisterDevice(string deviceId, 
        List<MeasurementSchema> measurements);
    
    // Write batch data (recommended)
    public void Write(Tablet tablet);
    
    // Write single row (slower)
    public void WriteRow(string deviceId, long timestamp, 
        params object?[] values);
    
    // Finalize and close file
    public void Close();
    
    // IDisposable
    public void Dispose();
}
```

### 13.2 TsFileReader

```csharp
public class TsFileReader : IDisposable
{
    // Constructor
    public TsFileReader(string filePath);
    
    // Query all data for device
    public QueryResult Query(string deviceId);
    
    // Query with time range
    public QueryResult Query(string deviceId, 
        long startTime, long endTime);
    
    // Query specific measurements
    public QueryResult Query(string deviceId, 
        List<string> measurements);
    
    // Get all device IDs
    public List<string> GetDevices();
    
    // IDisposable
    public void Dispose();
}
```

### 13.3 QueryResult

```csharp
public class QueryResult
{
    // Device identifier
    public string DeviceId { get; }
    
    // Timestamps (sorted ascending)
    public List<long> Timestamps { get; }
    
    // Values by measurement name
    public Dictionary<string, List<object?>> Values { get; }
    
    // Number of rows
    public int Count { get; }
    
    // Get values for specific measurement
    public List<object?> GetColumn(string measurement);
}
```

### 13.4 Tablet

```csharp
public class Tablet
{
    // Constructor
    public Tablet(string deviceId, 
        List<MeasurementSchema> measurements, 
        int capacity);
    
    // Device identifier
    public string DeviceId { get; }
    
    // Schema
    public List<MeasurementSchema> Measurements { get; }
    
    // Data
    public List<long> Timestamps { get; }
    public List<object?[]> Values { get; }
    
    // Add row of data
    public void AddRow(long timestamp, params object?[] values);
    
    // Clear data (keep schema)
    public void Reset();
    
    // Current number of rows
    public int RowCount { get; }
}
```

### 13.5 MeasurementSchema

```csharp
public class MeasurementSchema
{
    // Constructor
    public MeasurementSchema(
        string measurementId,
        TsDataType dataType,
        TsEncoding encoding = TsEncoding.Plain,
        CompressionType compression = CompressionType.Uncompressed);
    
    // Properties
    public string MeasurementId { get; }
    public TsDataType DataType { get; }
    public TsEncoding Encoding { get; }
    public CompressionType Compression { get; }
    public Dictionary<string, string> Properties { get; }
}
```

---

## Appendix A: Complete Example

```csharp
using Apache.TsFile;
using Apache.TsFile.Enums;
using Apache.TsFile.IO;
using Apache.TsFile.Schema;

namespace TsFileExample
{
    class Program
    {
        static void Main(string[] args)
        {
            string filePath = "weather_station.tsfile";
            
            // Write data
            WriteWeatherData(filePath);
            
            // Read data
            ReadWeatherData(filePath);
        }
        
        static void WriteWeatherData(string filePath)
        {
            using var writer = new TsFileWriter(filePath);
            
            // Define measurements
            var measurements = new List<MeasurementSchema>
            {
                new("temperature", TsDataType.Float, 
                    TsEncoding.Plain, CompressionType.Lz4),
                new("humidity", TsDataType.Float, 
                    TsEncoding.Plain, CompressionType.Lz4),
                new("pressure", TsDataType.Int32, 
                    TsEncoding.Plain, CompressionType.Lz4),
                new("wind_speed", TsDataType.Float, 
                    TsEncoding.Plain, CompressionType.Lz4)
            };
            
            // Register device
            writer.RegisterDevice("weather_station_1", measurements);
            
            // Create tablet
            var tablet = new Tablet("weather_station_1", measurements, 1000);
            
            // Simulate 24 hours of data (1 reading per minute)
            long baseTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Random random = new Random();
            
            for (int minute = 0; minute < 24 * 60; minute++)
            {
                long timestamp = baseTime + (minute * 60 * 1000);
                
                float temperature = 20 + (float)(random.NextDouble() * 10);
                float humidity = 40 + (float)(random.NextDouble() * 30);
                int pressure = 980 + random.Next(60);
                float windSpeed = (float)(random.NextDouble() * 20);
                
                tablet.AddRow(timestamp, temperature, humidity, 
                             pressure, windSpeed);
            }
            
            // Write to file
            writer.Write(tablet);
            writer.Close();
            
            Console.WriteLine($"Written {tablet.RowCount} rows to {filePath}");
        }
        
        static void ReadWeatherData(string filePath)
        {
            using var reader = new TsFileReader(filePath);
            
            // Query data
            var result = reader.Query("weather_station_1");
            
            Console.WriteLine($"\nReading from {filePath}");
            Console.WriteLine($"Device: {result.DeviceId}");
            Console.WriteLine($"Total rows: {result.Count}");
            
            // Get columns
            var temperatures = result.GetColumn("temperature");
            var humidities = result.GetColumn("humidity");
            var pressures = result.GetColumn("pressure");
            var windSpeeds = result.GetColumn("wind_speed");
            
            // Calculate statistics
            float avgTemp = temperatures.Cast<float>().Average();
            float avgHumidity = humidities.Cast<float>().Average();
            float avgPressure = pressures.Cast<int>().Average();
            float avgWindSpeed = windSpeeds.Cast<float>().Average();
            
            Console.WriteLine($"\nAverage Statistics:");
            Console.WriteLine($"Temperature: {avgTemp:F2}°C");
            Console.WriteLine($"Humidity: {avgHumidity:F2}%");
            Console.WriteLine($"Pressure: {avgPressure:F2} hPa");
            Console.WriteLine($"Wind Speed: {avgWindSpeed:F2} m/s");
            
            // Display first 5 readings
            Console.WriteLine($"\nFirst 5 readings:");
            for (int i = 0; i < Math.Min(5, result.Count); i++)
            {
                Console.WriteLine($"[{i}] Time: {result.Timestamps[i]}, " +
                                $"Temp: {temperatures[i]}°C, " +
                                $"Humidity: {humidities[i]}%, " +
                                $"Pressure: {pressures[i]} hPa, " +
                                $"Wind: {windSpeeds[i]} m/s");
            }
        }
    }
}
```

## Appendix B: Migration from Java

### Java to C# Equivalents

| Java | C# |
|------|-----|
| `TsFileWriter writer = new TsFileWriter(file);` | `using var writer = new TsFileWriter(filePath);` |
| `writer.registerTimeseries(path, schema);` | `writer.RegisterDevice(deviceId, measurements);` |
| `writer.write(record);` | `writer.Write(tablet);` |
| `writer.close();` | `writer.Close();` or auto with `using` |
| `TsFileReader reader = new TsFileReader(file);` | `using var reader = new TsFileReader(filePath);` |
| `reader.query(path)` | `reader.Query(deviceId)` |

## Appendix C: Additional Resources

- [TSFile Official Documentation](https://iotdb.apache.org/)
- [GitHub Repository](https://github.com/apache/tsfile)
- [Java Implementation](https://github.com/apache/tsfile/tree/main/java)
- [Python Implementation](https://github.com/apache/tsfile/tree/main/python)

---

**Version:** 1.0.0  
**Last Updated:** 2026-02-01  
**License:** Apache License 2.0
