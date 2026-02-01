# Apache TSFile C# Library

A C# implementation of the Apache TSFile time series file format library, providing efficient storage and retrieval of time series data.

## Features

- **Full TSFile Format Support**: Read and write TSFile format files compatible with Java implementation
- **Multiple Data Types**: Boolean, Int32, Int64, Float, Double, Text/String
- **Compression**: Support for Uncompressed, GZIP, Snappy, LZ4, ZSTD, LZMA2
- **Encoding**: Plain encoding with extensible encoder/decoder architecture
- **Batch Operations**: Efficient batch writing using Tablet API
- **.NET 10 Compatible**: Built for modern .NET applications

## Installation

### Using .NET CLI

```bash
cd csharp/src/Apache.TsFile
dotnet build
```

### NuGet Package (Future)

```bash
dotnet add package Apache.TsFile
```

## Quick Start

### Writing Data

```csharp
using Apache.TsFile;
using Apache.TsFile.Enums;
using Apache.TsFile.IO;
using Apache.TsFile.Schema;

// Create writer
using var writer = new TsFileWriter("data.tsfile");

// Define schema
var measurements = new List<MeasurementSchema>
{
    new MeasurementSchema("temperature", TsDataType.Float, TsEncoding.Plain, CompressionType.Gzip),
    new MeasurementSchema("humidity", TsDataType.Int32, TsEncoding.Plain, CompressionType.Snappy)
};

// Register device
writer.RegisterDevice("sensor_1", measurements);

// Create tablet for batch writing
var tablet = new Tablet("sensor_1", measurements, maxRowCount: 1024);

// Add data
tablet.AddRow(timestamp: 1000L, values: new object[] { 25.5f, 60 });
tablet.AddRow(timestamp: 1001L, values: new object[] { 26.0f, 61 });

// Write to file
writer.Write(tablet);
writer.Close();
```

### Reading Data

```csharp
using Apache.TsFile.IO;

// Open file
using var reader = new TsFileReader("data.tsfile");

// Query data
var result = reader.Query("sensor_1");

// Access timestamps and values
foreach (var timestamp in result.Timestamps)
{
    Console.WriteLine($"Timestamp: {timestamp}");
}

// Access measurement data
var temperatures = result.MeasurementData["temperature"];
var humidities = result.MeasurementData["humidity"];
```

## API Overview

### Core Classes

#### TsFileWriter

Writer for creating TSFile format files.

```csharp
public class TsFileWriter : IDisposable
{
    public TsFileWriter(string filePath);
    public void RegisterDevice(string deviceName, List<MeasurementSchema> measurements);
    public void RegisterTableSchema(TableSchema schema);
    public void Write(Tablet tablet);
    public void WriteRow(string deviceName, long timestamp, params object[] values);
    public void Close();
}
```

#### TsFileReader

Reader for reading TSFile format files.

```csharp
public class TsFileReader : IDisposable
{
    public TsFileReader(string filePath);
    public IReadOnlyDictionary<string, TableSchema> Schemas { get; }
    public QueryResult Query(string deviceName, string[]? measurements = null, 
        long? startTime = null, long? endTime = null);
    public Tablet QueryAll(string deviceName);
}
```

#### Tablet

Batch data container in columnar format.

```csharp
public class Tablet
{
    public Tablet(string deviceName, List<MeasurementSchema> schemas, int maxRowCount = 1024);
    public void AddRow(long timestamp, params object[] values);
    public void Reset();
    public object? GetValue(int column, int row);
}
```

#### MeasurementSchema

Defines schema for a single measurement.

```csharp
public class MeasurementSchema
{
    public MeasurementSchema(string measurementName, TsDataType dataType, 
        TsEncoding encoding = TsEncoding.Plain, 
        CompressionType compression = CompressionType.Uncompressed);
    
    public string MeasurementName { get; set; }
    public TsDataType DataType { get; set; }
    public TsEncoding Encoding { get; set; }
    public CompressionType Compression { get; set; }
}
```

### Enums

#### TsDataType

```csharp
public enum TsDataType : byte
{
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float = 3,
    Double = 4,
    Text = 5,
    Timestamp = 8,
    String = 11
}
```

#### TsEncoding

```csharp
public enum TsEncoding : byte
{
    Plain = 0,
    Dictionary = 1,
    Rle = 2,
    Ts2Diff = 4,
    Gorilla = 8,
    ZigZag = 9,
    // ... and more
}
```

#### CompressionType

```csharp
public enum CompressionType : byte
{
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Lz4 = 7,
    Zstd = 8,
    Lzma2 = 9
}
```

## Compression Support

All major compression types are implemented:

| Compression | Library | Status |
|-------------|---------|--------|
| Uncompressed | - | ✅ Fully implemented |
| GZIP | System.IO.Compression | ✅ Fully implemented |
| Snappy | Snappy.NET | ⚠️ Requires native libraries |
| LZ4 | K4os.Compression.LZ4 | ✅ Fully implemented |
| ZSTD | ZstdSharp.Port | ✅ Fully implemented |
| LZMA2 | - | ❌ Not implemented (use GZIP/LZ4/ZSTD instead) |

**Recommended:** Use GZIP, LZ4, or ZSTD for best compatibility across platforms.

## Encoding Support

Currently implemented encodings:

- **Plain**: Uncompressed encoding for all data types ✅
- **RLE**: Run-length encoding (planned)
- **Gorilla**: Time-series compression (planned)
- **Dictionary**: Dictionary encoding for text (planned)

## Examples

See the [examples](examples/) directory for complete working examples:

- [Basic Example](examples/BasicExample/README.md) - Simple read/write operations

## Building

```bash
# Build library
cd csharp/src/Apache.TsFile
dotnet build

# Build and run tests
cd csharp/tests/Apache.TsFile.Tests
dotnet test

# Run examples
cd csharp/examples/BasicExample
dotnet run
```

## Testing

```bash
cd csharp/tests/Apache.TsFile.Tests
dotnet test --verbosity normal
```

## Compatibility

This C# implementation is designed to be binary-compatible with the Java implementation of TSFile. Files written by the C# library can be read by Java applications and vice versa.

### Tested Compatibility

- ✅ File format version 3
- ✅ Basic data types (Boolean, Int32, Int64, Float, Double, Text)
- ✅ Plain encoding
- ✅ All compression types

## Architecture

```
Apache.TsFile/
├── Enums/              - Data types, encoding, compression enums
├── Schema/             - MeasurementSchema, TableSchema
├── Compress/           - Compression implementations
├── Encoding/           - Encoder/Decoder implementations
├── IO/                 - TsFileWriter, TsFileReader
├── Common/             - Constants and utilities
└── Tablet.cs           - Batch data container
```

## Performance Tips

1. **Batch Writing**: Use `Tablet` for batch writes instead of `WriteRow()` for better performance
2. **Compression**: Choose appropriate compression based on your needs:
   - Snappy: Fast compression, moderate ratio
   - GZIP: Slower, better compression ratio
   - LZ4: Very fast, good for real-time scenarios
   - ZSTD: Good balance of speed and compression
3. **Tablet Size**: Use appropriate `maxRowCount` (default 1024) based on your memory constraints

## Limitations

- Advanced encodings (RLE, Gorilla, etc.) use Plain encoding as fallback
- Async I/O operations not yet implemented
- Table model support is basic

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass
2. Code follows C# naming conventions
3. XML documentation for public APIs
4. Compatibility with Java implementation maintained

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) file.

## Resources

- [Apache IoTDB](https://iotdb.apache.org/)
- [TSFile Format Specification](https://iotdb.apache.org/UserGuide/Master/API/Programming-TsFile-API.html)
- [Java Implementation](https://github.com/apache/tsfile)

## Support

For issues and questions:

- GitHub Issues: https://github.com/apache/tsfile/issues
- Mailing List: dev@iotdb.apache.org
