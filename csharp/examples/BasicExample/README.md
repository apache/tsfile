# Apache TSFile C# Library - Basic Example

This example demonstrates basic usage of the Apache TSFile C# library.

## Building and Running

```bash
cd csharp/examples/BasicExample
dotnet run
```

## What This Example Shows

1. **Writing Data**: Creating a TSFile with multiple measurements
2. **Reading Data**: Querying data from a TSFile
3. **Compression**: Using different compression algorithms

## Code Overview

### Writing Data

```csharp
using var writer = new TsFileWriter("example.tsfile");

// Define measurements
var measurements = new List<MeasurementSchema>
{
    new MeasurementSchema("temperature", TsDataType.Float),
    new MeasurementSchema("humidity", TsDataType.Int32)
};

// Register device
writer.RegisterDevice("sensor_1", measurements);

// Create tablet and add data
var tablet = new Tablet("sensor_1", measurements, 100);
tablet.AddRow(timestamp, 25.5f, 60);

// Write to file
writer.Write(tablet);
writer.Close();
```

### Reading Data

```csharp
using var reader = new TsFileReader("example.tsfile");

// Query data
var result = reader.Query("sensor_1");

// Access data
for (int i = 0; i < result.Timestamps.Count; i++)
{
    var temp = result.MeasurementData["temperature"][i];
    var humidity = result.MeasurementData["humidity"][i];
    Console.WriteLine($"Time: {result.Timestamps[i]}, Temp: {temp}, Humidity: {humidity}");
}
```

## See Also

- [C# API Documentation](../README.md)
- [Apache TSFile Format Specification](https://iotdb.apache.org/UserGuide/Master/API/Programming-TsFile-API.html)
