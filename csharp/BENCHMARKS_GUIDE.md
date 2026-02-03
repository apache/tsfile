# Running Performance Benchmarks

## Quick Start

The C# TSFile implementation includes a comprehensive benchmark tool to measure performance metrics.

### Build the Benchmark Tool

```bash
cd csharp/benchmarks/Apache.TsFile.Benchmarks
dotnet build --configuration Release
```

### Run with Default Parameters

This runs the standard benchmark with 100M data points:

```bash
dotnet run --configuration Release
```

**Note**: The default benchmark generates 100M data points and may take several minutes to complete. For a quick test, use smaller parameters (see below).

### Run with Smaller Parameters (Quick Test)

```bash
dotnet run --configuration Release -- --tables 10 --devices 10 --measurements 10 --iterations 5
```

This generates 100K data points and completes in seconds.

## Benchmark Parameters

The default configuration matches the specification:

| Parameter | Default | Description |
|-----------|---------|-------------|
| Tables | 100 | Number of tables to create |
| Devices per table | 100 | Devices in each table |
| Measurements per device | 100 | Measurement columns per device |
| Rows per Tablet | 100 | Rows in each Tablet |
| Tablets | 100 | Number of Tablets to write |
| Iterations | 10 | Total benchmark runs |
| Warmup | 5 | Initial runs to discard (for JIT warmup) |

**Total data points**: 100 × 100 × 100 × 100 × 100 = 100,000,000 (100M)

## Metrics Measured

1. **Registration Time** (ns): Time to register all devices and measurements
2. **Write Time** (ns): Time to write all data to the file
3. **Close Time** (ns): Time to close and finalize the file
4. **Query Time** (ns): Time to query the middle device
5. **File Size** (bytes): Size of the generated TSFile
6. **Memory Usage** (bytes): Peak memory consumption

## Data Configuration

- **Data Type**: Int64 (Long)
- **Encoding**: Gorilla (optimized for time-series)
- **Compression**: LZ4 (fast with good compression ratio)
- **Device ID Format**: `table_{i}.0.0.{j}` where i=table index, j=device index
- **Measurement ID Format**: `s{k}` where k=measurement index

## Command-Line Options

```bash
# Custom table/device counts
dotnet run --configuration Release -- --tables 50 --devices 50

# Custom measurements and rows
dotnet run --configuration Release -- --measurements 50 --rows 200

# More iterations for better statistics
dotnet run --configuration Release -- --iterations 20 --warmup 10

# Specify output file
dotnet run --configuration Release -- --output /tmp/benchmark.tsfile

# Show help
dotnet run --configuration Release -- --help
```

## Example Output

```
=== TSFile Performance Benchmark ===
Configuration:
  Tables: 100
  Devices per table: 100
  Measurements per device: 100
  Rows per Tablet: 100
  Tablet count: 100
  Total data points: 100,000,000
  Iterations: 10 (first 5 warmup)

Running iteration 1/10...
  Registration: 1,234,567 ns, Write: 12,345,678,901 ns, Close: 234,567 ns, Query: 123,456 ns, File Size: 1,234,567,890 bytes, Memory: 123,456,789 bytes

...

=== Using last 5 iterations for results ===

=== Aggregated Results (Average ± StdDev) ===
Registration Time: 1,234,567 ± 12,345 ns (1.23 ms)
Write Time:        12,345,678,901 ± 123,456 ns (12345.68 ms)
Close Time:        234,567 ± 2,345 ns (0.23 ms)
Query Time:        123,456 ± 1,234 ns (0.12 ms)
Total Time:        12,347,037,491 ns (12347.04 ms)
File Size:         1,234,567,890 bytes (1177.38 MB)
Memory Usage:      123,456,789 bytes (117.74 MB)
```

## Interpreting Results

### Registration Time
- Measures the overhead of device and measurement registration
- Lower is better
- Scales linearly with number of devices

### Write Time
- Main performance metric for write throughput
- Divide by total data points to get per-point write time
- Affected by encoding and compression choices

### Close Time
- Time to flush buffers and write metadata
- Usually a small fraction of total time
- Important for write latency

### Query Time
- Time to read data for a single device
- Tests read performance
- Affected by file structure and encoding

### File Size
- Measures compression effectiveness
- Gorilla + LZ4 typically achieves 70-90% compression
- Compare to uncompressed size (8 bytes × data points)

### Memory Usage
- Peak memory consumption during benchmark
- Important for resource-constrained environments
- Scales with Tablet size and buffer usage

## Tips for Accurate Results

1. **Close other applications** to reduce system noise
2. **Run on the same hardware** for comparing different configurations
3. **Use Release build** for accurate performance measurements
4. **Increase iterations** (--iterations 20) for more stable results
5. **Monitor system resources** (CPU, disk I/O) during benchmark

## Comparing Configurations

To compare different encodings or compressions, modify `BenchmarkRunner.cs`:

```csharp
// Change encoding
TsEncoding.Gorilla    // Try: Plain, RLE, ZigZag, Dictionary, Ts2Diff

// Change compression
CompressionType.Lz4   // Try: Uncompressed, Gzip, Snappy, Zstd
```

Then rebuild and run the benchmark again.

## Troubleshooting

**OutOfMemoryException**: Reduce parameters (--tables 50 --devices 50)

**Benchmark too slow**: Use smaller parameters for testing, full parameters for final results

**High variance in results**: Increase warmup iterations (--warmup 10)

## Further Information

See `csharp/benchmarks/Apache.TsFile.Benchmarks/README.md` for detailed documentation.
