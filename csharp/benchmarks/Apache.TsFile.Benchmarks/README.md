# TSFile Performance Benchmarks

This tool provides comprehensive performance benchmarks for the C# TSFile implementation.

## Overview

The benchmark measures the following metrics:
- **Registration Time**: Time to register devices and measurements (nanoseconds)
- **Write Time**: Time to write all data to the TSFile (nanoseconds)
- **Close Time**: Time to close and finalize the file (nanoseconds)
- **Query Time**: Time to query a specific device (nanoseconds)
- **File Size**: Size of the generated TSFile (bytes)
- **Memory Usage**: Peak memory usage during the benchmark (bytes)

## Default Configuration

- **Tables**: 100
- **Devices per table**: 100
- **Measurements per device**: 100
- **Rows per Tablet**: 100
- **Number of Tablets**: 100
- **Total data points**: 100,000,000 (100M)
- **Data type**: Int64 (Long)
- **Encoding**: Gorilla
- **Compression**: LZ4
- **Iterations**: 10 (first 5 are warmup, last 5 averaged)

## Building

```bash
cd csharp/benchmarks/Apache.TsFile.Benchmarks
dotnet build --configuration Release
```

## Running

### Default Configuration
```bash
dotnet run --configuration Release
```

### Custom Configuration
```bash
# Smaller benchmark
dotnet run --configuration Release -- --tables 10 --devices 10 --measurements 10

# Custom iterations
dotnet run --configuration Release -- --iterations 20 --warmup 10

# Specify output path
dotnet run --configuration Release -- --output /tmp/benchmark.tsfile
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--tables N` | Number of tables | 100 |
| `--devices N` | Devices per table | 100 |
| `--measurements N` | Measurements per device | 100 |
| `--rows N` | Rows per Tablet | 100 |
| `--tablets N` | Number of Tablets | 100 |
| `--iterations N` | Total iterations | 10 |
| `--warmup N` | Warmup iterations (discarded) | 5 |
| `--output PATH` | Output file path | benchmark_output.tsfile |
| `--help` | Show help message | - |

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

## Understanding Results

- **Registration Time**: Lower is better. Measures device/measurement registration overhead.
- **Write Time**: Lower is better. Measures data writing throughput. Divide by total data points for per-point latency.
- **Close Time**: Lower is better. Measures file finalization overhead (metadata writing).
- **Query Time**: Lower is better. Measures point query performance.
- **File Size**: Smaller is better. Measures compression effectiveness.
- **Memory Usage**: Lower is better. Measures peak memory consumption.

## Performance Tips

1. **Gorilla encoding** works best for slowly-changing numeric data
2. **LZ4 compression** provides good balance of speed and compression ratio
3. **Batch writes** using Tablets are more efficient than row-by-row writes
4. Larger Tablets reduce overhead but increase memory usage

## Comparing Encodings/Compressions

To compare different encodings or compressions, modify the `BenchmarkRunner.cs` file and change:
```csharp
TsEncoding.Gorilla    // Try: Plain, RLE, ZigZag, Dictionary, Ts2Diff
CompressionType.Lz4   // Try: Uncompressed, Gzip, Snappy, Zstd
```

Then rebuild and run the benchmark.
