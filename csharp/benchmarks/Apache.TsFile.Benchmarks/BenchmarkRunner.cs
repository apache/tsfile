using System.Diagnostics;
using Apache.TsFile.Enums;
using Apache.TsFile.IO;
using Apache.TsFile.Schema;

namespace Apache.TsFile.Benchmarks;

/// <summary>
/// Main benchmark runner for TSFile performance testing
/// </summary>
public class BenchmarkRunner
{
    private readonly BenchmarkConfig _config;
    private readonly List<BenchmarkResult> _results = new();

    public BenchmarkRunner(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Run all benchmark iterations
    /// </summary>
    public AggregatedBenchmarkResult Run()
    {
        Console.WriteLine("=== TSFile Performance Benchmark ===");
        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  Tables: {_config.TableCount}");
        Console.WriteLine($"  Devices per table: {_config.DevicesPerTable}");
        Console.WriteLine($"  Measurements per device: {_config.MeasurementsPerDevice}");
        Console.WriteLine($"  Rows per Tablet: {_config.RowsPerTablet}");
        Console.WriteLine($"  Tablet count: {_config.TabletCount}");
        Console.WriteLine($"  Total data points: {_config.TotalDataPoints:N0}");
        Console.WriteLine($"  Iterations: {_config.Iterations} (first {_config.WarmupIterations} warmup)");
        Console.WriteLine();

        for (int i = 0; i < _config.Iterations; i++)
        {
            Console.WriteLine($"Running iteration {i + 1}/{_config.Iterations}...");
            var result = RunSingleIteration();
            _results.Add(result);
            Console.WriteLine($"  {result}");
            Console.WriteLine();

            // Clean up between iterations
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        // Take last (Iterations - WarmupIterations) results for aggregation
        var validResults = _results.Skip(_config.WarmupIterations).ToList();
        Console.WriteLine($"\n=== Using last {validResults.Count} iterations for results ===\n");

        var aggregated = AggregatedBenchmarkResult.FromResults(validResults);
        Console.WriteLine(aggregated);
        Console.WriteLine();

        return aggregated;
    }

    /// <summary>
    /// Run a single benchmark iteration
    /// </summary>
    private BenchmarkResult RunSingleIteration()
    {
        var result = new BenchmarkResult();
        var sw = Stopwatch.StartNew();

        // Clean up previous file if exists
        if (File.Exists(_config.OutputPath))
        {
            File.Delete(_config.OutputPath);
        }

        // Phase 1: Registration
        TsFileWriter? writer = null;
        long memoryBefore = GC.GetTotalMemory(true);

        try
        {
            sw.Restart();
            writer = new TsFileWriter(_config.OutputPath);

            // Register devices and measurements
            for (int tableIdx = 0; tableIdx < _config.TableCount; tableIdx++)
            {
                for (int deviceIdx = 0; deviceIdx < _config.DevicesPerTable; deviceIdx++)
                {
                    string deviceId = $"table_{tableIdx}.0.0.{deviceIdx}";
                    var measurements = new List<MeasurementSchema>();

                    for (int measIdx = 0; measIdx < _config.MeasurementsPerDevice; measIdx++)
                    {
                        measurements.Add(new MeasurementSchema(
                            $"s{measIdx}",
                            TsDataType.Int64,
                            TsEncoding.Gorilla,
                            CompressionType.Lz4
                        ));
                    }

                    writer.RegisterDevice(deviceId, measurements);
                }
            }

            result.RegistrationTimeNs = sw.ElapsedTicks * 1_000_000_000 / Stopwatch.Frequency;

            // Phase 2: Write data
            sw.Restart();
            long timestamp = 0;

            for (int tabletIdx = 0; tabletIdx < _config.TabletCount; tabletIdx++)
            {
                for (int tableIdx = 0; tableIdx < _config.TableCount; tableIdx++)
                {
                    for (int deviceIdx = 0; deviceIdx < _config.DevicesPerTable; deviceIdx++)
                    {
                        string deviceId = $"table_{tableIdx}.0.0.{deviceIdx}";
                        var measurements = new List<MeasurementSchema>();

                        for (int measIdx = 0; measIdx < _config.MeasurementsPerDevice; measIdx++)
                        {
                            measurements.Add(new MeasurementSchema(
                                $"s{measIdx}",
                                TsDataType.Int64,
                                TsEncoding.Gorilla,
                                CompressionType.Lz4
                            ));
                        }

                        var tablet = new Tablet(deviceId, measurements, _config.RowsPerTablet);

                        // Fill tablet with data
                        for (int row = 0; row < _config.RowsPerTablet; row++)
                        {
                            var rowData = new object[_config.MeasurementsPerDevice];
                            for (int col = 0; col < _config.MeasurementsPerDevice; col++)
                            {
                                rowData[col] = timestamp + row + col; // Generate varying data
                            }
                            tablet.AddRow(timestamp + row, rowData);
                        }

                        writer.Write(tablet);
                        timestamp += _config.RowsPerTablet;
                    }
                }
            }

            result.WriteTimeNs = sw.ElapsedTicks * 1_000_000_000 / Stopwatch.Frequency;

            // Phase 3: Close file
            sw.Restart();
            writer.Close();
            result.CloseTimeNs = sw.ElapsedTicks * 1_000_000_000 / Stopwatch.Frequency;
            writer = null;

            // Measure file size
            if (File.Exists(_config.OutputPath))
            {
                result.FileSizeBytes = new FileInfo(_config.OutputPath).Length;
            }

            // Phase 4: Query data (middle device)
            sw.Restart();
            using (var reader = new TsFileReader(_config.OutputPath))
            {
                int middleDevice = _config.DevicesPerTable / 2;
                int middleTable = _config.TableCount / 2;
                string queryDeviceId = $"table_{middleTable}.0.0.{middleDevice}";

                var queryResult = reader.Query(queryDeviceId);

                // Verify we got data
                if (queryResult == null || queryResult.Timestamps.Count == 0)
                {
                    Console.WriteLine($"  WARNING: Query returned no data for device {queryDeviceId}");
                }
            }
            result.QueryTimeNs = sw.ElapsedTicks * 1_000_000_000 / Stopwatch.Frequency;

            // Measure memory usage
            long memoryAfter = GC.GetTotalMemory(false);
            result.MemoryUsageBytes = Math.Max(0, memoryAfter - memoryBefore);
        }
        finally
        {
            writer?.Close();
        }

        return result;
    }
}
