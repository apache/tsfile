namespace Apache.TsFile.Benchmarks;

/// <summary>
/// Configuration for TSFile performance benchmarks
/// </summary>
public class BenchmarkConfig
{
    /// <summary>
    /// Number of tables to create
    /// </summary>
    public int TableCount { get; set; } = 100;

    /// <summary>
    /// Number of devices per table
    /// </summary>
    public int DevicesPerTable { get; set; } = 100;

    /// <summary>
    /// Number of measurements (columns) per device
    /// </summary>
    public int MeasurementsPerDevice { get; set; } = 100;

    /// <summary>
    /// Number of rows per Tablet
    /// </summary>
    public int RowsPerTablet { get; set; } = 100;

    /// <summary>
    /// Number of Tablets to write
    /// </summary>
    public int TabletCount { get; set; } = 100;

    /// <summary>
    /// Number of iterations to run the benchmark
    /// </summary>
    public int Iterations { get; set; } = 10;

    /// <summary>
    /// Number of warm-up iterations (results discarded)
    /// </summary>
    public int WarmupIterations { get; set; } = 5;

    /// <summary>
    /// Output file path for TSFile
    /// </summary>
    public string OutputPath { get; set; } = "benchmark_output.tsfile";

    /// <summary>
    /// Get total number of data points
    /// </summary>
    public long TotalDataPoints => (long)TableCount * DevicesPerTable * MeasurementsPerDevice * RowsPerTablet * TabletCount;
}
