namespace Apache.TsFile.Benchmarks;

/// <summary>
/// Results from a single benchmark iteration
/// </summary>
public class BenchmarkResult
{
    /// <summary>
    /// Time taken to register devices/measurements (nanoseconds)
    /// </summary>
    public long RegistrationTimeNs { get; set; }

    /// <summary>
    /// Time taken to write all data (nanoseconds)
    /// </summary>
    public long WriteTimeNs { get; set; }

    /// <summary>
    /// Time taken to close the file (nanoseconds)
    /// </summary>
    public long CloseTimeNs { get; set; }

    /// <summary>
    /// Time taken to query data (nanoseconds)
    /// </summary>
    public long QueryTimeNs { get; set; }

    /// <summary>
    /// Size of the output file in bytes
    /// </summary>
    public long FileSizeBytes { get; set; }

    /// <summary>
    /// Peak memory usage in bytes
    /// </summary>
    public long MemoryUsageBytes { get; set; }

    /// <summary>
    /// Total time for the iteration (nanoseconds)
    /// </summary>
    public long TotalTimeNs => RegistrationTimeNs + WriteTimeNs + CloseTimeNs + QueryTimeNs;

    public override string ToString()
    {
        return $"Registration: {RegistrationTimeNs:N0} ns, " +
               $"Write: {WriteTimeNs:N0} ns, " +
               $"Close: {CloseTimeNs:N0} ns, " +
               $"Query: {QueryTimeNs:N0} ns, " +
               $"File Size: {FileSizeBytes:N0} bytes, " +
               $"Memory: {MemoryUsageBytes:N0} bytes";
    }
}

/// <summary>
/// Aggregated results from multiple benchmark iterations
/// </summary>
public class AggregatedBenchmarkResult
{
    public double AvgRegistrationTimeNs { get; set; }
    public double AvgWriteTimeNs { get; set; }
    public double AvgCloseTimeNs { get; set; }
    public double AvgQueryTimeNs { get; set; }
    public double AvgFileSizeBytes { get; set; }
    public double AvgMemoryUsageBytes { get; set; }
    public double AvgTotalTimeNs { get; set; }

    public double StdDevRegistrationTimeNs { get; set; }
    public double StdDevWriteTimeNs { get; set; }
    public double StdDevCloseTimeNs { get; set; }
    public double StdDevQueryTimeNs { get; set; }

    public static AggregatedBenchmarkResult FromResults(List<BenchmarkResult> results)
    {
        var result = new AggregatedBenchmarkResult
        {
            AvgRegistrationTimeNs = results.Average(r => r.RegistrationTimeNs),
            AvgWriteTimeNs = results.Average(r => r.WriteTimeNs),
            AvgCloseTimeNs = results.Average(r => r.CloseTimeNs),
            AvgQueryTimeNs = results.Average(r => r.QueryTimeNs),
            AvgFileSizeBytes = results.Average(r => r.FileSizeBytes),
            AvgMemoryUsageBytes = results.Average(r => r.MemoryUsageBytes),
            AvgTotalTimeNs = results.Average(r => r.TotalTimeNs)
        };

        // Calculate standard deviations
        result.StdDevRegistrationTimeNs = CalculateStdDev(results.Select(r => (double)r.RegistrationTimeNs), result.AvgRegistrationTimeNs);
        result.StdDevWriteTimeNs = CalculateStdDev(results.Select(r => (double)r.WriteTimeNs), result.AvgWriteTimeNs);
        result.StdDevCloseTimeNs = CalculateStdDev(results.Select(r => (double)r.CloseTimeNs), result.AvgCloseTimeNs);
        result.StdDevQueryTimeNs = CalculateStdDev(results.Select(r => (double)r.QueryTimeNs), result.AvgQueryTimeNs);

        return result;
    }

    private static double CalculateStdDev(IEnumerable<double> values, double mean)
    {
        var valuesArray = values.ToArray();
        if (valuesArray.Length <= 1) return 0;

        var sumOfSquaredDiffs = valuesArray.Sum(v => Math.Pow(v - mean, 2));
        return Math.Sqrt(sumOfSquaredDiffs / (valuesArray.Length - 1));
    }

    public override string ToString()
    {
        return $"=== Aggregated Results (Average ± StdDev) ===\n" +
               $"Registration Time: {AvgRegistrationTimeNs:N0} ± {StdDevRegistrationTimeNs:N0} ns ({AvgRegistrationTimeNs / 1_000_000:F2} ms)\n" +
               $"Write Time:        {AvgWriteTimeNs:N0} ± {StdDevWriteTimeNs:N0} ns ({AvgWriteTimeNs / 1_000_000:F2} ms)\n" +
               $"Close Time:        {AvgCloseTimeNs:N0} ± {StdDevCloseTimeNs:N0} ns ({AvgCloseTimeNs / 1_000_000:F2} ms)\n" +
               $"Query Time:        {AvgQueryTimeNs:N0} ± {StdDevQueryTimeNs:N0} ns ({AvgQueryTimeNs / 1_000_000:F2} ms)\n" +
               $"Total Time:        {AvgTotalTimeNs:N0} ns ({AvgTotalTimeNs / 1_000_000:F2} ms)\n" +
               $"File Size:         {AvgFileSizeBytes:N0} bytes ({AvgFileSizeBytes / 1_024 / 1_024:F2} MB)\n" +
               $"Memory Usage:      {AvgMemoryUsageBytes:N0} bytes ({AvgMemoryUsageBytes / 1_024 / 1_024:F2} MB)";
    }
}
