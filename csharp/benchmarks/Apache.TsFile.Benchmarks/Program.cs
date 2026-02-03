using Apache.TsFile.Benchmarks;

Console.WriteLine("TSFile Performance Benchmark Tool");
Console.WriteLine("==================================\n");

// Parse command line arguments or use defaults
var config = new BenchmarkConfig();

if (args.Length > 0)
{
    for (int i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--tables" when i + 1 < args.Length:
                config.TableCount = int.Parse(args[++i]);
                break;
            case "--devices" when i + 1 < args.Length:
                config.DevicesPerTable = int.Parse(args[++i]);
                break;
            case "--measurements" when i + 1 < args.Length:
                config.MeasurementsPerDevice = int.Parse(args[++i]);
                break;
            case "--rows" when i + 1 < args.Length:
                config.RowsPerTablet = int.Parse(args[++i]);
                break;
            case "--tablets" when i + 1 < args.Length:
                config.TabletCount = int.Parse(args[++i]);
                break;
            case "--iterations" when i + 1 < args.Length:
                config.Iterations = int.Parse(args[++i]);
                break;
            case "--warmup" when i + 1 < args.Length:
                config.WarmupIterations = int.Parse(args[++i]);
                break;
            case "--output" when i + 1 < args.Length:
                config.OutputPath = args[++i];
                break;
            case "--help":
                PrintHelp();
                return;
        }
    }
}

// Run benchmarks
var runner = new BenchmarkRunner(config);
var results = runner.Run();

Console.WriteLine("\n=== Benchmark Complete ===");
Console.WriteLine($"Output file: {config.OutputPath}");

// Optionally clean up
if (File.Exists(config.OutputPath))
{
    Console.WriteLine($"Cleaning up test file: {config.OutputPath}");
    File.Delete(config.OutputPath);
}

static void PrintHelp()
{
    Console.WriteLine(@"
Usage: Apache.TsFile.Benchmarks [options]

Options:
  --tables N           Number of tables (default: 100)
  --devices N          Devices per table (default: 100)
  --measurements N     Measurements per device (default: 100)
  --rows N             Rows per Tablet (default: 100)
  --tablets N          Number of Tablets (default: 100)
  --iterations N       Total iterations (default: 10)
  --warmup N           Warmup iterations (default: 5)
  --output PATH        Output file path (default: benchmark_output.tsfile)
  --help               Show this help message

Example:
  Apache.TsFile.Benchmarks --tables 50 --devices 50 --iterations 5
");
}
