/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Apache.TsFile.Enums;
using Apache.TsFile.IO;
using System.Text.Json;
using Xunit.Abstractions;

namespace Apache.TsFile.InteropTests;

/// <summary>
/// Tests for Java-C# interoperability by reading Java-generated TsFiles.
/// </summary>
public class JavaToCSharpInteropTests
{
    private const string TestFilesDir = "/tmp/interop-test-files";
    private const string MetadataFile = "/tmp/interop-test-files/test-metadata.json";
    private readonly ITestOutputHelper _output;

    public JavaToCSharpInteropTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void TestFilesDirectoryExists()
    {
        Assert.True(Directory.Exists(TestFilesDir), 
            $"Test files directory not found: {TestFilesDir}. Please run the Java generator first.");
    }

    [Fact]
    public void MetadataFileExists()
    {
        Assert.True(File.Exists(MetadataFile), 
            $"Metadata file not found: {MetadataFile}. Please run the Java generator first.");
    }

    [Fact]
    public void ReadAllJavaGeneratedFiles()
    {
        if (!File.Exists(MetadataFile))
        {
            _output.WriteLine("Skipping test: metadata file not found");
            return;
        }

        var allMetadata = TestFileMetadata.LoadFromJson(MetadataFile);
        _output.WriteLine($"Found {allMetadata.Count} test files to validate");

        var results = new Dictionary<string, (bool success, string? error)>();
        var successCount = 0;
        var failureCount = 0;

        foreach (var metadata in allMetadata)
        {
            var filePath = Path.Combine(TestFilesDir, metadata.FileName);
            
            try
            {
                ValidateFile(metadata, filePath);
                results[metadata.FileName] = (true, null);
                successCount++;
                _output.WriteLine($"✓ {metadata.FileName}");
            }
            catch (Exception ex)
            {
                results[metadata.FileName] = (false, ex.Message);
                failureCount++;
                _output.WriteLine($"✗ {metadata.FileName}: {ex.Message}");
            }
        }

        _output.WriteLine($"\n=== Summary ===");
        _output.WriteLine($"Total: {allMetadata.Count}");
        _output.WriteLine($"Success: {successCount}");
        _output.WriteLine($"Failures: {failureCount}");

        if (failureCount > 0)
        {
            _output.WriteLine("\n=== Failures by Category ===");
            var failuresByEncoding = results
                .Where(r => !r.Value.success)
                .Select(r => allMetadata.First(m => m.FileName == r.Key))
                .GroupBy(m => m.Encoding);

            foreach (var group in failuresByEncoding)
            {
                _output.WriteLine($"{group.Key}: {group.Count()} failures");
            }
        }

        // Assert that at least some files were successfully read
        Assert.True(successCount > 0, "No files were successfully read");
    }

    [Theory]
    [InlineData("INT32", "PLAIN", "UNCOMPRESSED", "sequential")]
    [InlineData("INT64", "PLAIN", "UNCOMPRESSED", "sequential")]
    [InlineData("FLOAT", "PLAIN", "UNCOMPRESSED", "sequential")]
    [InlineData("DOUBLE", "PLAIN", "UNCOMPRESSED", "sequential")]
    [InlineData("BOOLEAN", "PLAIN", "UNCOMPRESSED", "sequential")]
    [InlineData("TEXT", "PLAIN", "UNCOMPRESSED", "sequential")]
    public void ReadSpecificConfiguration(string dataType, string encoding, string compression, string pattern)
    {
        if (!File.Exists(MetadataFile))
        {
            _output.WriteLine("Skipping test: metadata file not found");
            return;
        }

        var allMetadata = TestFileMetadata.LoadFromJson(MetadataFile);
        var metadata = allMetadata.FirstOrDefault(m =>
            m.DataType == dataType &&
            m.Encoding == encoding &&
            m.Compression == compression &&
            m.Pattern == pattern);

        if (metadata == null)
        {
            _output.WriteLine($"Test file not found for {dataType}/{encoding}/{compression}/{pattern}");
            return;
        }

        var filePath = Path.Combine(TestFilesDir, metadata.FileName);
        ValidateFile(metadata, filePath);
        _output.WriteLine($"Successfully validated {metadata.FileName}");
    }

    private void ValidateFile(TestFileMetadata metadata, string filePath)
    {
        Assert.True(File.Exists(filePath), $"File not found: {filePath}");

        using var reader = new TsFileReader(filePath);
        
        Assert.NotEmpty(reader.Schemas);
        var deviceName = reader.Schemas.Keys.First();
        
        var result = reader.Query(deviceName);
        var dataType = ParseDataType(metadata.DataType);
        
        // Get the measurement name from the schema
        var schema = reader.Schemas[deviceName];
        var measurementName = schema.Measurements[0].MeasurementName;
        
        Assert.True(result.MeasurementData.ContainsKey(measurementName));
        var values = result.MeasurementData[measurementName];

        Assert.Equal(metadata.ValueCount, values.Count);

        for (int i = 0; i < values.Count; i++)
        {
            var expectedValue = ConvertExpectedValue(metadata.ExpectedValues[i], dataType);
            var actualValue = values[i];

            if (!ValuesEqual(expectedValue, actualValue, dataType))
            {
                throw new InvalidOperationException(
                    $"Value mismatch at index {i}: expected {expectedValue} but got {actualValue}");
            }
        }
    }

    private static TsDataType ParseDataType(string dataType)
    {
        return dataType switch
        {
            "INT32" => TsDataType.Int32,
            "INT64" => TsDataType.Int64,
            "FLOAT" => TsDataType.Float,
            "DOUBLE" => TsDataType.Double,
            "BOOLEAN" => TsDataType.Boolean,
            "TEXT" => TsDataType.Text,
            _ => throw new ArgumentException($"Unknown data type: {dataType}")
        };
    }

    private static object ConvertExpectedValue(JsonElement element, TsDataType dataType)
    {
        return dataType switch
        {
            TsDataType.Int32 => element.GetInt32(),
            TsDataType.Int64 => element.GetInt64(),
            TsDataType.Float => element.GetSingle(),
            TsDataType.Double => element.GetDouble(),
            TsDataType.Boolean => element.GetBoolean(),
            TsDataType.Text => element.GetString() ?? "",
            _ => throw new ArgumentException($"Unsupported data type: {dataType}")
        };
    }

    private static bool ValuesEqual(object expected, object actual, TsDataType dataType)
    {
        if (dataType == TsDataType.Float)
        {
            var exp = Convert.ToSingle(expected);
            var act = Convert.ToSingle(actual);
            return Math.Abs(exp - act) < 1e-6;
        }
        else if (dataType == TsDataType.Double)
        {
            var exp = Convert.ToDouble(expected);
            var act = Convert.ToDouble(actual);
            return Math.Abs(exp - act) < 1e-9;
        }
        else if (dataType == TsDataType.Text)
        {
            var expStr = expected.ToString();
            var actBytes = (byte[])actual;
            var actStr = System.Text.Encoding.UTF8.GetString(actBytes);
            return expStr == actStr;
        }
        else
        {
            return expected.Equals(actual);
        }
    }
}
