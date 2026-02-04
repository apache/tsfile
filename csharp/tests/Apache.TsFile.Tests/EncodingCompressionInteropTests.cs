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
using Apache.TsFile.Schema;
using Xunit;

namespace Apache.TsFile.Tests;

/// <summary>
/// Comprehensive encoding and compression interoperability tests.
/// Tests various combinations of data types, encoding types, and compression types.
/// </summary>
public class EncodingCompressionInteropTests
{
    #region Data Type Specific Tests
    
    [Theory]
    [InlineData(TsEncoding.Plain, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Plain, CompressionType.Snappy)]
    [InlineData(TsEncoding.Plain, CompressionType.Gzip)]
    [InlineData(TsEncoding.Plain, CompressionType.Lz4)]
    [InlineData(TsEncoding.Plain, CompressionType.Zstd)]
    [InlineData(TsEncoding.Rle, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Rle, CompressionType.Snappy)]
    [InlineData(TsEncoding.Ts2Diff, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Ts2Diff, CompressionType.Lz4)]
    [InlineData(TsEncoding.Gorilla, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.ZigZag, CompressionType.Uncompressed)]
    public void Int32_EncodingCompression_RoundTrip(TsEncoding encoding, CompressionType compression)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"int32_{encoding}_{compression}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("test_int32", TsDataType.Int32, encoding, compression);
            var values = GenerateInt32Values(100);
            
            WriteV4File(testFile, schema, values, TsDataType.Int32);
            VerifyV4File(testFile, "test_int32");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Theory]
    [InlineData(TsEncoding.Plain, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Plain, CompressionType.Snappy)]
    [InlineData(TsEncoding.Plain, CompressionType.Gzip)]
    [InlineData(TsEncoding.Plain, CompressionType.Lz4)]
    [InlineData(TsEncoding.Plain, CompressionType.Zstd)]
    [InlineData(TsEncoding.Rle, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Ts2Diff, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Gorilla, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.ZigZag, CompressionType.Uncompressed)]
    public void Int64_EncodingCompression_RoundTrip(TsEncoding encoding, CompressionType compression)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"int64_{encoding}_{compression}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("test_int64", TsDataType.Int64, encoding, compression);
            var values = GenerateInt64Values(100);
            
            WriteV4File(testFile, schema, values, TsDataType.Int64);
            VerifyV4File(testFile, "test_int64");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Theory]
    [InlineData(TsEncoding.Plain, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Plain, CompressionType.Snappy)]
    [InlineData(TsEncoding.Plain, CompressionType.Gzip)]
    [InlineData(TsEncoding.Plain, CompressionType.Lz4)]
    [InlineData(TsEncoding.Plain, CompressionType.Zstd)]
    [InlineData(TsEncoding.Gorilla, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.GorillaV1, CompressionType.Uncompressed)]
    public void Float_EncodingCompression_RoundTrip(TsEncoding encoding, CompressionType compression)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"float_{encoding}_{compression}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("test_float", TsDataType.Float, encoding, compression);
            var values = GenerateFloatValues(100);
            
            WriteV4File(testFile, schema, values, TsDataType.Float);
            VerifyV4File(testFile, "test_float");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Theory]
    [InlineData(TsEncoding.Plain, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Plain, CompressionType.Snappy)]
    [InlineData(TsEncoding.Plain, CompressionType.Gzip)]
    [InlineData(TsEncoding.Plain, CompressionType.Lz4)]
    [InlineData(TsEncoding.Plain, CompressionType.Zstd)]
    [InlineData(TsEncoding.Gorilla, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.GorillaV1, CompressionType.Uncompressed)]
    public void Double_EncodingCompression_RoundTrip(TsEncoding encoding, CompressionType compression)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"double_{encoding}_{compression}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("test_double", TsDataType.Double, encoding, compression);
            var values = GenerateDoubleValues(100);
            
            WriteV4File(testFile, schema, values, TsDataType.Double);
            VerifyV4File(testFile, "test_double");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Theory]
    [InlineData(TsEncoding.Plain, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Plain, CompressionType.Snappy)]
    [InlineData(TsEncoding.Plain, CompressionType.Gzip)]
    [InlineData(TsEncoding.Rle, CompressionType.Uncompressed)]
    public void Boolean_EncodingCompression_RoundTrip(TsEncoding encoding, CompressionType compression)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"bool_{encoding}_{compression}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("test_bool", TsDataType.Boolean, encoding, compression);
            var values = GenerateBooleanValues(100);
            
            WriteV4File(testFile, schema, values, TsDataType.Boolean);
            VerifyV4File(testFile, "test_bool");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Theory]
    [InlineData(TsEncoding.Plain, CompressionType.Uncompressed)]
    [InlineData(TsEncoding.Plain, CompressionType.Snappy)]
    [InlineData(TsEncoding.Plain, CompressionType.Gzip)]
    [InlineData(TsEncoding.Plain, CompressionType.Lz4)]
    [InlineData(TsEncoding.Plain, CompressionType.Zstd)]
    [InlineData(TsEncoding.Dictionary, CompressionType.Uncompressed)]
    public void String_EncodingCompression_RoundTrip(TsEncoding encoding, CompressionType compression)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"string_{encoding}_{compression}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("test_string", TsDataType.String, encoding, compression);
            var values = GenerateStringValues(100);
            
            WriteV4File(testFile, schema, values, TsDataType.String);
            VerifyV4File(testFile, "test_string");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    #endregion
    
    #region Multi-Column Tests with Mixed Types
    
    [Fact]
    public void MixedTypes_AllDataTypes_RoundTrip()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"mixed_types_{Guid.NewGuid()}.tsfile");
        try
        {
            // Create schema with all data types
            var schema = new TableSchema("mixed_data");
            schema.ColumnSchemas = new List<ColumnSchema>
            {
                new ColumnSchema("tag1", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("int32_col", ColumnCategory.Field, TsDataType.Int32, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("int64_col", ColumnCategory.Field, TsDataType.Int64, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("float_col", ColumnCategory.Field, TsDataType.Float, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("double_col", ColumnCategory.Field, TsDataType.Double, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("bool_col", ColumnCategory.Field, TsDataType.Boolean, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("string_col", ColumnCategory.Field, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed)
            };
            
            foreach (var col in schema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Field))
            {
                schema.AddMeasurement(new MeasurementSchema(col.Name, col.DataType, col.Encoding, col.Compression));
            }
            
            using (var writer = new TsFileWriterV4(testFile, schema))
            {
                var tablet = new TabletV4(
                    new List<string> { "tag1", "int32_col", "int64_col", "float_col", "double_col", "bool_col", "string_col" },
                    new List<TsDataType> { TsDataType.String, TsDataType.Int32, TsDataType.Int64, TsDataType.Float, TsDataType.Double, TsDataType.Boolean, TsDataType.String }
                );
                
                for (int i = 0; i < 100; i++)
                {
                    tablet.AddTimestamp(i, i * 1000L);
                    tablet.AddValue(i, "tag1", $"device_{i % 3}");
                    tablet.AddValue(i, "int32_col", i);
                    tablet.AddValue(i, "int64_col", (long)i * 1000);
                    tablet.AddValue(i, "float_col", i * 1.5f);
                    tablet.AddValue(i, "double_col", i * 2.5);
                    tablet.AddValue(i, "bool_col", i % 2 == 0);
                    tablet.AddValue(i, "string_col", $"value_{i}");
                }
                
                writer.Write(tablet);
                writer.Close();
            }
            
            VerifyV4File(testFile, "mixed_data");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Fact]
    public void MixedCompression_MultiColumn_RoundTrip()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"mixed_compression_{Guid.NewGuid()}.tsfile");
        try
        {
            // Create schema with different compression per column
            var schema = new TableSchema("compression_test");
            schema.ColumnSchemas = new List<ColumnSchema>
            {
                new ColumnSchema("tag1", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("uncompressed", ColumnCategory.Field, TsDataType.Int64, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("snappy_col", ColumnCategory.Field, TsDataType.Int64, TsEncoding.Plain, CompressionType.Snappy),
                new ColumnSchema("gzip_col", ColumnCategory.Field, TsDataType.Int64, TsEncoding.Plain, CompressionType.Gzip),
                new ColumnSchema("lz4_col", ColumnCategory.Field, TsDataType.Int64, TsEncoding.Plain, CompressionType.Lz4),
                new ColumnSchema("zstd_col", ColumnCategory.Field, TsDataType.Int64, TsEncoding.Plain, CompressionType.Zstd)
            };
            
            foreach (var col in schema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Field))
            {
                schema.AddMeasurement(new MeasurementSchema(col.Name, col.DataType, col.Encoding, col.Compression));
            }
            
            using (var writer = new TsFileWriterV4(testFile, schema))
            {
                var tablet = new TabletV4(
                    new List<string> { "tag1", "uncompressed", "snappy_col", "gzip_col", "lz4_col", "zstd_col" },
                    new List<TsDataType> { TsDataType.String, TsDataType.Int64, TsDataType.Int64, TsDataType.Int64, TsDataType.Int64, TsDataType.Int64 }
                );
                
                for (int i = 0; i < 100; i++)
                {
                    tablet.AddTimestamp(i, i * 1000L);
                    tablet.AddValue(i, "tag1", "device1");
                    tablet.AddValue(i, "uncompressed", (long)i);
                    tablet.AddValue(i, "snappy_col", (long)i * 10);
                    tablet.AddValue(i, "gzip_col", (long)i * 100);
                    tablet.AddValue(i, "lz4_col", (long)i * 1000);
                    tablet.AddValue(i, "zstd_col", (long)i * 10000);
                }
                
                writer.Write(tablet);
                writer.Close();
            }
            
            VerifyV4File(testFile, "compression_test");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    #endregion
    
    #region Edge Cases and Patterns
    
    [Theory]
    [InlineData("sequential")]
    [InlineData("repeated")]
    [InlineData("alternating")]
    [InlineData("random")]
    public void Int64_DataPatterns_RoundTrip(string pattern)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"pattern_{pattern}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("pattern_test", TsDataType.Int64, TsEncoding.Plain, CompressionType.Uncompressed);
            var values = GeneratePatternedValues(100, pattern);
            
            WriteV4File(testFile, schema, values, TsDataType.Int64);
            VerifyV4File(testFile, "pattern_test");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public void VaryingRowCounts_RoundTrip(int rowCount)
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"rows_{rowCount}_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("row_test", TsDataType.Int64, TsEncoding.Plain, CompressionType.Uncompressed);
            var values = GenerateInt64Values(rowCount);
            
            WriteV4File(testFile, schema, values, TsDataType.Int64);
            VerifyV4File(testFile, "row_test");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Fact]
    public void LargeStringValues_RoundTrip()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"large_strings_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = CreateTableSchema("large_string_test", TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed);
            var values = new List<object>();
            
            for (int i = 0; i < 50; i++)
            {
                // Generate strings of varying sizes
                var size = 100 + (i * 50);
                values.Add(new string('x', size) + "_" + i);
            }
            
            WriteV4File(testFile, schema, values, TsDataType.String);
            VerifyV4File(testFile, "large_string_test");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    [Fact]
    public void MultipleDevices_RoundTrip()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"multi_device_{Guid.NewGuid()}.tsfile");
        try
        {
            var schema = new TableSchema("multi_device");
            schema.ColumnSchemas = new List<ColumnSchema>
            {
                new ColumnSchema("region", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("device", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("value", ColumnCategory.Field, TsDataType.Double, TsEncoding.Plain, CompressionType.Uncompressed)
            };
            
            schema.AddMeasurement(new MeasurementSchema("value", TsDataType.Double, TsEncoding.Plain, CompressionType.Uncompressed));
            
            using (var writer = new TsFileWriterV4(testFile, schema))
            {
                var tablet = new TabletV4(
                    new List<string> { "region", "device", "value" },
                    new List<TsDataType> { TsDataType.String, TsDataType.String, TsDataType.Double }
                );
                
                string[] regions = { "North", "South", "East", "West" };
                string[] devices = { "D1", "D2", "D3" };
                
                int row = 0;
                foreach (var region in regions)
                {
                    foreach (var device in devices)
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            tablet.AddTimestamp(row, row * 100L);
                            tablet.AddValue(row, "region", region);
                            tablet.AddValue(row, "device", device);
                            tablet.AddValue(row, "value", row * 1.5);
                            row++;
                        }
                    }
                }
                
                writer.Write(tablet);
                writer.Close();
            }
            
            VerifyV4File(testFile, "multi_device");
        }
        finally
        {
            if (File.Exists(testFile)) File.Delete(testFile);
        }
    }
    
    #endregion
    
    #region Helper Methods
    
    private static TableSchema CreateTableSchema(string tableName, TsDataType dataType, TsEncoding encoding, CompressionType compression)
    {
        var schema = new TableSchema(tableName);
        schema.ColumnSchemas = new List<ColumnSchema>
        {
            new ColumnSchema("tag1", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
            new ColumnSchema("value", ColumnCategory.Field, dataType, encoding, compression)
        };
        schema.AddMeasurement(new MeasurementSchema("value", dataType, encoding, compression));
        return schema;
    }
    
    private static void WriteV4File(string filePath, TableSchema schema, List<object> values, TsDataType dataType)
    {
        using var writer = new TsFileWriterV4(filePath, schema);
        
        var tablet = new TabletV4(
            new List<string> { "tag1", "value" },
            new List<TsDataType> { TsDataType.String, dataType }
        );
        
        for (int i = 0; i < values.Count; i++)
        {
            tablet.AddTimestamp(i, i * 1000L);
            tablet.AddValue(i, "tag1", "device1");
            tablet.AddValue(i, "value", values[i]);
        }
        
        writer.Write(tablet);
        writer.Close();
    }
    
    private static void VerifyV4File(string filePath, string expectedTable)
    {
        Assert.True(File.Exists(filePath), $"Expected file to exist at: {filePath}");
        
        using var reader = new TsFileReaderV4(filePath);
        Assert.Equal(4, reader.FileVersion);
        Assert.True(reader.Schemas.ContainsKey(expectedTable), $"Schema should contain table '{expectedTable}'");
    }
    
    private static List<object> GenerateInt32Values(int count)
    {
        return Enumerable.Range(0, count).Select(i => (object)i).ToList();
    }
    
    private static List<object> GenerateInt64Values(int count)
    {
        return Enumerable.Range(0, count).Select(i => (object)(long)i).ToList();
    }
    
    private static List<object> GenerateFloatValues(int count)
    {
        return Enumerable.Range(0, count).Select(i => (object)(float)i).ToList();
    }
    
    private static List<object> GenerateDoubleValues(int count)
    {
        return Enumerable.Range(0, count).Select(i => (object)(double)i).ToList();
    }
    
    private static List<object> GenerateBooleanValues(int count)
    {
        return Enumerable.Range(0, count).Select(i => (object)(i % 2 == 0)).ToList();
    }
    
    private static List<object> GenerateStringValues(int count)
    {
        return Enumerable.Range(0, count).Select(i => (object)$"value_{i}").ToList();
    }
    
    private static List<object> GeneratePatternedValues(int count, string pattern)
    {
        var random = new Random(42); // Fixed seed for reproducibility
        return pattern switch
        {
            "sequential" => Enumerable.Range(0, count).Select(i => (object)(long)i).ToList(),
            "repeated" => Enumerable.Range(0, count).Select(i => (object)(long)(i / 10)).ToList(),
            "alternating" => Enumerable.Range(0, count).Select(i => (object)(i % 2 == 0 ? 100L : 200L)).ToList(),
            "random" => Enumerable.Range(0, count).Select(_ => (object)(long)random.Next(0, 10000)).ToList(),
            _ => throw new ArgumentException($"Unknown pattern: {pattern}")
        };
    }
    
    #endregion
}
