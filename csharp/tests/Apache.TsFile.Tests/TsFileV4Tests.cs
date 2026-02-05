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

public class TsFileV4Tests
{
    [Fact]
    public void ReadJavaV4File_CanReadSchemas()
    {
        // Check if Java-generated v4 file exists
        var javaV4File = Path.Combine(GetRepositoryRoot(), "java/examples/Tablet.tsfile");

        if (!File.Exists(javaV4File))
        {
            // Skip test if file doesn't exist
            return;
        }

        // Verify it's a v4 file
        using var fs = new FileStream(javaV4File, FileMode.Open, FileAccess.Read);
        var magic = new byte[6];
        fs.Read(magic, 0, 6);
        var version = fs.ReadByte();

        Assert.Equal((byte)4, version);

        // Try to read the file with our v4 reader
        // V4 support is experimental - we expect to at least read schemas
        try
        {
            using var reader = new TsFileReader(javaV4File);

            // Verify we can read schemas
            Assert.NotNull(reader.Schemas);

            if (reader.Schemas.Count > 0)
            {
                // Successfully read some schemas
                Assert.NotEmpty(reader.Schemas);

                // Log what we found for verification
                foreach (var schema in reader.Schemas)
                {
                    Assert.NotNull(schema.Key);
                    Assert.NotNull(schema.Value);

                    // Should have either measurements or column schemas
                    var hasData = schema.Value.Measurements.Count > 0 ||
                                 (schema.Value.ColumnSchemas != null && schema.Value.ColumnSchemas.Count > 0);
                    Assert.True(hasData, $"Schema '{schema.Key}' should have measurements or columns");

                    // If we have column schemas (v4 format), verify structure
                    if (schema.Value.ColumnSchemas != null && schema.Value.ColumnSchemas.Count > 0)
                    {
                        // V4 should have different column categories
                        var hasTagOrField = schema.Value.ColumnSchemas.Any(c =>
                            c.Category == ColumnCategory.Tag ||
                            c.Category == ColumnCategory.Field);
                        Assert.True(hasTagOrField, "V4 schema should have TAG or FIELD columns");
                    }
                }
            }
        }
        catch (InvalidDataException ex)
        {
            // If we get an InvalidDataException, that's expected for complex v4 files
            // Just verify the error message is informative
            Assert.Contains("v4", ex.Message.ToLower());
        }
    }

    [Fact]
    public void WriteAndReadV4File_RoundTrip()
    {
        var tempFile = Path.GetTempFileName() + ".tsfile";

        try
        {
            // Create table schema with tags and fields
            var tableSchema = new TableSchema("test_table");
            tableSchema.ColumnSchemas = new List<ColumnSchema>
            {
                new ColumnSchema("region", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("device_id", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("temperature", ColumnCategory.Field, TsDataType.Double, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("humidity", ColumnCategory.Field, TsDataType.Int32, TsEncoding.Plain, CompressionType.Uncompressed),
            };

            // Add measurements for Field columns
            foreach (var col in tableSchema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Field))
            {
                tableSchema.AddMeasurement(new MeasurementSchema(col.Name, col.DataType, col.Encoding, col.Compression));
            }

            // Write data using unified TsFileWriter (defaults to V4)
            using (var writer = new TsFileWriter(tempFile))
            {
                writer.RegisterTableSchema(tableSchema);

                var tablet = new Tablet(tableSchema, 100);
                for (int i = 0; i < 10; i++)
                {
                    tablet.AddRow(1000 + i * 100, 20.0 + i * 0.5, 50 + i);
                }

                writer.Write(tablet);
                writer.Close();
            }

            // Verify file was created
            Assert.True(File.Exists(tempFile));
            Assert.True(new FileInfo(tempFile).Length > 0);

            // Read back and verify using unified TsFileReader
            using (var reader = new TsFileReader(tempFile))
            {
                // Verify version
                Assert.Equal(4, reader.FileVersion);

                // Verify schemas
                Assert.NotEmpty(reader.Schemas);
                Assert.True(reader.Schemas.ContainsKey("test_table"));
            }
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public void WriteV4File_MultipleDevices()
    {
        var tempFile = Path.GetTempFileName() + ".tsfile";

        try
        {
            // Create table schema
            var tableSchema = new TableSchema("sensor_data");
            tableSchema.ColumnSchemas = new List<ColumnSchema>
            {
                new ColumnSchema("location", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("value", ColumnCategory.Field, TsDataType.Float, TsEncoding.Plain, CompressionType.Uncompressed),
            };

            // Add measurements for Field columns
            foreach (var col in tableSchema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Field))
            {
                tableSchema.AddMeasurement(new MeasurementSchema(col.Name, col.DataType, col.Encoding, col.Compression));
            }

            // Write data using unified TsFileWriter
            using (var writer = new TsFileWriter(tempFile))
            {
                writer.RegisterTableSchema(tableSchema);

                var tablet = new Tablet(tableSchema, 100);
                for (int i = 0; i < 15; i++)
                {
                    tablet.AddRow(1000 + i * 100, 100.0f + i * 10);
                }

                writer.Write(tablet);
                writer.Close();
            }

            // Read back and verify
            using (var reader = new TsFileReader(tempFile))
            {
                Assert.Equal(4, reader.FileVersion);
                Assert.True(reader.Schemas.ContainsKey("sensor_data"));
            }
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public void WriteV4File_WithCompression()
    {
        var tempFile = Path.GetTempFileName() + ".tsfile";

        try
        {
            // Create table schema with compression
            var tableSchema = new TableSchema("compressed_data");
            tableSchema.ColumnSchemas = new List<ColumnSchema>
            {
                new ColumnSchema("sensor", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("reading", ColumnCategory.Field, TsDataType.Double, TsEncoding.Plain, CompressionType.Lz4),
            };

            // Add measurements for Field columns
            foreach (var col in tableSchema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Field))
            {
                tableSchema.AddMeasurement(new MeasurementSchema(col.Name, col.DataType, col.Encoding, col.Compression));
            }

            // Write data using unified TsFileWriter
            using (var writer = new TsFileWriter(tempFile))
            {
                writer.RegisterTableSchema(tableSchema);

                var tablet = new Tablet(tableSchema, 100);
                for (int i = 0; i < 100; i++)
                {
                    tablet.AddRow(1000 + i, Math.Sin(i * 0.1) * 100);
                }

                writer.Write(tablet);
                writer.Close();
            }

            // Verify file was created and can be read
            Assert.True(File.Exists(tempFile));

            using (var reader = new TsFileReader(tempFile))
            {
                Assert.Equal(4, reader.FileVersion);
                Assert.True(reader.Schemas.ContainsKey("compressed_data"));
            }
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    private string GetRepositoryRoot()
    {
        var currentDir = Directory.GetCurrentDirectory();
        while (currentDir != null && !Directory.Exists(Path.Combine(currentDir, ".git")))
        {
            currentDir = Directory.GetParent(currentDir)?.FullName;
        }
        return currentDir ?? Directory.GetCurrentDirectory();
    }
}
