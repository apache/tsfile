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
/// Tests for table model (Table/Column) data organization.
/// </summary>
public class TableModelTests
{
    [Fact]
    public void RegisterTable_WithColumnSchemas_Success()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"table_model_{Guid.NewGuid()}.tsfile");

        try
        {
            var schema = new TableSchema("sensor_data");
            schema.ColumnSchemas = new List<ColumnSchema>
            {
                new ColumnSchema("region", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("device", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("temperature", ColumnCategory.Field, TsDataType.Double, TsEncoding.Plain, CompressionType.Uncompressed),
                new ColumnSchema("humidity", ColumnCategory.Field, TsDataType.Int32, TsEncoding.Plain, CompressionType.Uncompressed)
            };

            foreach (var col in schema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Field))
            {
                schema.AddMeasurement(new MeasurementSchema(col.Name, col.DataType, col.Encoding, col.Compression));
            }

            using (var writer = new TsFileWriter(testFile))
            {
                writer.RegisterTable(schema);

                var tablet = new Tablet(schema, 100);
                for (int i = 0; i < 10; i++)
                {
                    tablet.AddRow(1000L + i, 25.5 + i, 60 + i);
                }

                writer.WriteTable(tablet);
                writer.Close();
            }

            Assert.True(File.Exists(testFile));

            using (var reader = new TsFileReader(testFile))
            {
                Assert.Equal(4, reader.FileVersion);
                Assert.True(reader.Schemas.ContainsKey("sensor_data"));
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }

    [Fact]
    public void TableSchema_WithTagsAndFields_CorrectStructure()
    {
        var schema = new TableSchema("test_table");
        schema.ColumnSchemas = new List<ColumnSchema>
        {
            new ColumnSchema("tag1", ColumnCategory.Tag, TsDataType.String, TsEncoding.Plain, CompressionType.Uncompressed),
            new ColumnSchema("field1", ColumnCategory.Field, TsDataType.Int32, TsEncoding.Plain, CompressionType.Uncompressed),
            new ColumnSchema("field2", ColumnCategory.Field, TsDataType.Double, TsEncoding.Plain, CompressionType.Uncompressed)
        };

        var tags = schema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Tag).ToList();
        var fields = schema.ColumnSchemas.Where(c => c.Category == ColumnCategory.Field).ToList();

        Assert.Single(tags);
        Assert.Equal(2, fields.Count);
        Assert.Equal("tag1", tags[0].Name);
        Assert.Equal("field1", fields[0].Name);
        Assert.Equal("field2", fields[1].Name);
    }

    [Fact]
    public void WriteTable_MultipleTables_Success()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"multi_table_{Guid.NewGuid()}.tsfile");

        try
        {
            var schema1 = new TableSchema("table1");
            schema1.AddMeasurement(new MeasurementSchema("value", TsDataType.Int32));

            var schema2 = new TableSchema("table2");
            schema2.AddMeasurement(new MeasurementSchema("reading", TsDataType.Double));

            using (var writer = new TsFileWriter(testFile))
            {
                writer.RegisterTable(schema1);
                writer.RegisterTable(schema2);

                var tablet1 = new Tablet(schema1, 10);
                tablet1.AddRow(1000L, 100);
                writer.WriteTable(tablet1);

                var tablet2 = new Tablet(schema2, 10);
                tablet2.AddRow(1000L, 3.14);
                writer.WriteTable(tablet2);

                writer.Close();
            }

            using (var reader = new TsFileReader(testFile))
            {
                Assert.Equal(4, reader.FileVersion);
                Assert.Equal(2, reader.Schemas.Count);
                Assert.True(reader.Schemas.ContainsKey("table1"));
                Assert.True(reader.Schemas.ContainsKey("table2"));
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }

    [Fact]
    public void Tablet_FromTableSchema_CorrectColumnNames()
    {
        var schema = new TableSchema("test");
        schema.AddMeasurement(new MeasurementSchema("col1", TsDataType.Int32));
        schema.AddMeasurement(new MeasurementSchema("col2", TsDataType.Double));

        var tablet = new Tablet(schema, 100);

        Assert.Equal("test", tablet.DeviceName);
        Assert.Equal("test", tablet.TableName);
        Assert.NotNull(tablet.ColumnNames);
        Assert.Equal(2, tablet.ColumnNames.Count);
        Assert.Equal("col1", tablet.ColumnNames[0]);
        Assert.Equal("col2", tablet.ColumnNames[1]);
    }
}
