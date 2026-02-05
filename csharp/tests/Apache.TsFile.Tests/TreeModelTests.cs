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
/// Tests for tree model (Device/Measurement) data organization.
/// </summary>
public class TreeModelTests
{
    [Fact]
    public void RegisterTimeseries_CreatesLogicalTableSchema()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"tree_model_{Guid.NewGuid()}.tsfile");

        try
        {
            var measurements = new List<MeasurementSchema>
            {
                new MeasurementSchema("temperature", TsDataType.Float, TsEncoding.Plain, CompressionType.Uncompressed),
                new MeasurementSchema("humidity", TsDataType.Int32, TsEncoding.Plain, CompressionType.Uncompressed)
            };

            using (var writer = new TsFileWriter(testFile))
            {
                writer.RegisterTimeseries("root.db1.device1", measurements);

                var tablet = new Tablet("root.db1.device1", measurements, 100);
                for (int i = 0; i < 10; i++)
                {
                    tablet.AddRow(1000L + i, 25.5f + i, 60 + i);
                }

                writer.Write(tablet);
                writer.Close();
            }

            Assert.True(File.Exists(testFile));

            using (var reader = new TsFileReader(testFile))
            {
                Assert.Equal(4, reader.FileVersion);
                Assert.NotEmpty(reader.Schemas);
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }

    [Fact]
    public void StringArrayDeviceID_FromTreePath_ConvertsCorrectly()
    {
        var deviceId = new StringArrayDeviceID("root.db1.device1");

        Assert.False(deviceId.IsEmpty);
        Assert.False(deviceId.IsTableModel);
        Assert.Equal("root.db1.device1", deviceId.GetTableName());
    }

    [Fact]
    public void StringArrayDeviceID_FromTableModel_ConvertsCorrectly()
    {
        var deviceId = new StringArrayDeviceID("sensor_data", "Beijing", "Device1");

        Assert.False(deviceId.IsEmpty);
        Assert.True(deviceId.IsTableModel);
        Assert.Equal("sensor_data", deviceId.GetTableName());
        Assert.Equal(3, deviceId.SegmentCount);
    }

    [Fact]
    public void TableSchema_CreateFromTreeModel_GeneratesCorrectSchema()
    {
        var measurements = new List<MeasurementSchema>
        {
            new MeasurementSchema("s1", TsDataType.Int32),
            new MeasurementSchema("s2", TsDataType.Double)
        };

        var schema = TableSchema.CreateFromTreeModel("root.db1.d1", measurements);

        Assert.True(schema.IsLogicalTable);
        Assert.Equal("root.db1.d1", schema.TableName);
        Assert.Equal(2, schema.Measurements.Count);
    }

    [Fact]
    public void TreeModel_MultipleDevices_WriteAndRead()
    {
        var testFile = Path.Combine(Path.GetTempPath(), $"tree_multi_{Guid.NewGuid()}.tsfile");

        try
        {
            var measurements = new List<MeasurementSchema>
            {
                new MeasurementSchema("value", TsDataType.Double)
            };

            using (var writer = new TsFileWriter(testFile))
            {
                writer.RegisterTimeseries("root.db1.d1", measurements);
                writer.RegisterTimeseries("root.db1.d2", measurements);

                var tablet1 = new Tablet("root.db1.d1", measurements, 10);
                tablet1.AddRow(1000L, 100.0);
                writer.Write(tablet1);

                var tablet2 = new Tablet("root.db1.d2", measurements, 10);
                tablet2.AddRow(1000L, 200.0);
                writer.Write(tablet2);

                writer.Close();
            }

            using (var reader = new TsFileReader(testFile))
            {
                Assert.Equal(4, reader.FileVersion);
                Assert.Equal(2, reader.Schemas.Count);
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }
}
