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

using Apache.TsFile.IO;
using Apache.TsFile.Schema;
using Apache.TsFile.Enums;
using Xunit;

namespace Apache.TsFile.Tests;

public class TsFileIntegrationTests
{
    [Fact]
    public void WriteAndRead_SimpleData_Success()
    {
        var testFile = Path.GetTempFileName() + ".tsfile";
        
        try
        {
            // Write data
            using (var writer = new TsFileWriter(testFile))
            {
                var measurements = new List<MeasurementSchema>
                {
                    new MeasurementSchema("temperature", TsDataType.Float, TsEncoding.Plain, CompressionType.Uncompressed),
                    new MeasurementSchema("humidity", TsDataType.Int32, TsEncoding.Plain, CompressionType.Uncompressed)
                };
                
                writer.RegisterDevice("device_1", measurements);
                
                var tablet = new Tablet("device_1", measurements, 100);
                
                for (int i = 0; i < 10; i++)
                {
                    tablet.AddRow(
                        timestamp: 1000L + i,
                        values: new object[] { 25.5f + i, 60 + i }
                    );
                }
                
                writer.Write(tablet);
                writer.Close();
            }
            
            // Read data
            using (var reader = new TsFileReader(testFile))
            {
                Assert.Single(reader.Schemas);
                Assert.True(reader.Schemas.ContainsKey("device_1"));
                
                var result = reader.Query("device_1");
                
                Assert.Equal(10, result.Timestamps.Count);
                Assert.Equal(1000L, result.Timestamps[0]);
                Assert.Equal(1009L, result.Timestamps[9]);
                
                Assert.True(result.MeasurementData.ContainsKey("temperature"));
                Assert.True(result.MeasurementData.ContainsKey("humidity"));
                
                Assert.Equal(10, result.MeasurementData["temperature"].Count);
                Assert.Equal(10, result.MeasurementData["humidity"].Count);
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }
    
    [Fact]
    public void WriteAndRead_WithCompression_Success()
    {
        var testFile = Path.GetTempFileName() + ".tsfile";
        
        try
        {
            using (var writer = new TsFileWriter(testFile))
            {
                var measurements = new List<MeasurementSchema>
                {
                    new MeasurementSchema("value", TsDataType.Double, TsEncoding.Plain, CompressionType.Gzip)
                };
                
                writer.RegisterDevice("sensor_1", measurements);
                
                var tablet = new Tablet("sensor_1", measurements, 100);
                
                for (int i = 0; i < 50; i++)
                {
                    tablet.AddRow(2000L + i, new object[] { 3.14159 * i });
                }
                
                writer.Write(tablet);
                writer.Close();
            }
            
            using (var reader = new TsFileReader(testFile))
            {
                var result = reader.Query("sensor_1");
                
                Assert.Equal(50, result.Timestamps.Count);
                Assert.Equal(50, result.MeasurementData["value"].Count);
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }
    
    [Fact]
    public void WriteAndRead_MultipleDevices_Success()
    {
        var testFile = Path.GetTempFileName() + ".tsfile";
        
        try
        {
            using (var writer = new TsFileWriter(testFile))
            {
                var measurements1 = new List<MeasurementSchema>
                {
                    new MeasurementSchema("temperature", TsDataType.Float)
                };
                
                var measurements2 = new List<MeasurementSchema>
                {
                    new MeasurementSchema("pressure", TsDataType.Double)
                };
                
                writer.RegisterDevice("device_1", measurements1);
                writer.RegisterDevice("device_2", measurements2);
                
                var tablet1 = new Tablet("device_1", measurements1, 10);
                tablet1.AddRow(1000L, new object[] { 25.5f });
                writer.Write(tablet1);
                
                var tablet2 = new Tablet("device_2", measurements2, 10);
                tablet2.AddRow(1000L, new object[] { 101.325 });
                writer.Write(tablet2);
                
                writer.Close();
            }
            
            using (var reader = new TsFileReader(testFile))
            {
                Assert.Equal(2, reader.Schemas.Count);
                
                var result1 = reader.Query("device_1");
                Assert.Single(result1.Timestamps);
                
                var result2 = reader.Query("device_2");
                Assert.Single(result2.Timestamps);
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }
    
    [Fact]
    public void WriteRow_SingleValues_Success()
    {
        var testFile = Path.GetTempFileName() + ".tsfile";
        
        try
        {
            using (var writer = new TsFileWriter(testFile))
            {
                var measurements = new List<MeasurementSchema>
                {
                    new MeasurementSchema("value", TsDataType.Int32)
                };
                
                writer.RegisterDevice("device_1", measurements);
                
                writer.WriteRow("device_1", 1000L, 42);
                writer.WriteRow("device_1", 1001L, 43);
                writer.WriteRow("device_1", 1002L, 44);
                
                writer.Close();
            }
            
            using (var reader = new TsFileReader(testFile))
            {
                var result = reader.Query("device_1");
                Assert.Equal(3, result.Timestamps.Count);
            }
        }
        finally
        {
            if (File.Exists(testFile))
                File.Delete(testFile);
        }
    }
}
