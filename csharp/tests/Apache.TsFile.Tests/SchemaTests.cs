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

using Apache.TsFile.Schema;
using Apache.TsFile.Enums;
using Xunit;

namespace Apache.TsFile.Tests;

public class SchemaTests
{
    [Fact]
    public void MeasurementSchema_Constructor_ValidatesParameters()
    {
        Assert.Throws<ArgumentException>(() => 
            new MeasurementSchema("", TsDataType.Int32));
        
        Assert.Throws<ArgumentException>(() => 
            new MeasurementSchema("test", TsDataType.Boolean, TsEncoding.Gorilla));
    }
    
    [Fact]
    public void MeasurementSchema_SerializeDeserialize_PreservesData()
    {
        var original = new MeasurementSchema(
            "temperature", 
            TsDataType.Float, 
            TsEncoding.Plain, 
            CompressionType.Gzip);
        
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, true))
        {
            original.Serialize(writer);
        }
        
        stream.Position = 0;
        
        using var reader = new BinaryReader(stream);
        var deserialized = MeasurementSchema.Deserialize(reader);
        
        Assert.Equal(original.MeasurementName, deserialized.MeasurementName);
        Assert.Equal(original.DataType, deserialized.DataType);
        Assert.Equal(original.Encoding, deserialized.Encoding);
        Assert.Equal(original.Compression, deserialized.Compression);
    }
    
    [Fact]
    public void TableSchema_AddMeasurement_AddsSuccessfully()
    {
        var schema = new TableSchema("test_table");
        
        var measurement1 = new MeasurementSchema("temp", TsDataType.Float);
        var measurement2 = new MeasurementSchema("humidity", TsDataType.Int32);
        
        schema.AddMeasurement(measurement1);
        schema.AddMeasurement(measurement2);
        
        Assert.Equal(2, schema.MeasurementCount);
        Assert.NotNull(schema.GetMeasurement("temp"));
        Assert.NotNull(schema.GetMeasurement("humidity"));
    }
    
    [Fact]
    public void TableSchema_AddDuplicateMeasurement_ThrowsException()
    {
        var schema = new TableSchema("test_table");
        
        var measurement = new MeasurementSchema("temp", TsDataType.Float);
        
        schema.AddMeasurement(measurement);
        
        Assert.Throws<ArgumentException>(() => 
            schema.AddMeasurement(measurement));
    }
    
    [Fact]
    public void TableSchema_SerializeDeserialize_PreservesData()
    {
        var original = new TableSchema("test_table");
        original.AddMeasurement(new MeasurementSchema("temp", TsDataType.Float));
        original.AddMeasurement(new MeasurementSchema("humidity", TsDataType.Int32));
        
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, true))
        {
            original.Serialize(writer);
        }
        
        stream.Position = 0;
        
        using var reader = new BinaryReader(stream);
        var deserialized = TableSchema.Deserialize(reader);
        
        Assert.Equal(original.TableName, deserialized.TableName);
        Assert.Equal(original.MeasurementCount, deserialized.MeasurementCount);
    }
}
