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

public class TabletTests
{
    [Fact]
    public void Tablet_Constructor_ValidatesParameters()
    {
        var schemas = new List<MeasurementSchema>
        {
            new MeasurementSchema("temp", TsDataType.Float)
        };
        
        Assert.Throws<ArgumentException>(() => 
            new Tablet("", schemas));
        
        Assert.Throws<ArgumentException>(() => 
            new Tablet("device", new List<MeasurementSchema>()));
        
        Assert.Throws<ArgumentException>(() => 
            new Tablet("device", schemas, -1));
    }
    
    [Fact]
    public void Tablet_AddRow_AddsDataCorrectly()
    {
        var schemas = new List<MeasurementSchema>
        {
            new MeasurementSchema("temp", TsDataType.Float),
            new MeasurementSchema("humidity", TsDataType.Int32)
        };
        
        var tablet = new Tablet("device_1", schemas, 10);
        
        tablet.AddRow(1000L, 25.5f, 60);
        tablet.AddRow(1001L, 26.0f, 61);
        
        Assert.Equal(2, tablet.RowCount);
        Assert.Equal(1000L, tablet.Timestamps[0]);
        Assert.Equal(1001L, tablet.Timestamps[1]);
        
        Assert.Equal(25.5f, tablet.GetValue(0, 0));
        Assert.Equal(60, tablet.GetValue(1, 0));
        Assert.Equal(26.0f, tablet.GetValue(0, 1));
        Assert.Equal(61, tablet.GetValue(1, 1));
    }
    
    [Fact]
    public void Tablet_AddRow_ThrowsWhenFull()
    {
        var schemas = new List<MeasurementSchema>
        {
            new MeasurementSchema("temp", TsDataType.Float)
        };
        
        var tablet = new Tablet("device_1", schemas, 2);
        
        tablet.AddRow(1000L, 25.5f);
        tablet.AddRow(1001L, 26.0f);
        
        Assert.Throws<InvalidOperationException>(() => 
            tablet.AddRow(1002L, 27.0f));
    }
    
    [Fact]
    public void Tablet_Reset_ClearsData()
    {
        var schemas = new List<MeasurementSchema>
        {
            new MeasurementSchema("temp", TsDataType.Float)
        };
        
        var tablet = new Tablet("device_1", schemas, 10);
        
        tablet.AddRow(1000L, 25.5f);
        tablet.AddRow(1001L, 26.0f);
        
        Assert.Equal(2, tablet.RowCount);
        
        tablet.Reset();
        
        Assert.Equal(0, tablet.RowCount);
    }
    
    [Fact]
    public void Tablet_SupportsAllDataTypes()
    {
        var schemas = new List<MeasurementSchema>
        {
            new MeasurementSchema("bool_val", TsDataType.Boolean),
            new MeasurementSchema("int_val", TsDataType.Int32),
            new MeasurementSchema("long_val", TsDataType.Int64),
            new MeasurementSchema("float_val", TsDataType.Float),
            new MeasurementSchema("double_val", TsDataType.Double),
            new MeasurementSchema("text_val", TsDataType.Text)
        };
        
        var tablet = new Tablet("device_1", schemas, 10);
        
        tablet.AddRow(1000L, true, 42, 123456789L, 3.14f, 2.718, "Hello");
        
        Assert.Equal(1, tablet.RowCount);
        Assert.Equal(true, tablet.GetValue(0, 0));
        Assert.Equal(42, tablet.GetValue(1, 0));
        Assert.Equal(123456789L, tablet.GetValue(2, 0));
        Assert.Equal(3.14f, (float)tablet.GetValue(3, 0)!, 2);
        Assert.Equal(2.718, (double)tablet.GetValue(4, 0)!, 3);
        Assert.Equal("Hello", tablet.GetValue(5, 0));
    }
}
