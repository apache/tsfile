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
using Apache.TsFile.Schema;

namespace Apache.TsFile;

/// <summary>
/// Represents a batch of time-series data in columnar format.
/// Used for efficient batch writes to TSFile.
/// </summary>
public class Tablet
{
    /// <summary>
    /// Gets the device/table name for this tablet.
    /// </summary>
    public string DeviceName { get; }

    /// <summary>
    /// Gets or sets the table name (for table model, may differ from DeviceName).
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// Gets the column names (for table model).
    /// </summary>
    public IReadOnlyList<string>? ColumnNames { get; }

    /// <summary>
    /// Gets the list of measurement schemas.
    /// </summary>
    public List<MeasurementSchema> Schemas { get; }
    
    /// <summary>
    /// Gets the timestamp column.
    /// </summary>
    public long[] Timestamps { get; }
    
    /// <summary>
    /// Gets the value columns (one per measurement).
    /// </summary>
    public object?[] Values { get; }
    
    /// <summary>
    /// Gets or sets the current row count in this tablet.
    /// </summary>
    public int RowCount { get; set; }
    
    /// <summary>
    /// Gets the maximum number of rows this tablet can hold.
    /// </summary>
    public int MaxRowCount { get; }
    
    /// <summary>
    /// Initializes a new instance of the Tablet class.
    /// </summary>
    public Tablet(string deviceName, List<MeasurementSchema> schemas, int maxRowCount = 1024)
    {
        if (string.IsNullOrWhiteSpace(deviceName))
            throw new ArgumentException("Device name cannot be null or empty", nameof(deviceName));
        
        if (schemas == null || schemas.Count == 0)
            throw new ArgumentException("Schemas cannot be null or empty", nameof(schemas));
        
        if (maxRowCount <= 0)
            throw new ArgumentException("Max row count must be positive", nameof(maxRowCount));
        
        DeviceName = deviceName;
        Schemas = schemas;
        MaxRowCount = maxRowCount;
        RowCount = 0;
        
        Timestamps = new long[maxRowCount];
        Values = new object[schemas.Count];

        // Initialize value arrays based on data types
        for (int i = 0; i < schemas.Count; i++)
        {
            Values[i] = CreateValueArray(schemas[i].DataType, maxRowCount);
        }
    }

    /// <summary>
    /// Initializes a new instance of the Tablet class for table model.
    /// </summary>
    public Tablet(TableSchema tableSchema, int maxRowCount = 1024)
        : this(tableSchema.TableName, tableSchema.Measurements, maxRowCount)
    {
        TableName = tableSchema.TableName;
        ColumnNames = tableSchema.Measurements.Select(m => m.MeasurementName).ToList();
    }
    
    /// <summary>
    /// Adds a row of data to the tablet.
    /// </summary>
    public void AddRow(long timestamp, params object[] values)
    {
        if (RowCount >= MaxRowCount)
            throw new InvalidOperationException("Tablet is full");
        
        if (values.Length != Schemas.Count)
            throw new ArgumentException($"Expected {Schemas.Count} values, got {values.Length}");
        
        Timestamps[RowCount] = timestamp;
        
        for (int i = 0; i < values.Length; i++)
        {
            SetValue(i, RowCount, values[i]);
        }
        
        RowCount++;
    }
    
    /// <summary>
    /// Resets the tablet to empty state.
    /// </summary>
    public void Reset()
    {
        RowCount = 0;
    }
    
    /// <summary>
    /// Gets a value at the specified column and row.
    /// </summary>
    public object? GetValue(int column, int row)
    {
        if (column < 0 || column >= Schemas.Count)
            throw new ArgumentOutOfRangeException(nameof(column));
        
        if (row < 0 || row >= RowCount)
            throw new ArgumentOutOfRangeException(nameof(row));
        
        var array = Values[column];
        var dataType = Schemas[column].DataType;
        
        return dataType switch
        {
            TsDataType.Boolean => ((bool[])array!)[row],
            TsDataType.Int32 => ((int[])array!)[row],
            TsDataType.Int64 => ((long[])array!)[row],
            TsDataType.Float => ((float[])array!)[row],
            TsDataType.Double => ((double[])array!)[row],
            TsDataType.Text or TsDataType.String => ((string[])array!)[row],
            _ => null
        };
    }
    
    private void SetValue(int column, int row, object? value)
    {
        var array = Values[column];
        var dataType = Schemas[column].DataType;
        
        switch (dataType)
        {
            case TsDataType.Boolean:
                ((bool[])array!)[row] = Convert.ToBoolean(value);
                break;
            case TsDataType.Int32:
                ((int[])array!)[row] = Convert.ToInt32(value);
                break;
            case TsDataType.Int64:
            case TsDataType.Timestamp:
                ((long[])array!)[row] = Convert.ToInt64(value);
                break;
            case TsDataType.Float:
                ((float[])array!)[row] = Convert.ToSingle(value);
                break;
            case TsDataType.Double:
                ((double[])array!)[row] = Convert.ToDouble(value);
                break;
            case TsDataType.Text:
            case TsDataType.String:
                ((string[])array!)[row] = value?.ToString() ?? string.Empty;
                break;
        }
    }
    
    private static object CreateValueArray(TsDataType dataType, int size)
    {
        return dataType switch
        {
            TsDataType.Boolean => new bool[size],
            TsDataType.Int32 => new int[size],
            TsDataType.Int64 => new long[size],
            TsDataType.Timestamp => new long[size],
            TsDataType.Float => new float[size],
            TsDataType.Double => new double[size],
            TsDataType.Text => new string[size],
            TsDataType.String => new string[size],
            _ => throw new NotSupportedException($"Data type {dataType} not supported")
        };
    }
}
