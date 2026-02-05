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

namespace Apache.TsFile.Schema;

/// <summary>
/// Defines the schema for a table containing multiple measurements.
/// </summary>
public class TableSchema
{
    /// <summary>
    /// Gets the list of measurement schemas in this table.
    /// </summary>
    public List<MeasurementSchema> Measurements { get; }
    
    /// <summary>
    /// Gets the list of column schemas (v4 format) in this table.
    /// </summary>
    public List<ColumnSchema>? ColumnSchemas { get; set; }
    
    /// <summary>
    /// Gets or sets the table name.
    /// </summary>
    public string TableName { get; set; }

    /// <summary>
    /// Gets or sets whether this is a logical table schema (auto-generated from tree model).
    /// </summary>
    public bool IsLogicalTable { get; set; }

    /// <summary>
    /// Gets or sets the maximum level for tree model ID columns.
    /// </summary>
    public int MaxIdLevel { get; set; }
    
    /// <summary>
    /// Initializes a new instance of the TableSchema class.
    /// </summary>
    public TableSchema(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
        
        TableName = tableName;
        Measurements = new List<MeasurementSchema>();
    }
    
    /// <summary>
    /// Adds a measurement schema to this table.
    /// </summary>
    public void AddMeasurement(MeasurementSchema measurement)
    {
        if (measurement == null)
            throw new ArgumentNullException(nameof(measurement));
        
        if (Measurements.Any(m => m.MeasurementName == measurement.MeasurementName))
            throw new ArgumentException($"Measurement {measurement.MeasurementName} already exists in table");
        
        Measurements.Add(measurement);
    }
    
    /// <summary>
    /// Gets a measurement schema by name.
    /// </summary>
    public MeasurementSchema? GetMeasurement(string measurementName)
    {
        return Measurements.FirstOrDefault(m => m.MeasurementName == measurementName);
    }
    
    /// <summary>
    /// Gets the number of measurements in this table.
    /// </summary>
    public int MeasurementCount => Measurements.Count;
    
    /// <summary>
    /// Serializes the table schema to a binary stream.
    /// </summary>
    public void Serialize(BinaryWriter writer)
    {
        writer.Write(TableName);
        writer.Write(Measurements.Count);
        
        foreach (var measurement in Measurements)
        {
            measurement.Serialize(writer);
        }
    }
    
    /// <summary>
    /// Deserializes the table schema from a binary stream.
    /// </summary>
    public static TableSchema Deserialize(BinaryReader reader)
    {
        var tableName = reader.ReadString();
        var count = reader.ReadInt32();
        
        var tableSchema = new TableSchema(tableName);
        
        for (int i = 0; i < count; i++)
        {
            var measurement = MeasurementSchema.Deserialize(reader);
            tableSchema.AddMeasurement(measurement);
        }
        
        return tableSchema;
    }
    
    /// <summary>
    /// Deserializes the table schema from a binary stream (v4 format).
    /// </summary>
    public static TableSchema DeserializeV4(string tableName, BinaryReader reader, 
        Func<int> readVarInt, Func<string> readVarIntString)
    {
        var tableSchema = new TableSchema(tableName);
        
        // Read column schemas
        var columnCount = readVarInt();
        tableSchema.ColumnSchemas = new List<ColumnSchema>();
        
        for (int i = 0; i < columnCount; i++)
        {
            var columnSchema = ColumnSchema.Deserialize(reader, readVarInt, readVarIntString);
            tableSchema.ColumnSchemas.Add(columnSchema);
            
            // Convert FIELD columns to MeasurementSchema for backward compatibility
            if (columnSchema.Category == ColumnCategory.Field)
            {
                var measurement = new MeasurementSchema(
                    columnSchema.Name,
                    columnSchema.DataType,
                    columnSchema.Encoding,
                    columnSchema.Compression
                );
                tableSchema.Measurements.Add(measurement);
            }
        }
        
        return tableSchema;
    }
    
    public override string ToString()
    {
        return $"TableSchema{{Name={TableName}, Measurements={Measurements.Count}}}";
    }

    /// <summary>
    /// Creates a logical table schema from tree model device path.
    /// </summary>
    /// <param name="devicePath">Device path like "root.db1.d1"</param>
    /// <param name="measurements">Measurement schemas</param>
    /// <param name="segmentNumForTableName">Number of segments for table name (default 3)</param>
    public static TableSchema CreateFromTreeModel(string devicePath, List<MeasurementSchema> measurements,
        int segmentNumForTableName = 3)
    {
        var segments = devicePath.Split('.');

        // For tree model, use the full device path as table name
        // This ensures each device has its own logical table
        var schema = new TableSchema(devicePath)
        {
            IsLogicalTable = true,
            MaxIdLevel = Math.Max(1, segments.Length - segmentNumForTableName + 1)
        };

        foreach (var m in measurements)
        {
            schema.AddMeasurement(m);
        }

        return schema;
    }
}
