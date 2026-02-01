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
/// Defines the schema for a single measurement (column) in TSFile.
/// </summary>
public class MeasurementSchema
{
    /// <summary>
    /// Gets or sets the measurement name.
    /// </summary>
    public string MeasurementName { get; set; }
    
    /// <summary>
    /// Gets or sets the data type of this measurement.
    /// </summary>
    public TsDataType DataType { get; set; }
    
    /// <summary>
    /// Gets or sets the encoding type for this measurement.
    /// </summary>
    public TsEncoding Encoding { get; set; }
    
    /// <summary>
    /// Gets or sets the compression type for this measurement.
    /// </summary>
    public CompressionType Compression { get; set; }
    
    /// <summary>
    /// Initializes a new instance of the MeasurementSchema class.
    /// </summary>
    public MeasurementSchema(
        string measurementName,
        TsDataType dataType,
        TsEncoding encoding = TsEncoding.Plain,
        CompressionType compression = CompressionType.Uncompressed)
    {
        if (string.IsNullOrWhiteSpace(measurementName))
            throw new ArgumentException("Measurement name cannot be null or empty", nameof(measurementName));
        
        if (!encoding.IsSupported(dataType))
            throw new ArgumentException($"Encoding {encoding} is not supported for data type {dataType}");
        
        MeasurementName = measurementName;
        DataType = dataType;
        Encoding = encoding;
        Compression = compression;
    }
    
    /// <summary>
    /// Serializes the schema to a binary stream.
    /// </summary>
    public void Serialize(BinaryWriter writer)
    {
        writer.Write(MeasurementName);
        writer.Write((byte)DataType);
        writer.Write((byte)Encoding);
        writer.Write((byte)Compression);
    }
    
    /// <summary>
    /// Deserializes the schema from a binary stream.
    /// </summary>
    public static MeasurementSchema Deserialize(BinaryReader reader)
    {
        var measurementName = reader.ReadString();
        var dataType = (TsDataType)reader.ReadByte();
        var encoding = (TsEncoding)reader.ReadByte();
        var compression = (CompressionType)reader.ReadByte();
        
        return new MeasurementSchema(measurementName, dataType, encoding, compression);
    }
    
    public override string ToString()
    {
        return $"MeasurementSchema{{Name={MeasurementName}, Type={DataType}, Encoding={Encoding}, Compression={Compression}}}";
    }
}
