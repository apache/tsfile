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
/// Column schema for TsFile v4 table-based model.
/// </summary>
public class ColumnSchema
{
    /// <summary>
    /// Gets or sets the column name.
    /// </summary>
    public string Name { get; set; }
    
    /// <summary>
    /// Gets or sets the column category (TAG, FIELD, or TIMESTAMP).
    /// </summary>
    public ColumnCategory Category { get; set; }
    
    /// <summary>
    /// Gets or sets the data type of this column.
    /// </summary>
    public TsDataType DataType { get; set; }
    
    /// <summary>
    /// Gets or sets the encoding type for this column.
    /// </summary>
    public TsEncoding Encoding { get; set; }
    
    /// <summary>
    /// Gets or sets the compression type for this column.
    /// </summary>
    public CompressionType Compression { get; set; }
    
    public ColumnSchema(string name, ColumnCategory category, TsDataType dataType, 
        TsEncoding encoding, CompressionType compression)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Category = category;
        DataType = dataType;
        Encoding = encoding;
        Compression = compression;
    }
    
    /// <summary>
    /// Deserializes a column schema from a binary stream (v4 format).
    /// </summary>
    public static ColumnSchema Deserialize(BinaryReader reader, Func<int> readVarInt, Func<string> readVarIntString)
    {
        var name = readVarIntString();
        var category = (ColumnCategory)reader.ReadByte();
        var dataType = (TsDataType)reader.ReadByte();
        var encoding = (TsEncoding)reader.ReadByte();
        var compression = (CompressionType)reader.ReadByte();
        
        return new ColumnSchema(name, category, dataType, encoding, compression);
    }
    
    public override string ToString()
    {
        return $"ColumnSchema{{Name={Name}, Category={Category}, DataType={DataType}, Encoding={Encoding}, Compression={Compression}}}";
    }
}
