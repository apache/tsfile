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

namespace Apache.TsFile.Enums;

/// <summary>
/// Enumeration of supported data types in TSFile format.
/// </summary>
public enum TsDataType : byte
{
    /// <summary>Boolean data type.</summary>
    Boolean = 0,
    
    /// <summary>32-bit integer data type.</summary>
    Int32 = 1,
    
    /// <summary>64-bit integer data type.</summary>
    Int64 = 2,
    
    /// <summary>Single-precision floating point data type.</summary>
    Float = 3,
    
    /// <summary>Double-precision floating point data type.</summary>
    Double = 4,
    
    /// <summary>Text/string data type.</summary>
    Text = 5,
    
    /// <summary>Vector data type.</summary>
    Vector = 6,
    
    /// <summary>Unknown data type.</summary>
    Unknown = 7,
    
    /// <summary>Timestamp data type.</summary>
    Timestamp = 8,
    
    /// <summary>Date data type.</summary>
    Date = 9,
    
    /// <summary>Binary large object data type.</summary>
    Blob = 10,
    
    /// <summary>String data type.</summary>
    String = 11,
    
    /// <summary>Object data type.</summary>
    Object = 12
}

/// <summary>
/// Extension methods for TsDataType enum.
/// </summary>
public static class TsDataTypeExtensions
{
    /// <summary>
    /// Gets the size in bytes of this data type.
    /// </summary>
    public static int GetDataTypeSize(this TsDataType dataType)
    {
        return dataType switch
        {
            TsDataType.Boolean => 1,
            TsDataType.Int32 => 4,
            TsDataType.Float => 4,
            TsDataType.Date => 4,
            TsDataType.Int64 => 8,
            TsDataType.Double => 8,
            TsDataType.Text => 8,
            TsDataType.Vector => 8,
            TsDataType.Blob => 8,
            TsDataType.Object => 8,
            TsDataType.String => 8,
            TsDataType.Timestamp => 8,
            _ => throw new NotSupportedException($"Unsupported data type: {dataType}")
        };
    }
    
    /// <summary>
    /// Determines if this data type is numeric.
    /// </summary>
    public static bool IsNumeric(this TsDataType dataType)
    {
        return dataType switch
        {
            TsDataType.Int32 or TsDataType.Int64 or TsDataType.Float or TsDataType.Double => true,
            _ => false
        };
    }
    
    /// <summary>
    /// Deserializes a TsDataType from a byte value.
    /// </summary>
    public static TsDataType Deserialize(byte value)
    {
        if (!Enum.IsDefined(typeof(TsDataType), value))
            throw new ArgumentException($"Invalid TsDataType value: {value}");
        return (TsDataType)value;
    }
}
