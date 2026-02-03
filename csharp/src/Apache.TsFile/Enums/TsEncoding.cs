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
/// Enumeration of supported encoding types in TSFile format.
/// </summary>
public enum TsEncoding : byte
{
    /// <summary>Plain encoding - no compression.</summary>
    Plain = 0,
    
    /// <summary>Dictionary encoding for text data.</summary>
    Dictionary = 1,
    
    /// <summary>Run-length encoding.</summary>
    Rle = 2,
    
    /// <summary>Differential encoding.</summary>
    Diff = 3,
    
    /// <summary>Two-differential encoding for timestamps.</summary>
    Ts2Diff = 4,
    
    /// <summary>Bitmap encoding.</summary>
    Bitmap = 5,
    
    /// <summary>Gorilla encoding version 1.</summary>
    GorillaV1 = 6,
    
    /// <summary>Regular encoding.</summary>
    Regular = 7,
    
    /// <summary>Gorilla encoding.</summary>
    Gorilla = 8,
    
    /// <summary>ZigZag encoding.</summary>
    ZigZag = 9,
    
    /// <summary>FREQ encoding (deprecated, for compatibility only).</summary>
    [Obsolete("FREQ encoding is deprecated and should not be used in new code.")]
    Freq = 10,
    
    /// <summary>CHIMP encoding.</summary>
    Chimp = 11,
    
    /// <summary>SPRINTZ encoding.</summary>
    Sprintz = 12,
    
    /// <summary>RLBE encoding.</summary>
    Rlbe = 13,
    
    /// <summary>CAMEL encoding.</summary>
    Camel = 14
}

/// <summary>
/// Extension methods for TsEncoding enum.
/// </summary>
public static class TsEncodingExtensions
{
    /// <summary>
    /// Deserializes a TsEncoding from a byte value.
    /// </summary>
    public static TsEncoding Deserialize(byte value)
    {
        return value switch
        {
            0 => TsEncoding.Plain,
            1 => TsEncoding.Dictionary,
            2 => TsEncoding.Rle,
            3 => TsEncoding.Diff,
            4 => TsEncoding.Ts2Diff,
            5 => TsEncoding.Bitmap,
            6 => TsEncoding.GorillaV1,
            7 => TsEncoding.Regular,
            8 => TsEncoding.Gorilla,
            9 => TsEncoding.ZigZag,
            10 => TsEncoding.Freq,
            11 => TsEncoding.Chimp,
            12 => TsEncoding.Sprintz,
            13 => TsEncoding.Rlbe,
            14 => TsEncoding.Camel,
            _ => throw new ArgumentException($"Invalid TsEncoding value: {value}")
        };
    }
    
    /// <summary>
    /// Checks if this encoding is supported for the given data type.
    /// </summary>
    public static bool IsSupported(this TsEncoding encoding, TsDataType dataType)
    {
        return dataType switch
        {
            TsDataType.Boolean => encoding is TsEncoding.Plain or TsEncoding.Rle,
            
            TsDataType.Int32 or TsDataType.Int64 or TsDataType.Timestamp or TsDataType.Date => 
                encoding is TsEncoding.Plain or TsEncoding.Rle or TsEncoding.Ts2Diff 
                or TsEncoding.Gorilla or TsEncoding.ZigZag or TsEncoding.Chimp 
                or TsEncoding.Sprintz or TsEncoding.Rlbe,
            
            TsDataType.Float => 
                encoding is TsEncoding.Plain or TsEncoding.Rle or TsEncoding.Ts2Diff 
                or TsEncoding.GorillaV1 or TsEncoding.Gorilla or TsEncoding.Chimp 
                or TsEncoding.Sprintz or TsEncoding.Rlbe,
            
            TsDataType.Double => 
                encoding is TsEncoding.Plain or TsEncoding.Rle or TsEncoding.Ts2Diff 
                or TsEncoding.GorillaV1 or TsEncoding.Gorilla or TsEncoding.Chimp 
                or TsEncoding.Sprintz or TsEncoding.Rlbe or TsEncoding.Camel,
            
            TsDataType.Text or TsDataType.String => 
                encoding is TsEncoding.Plain or TsEncoding.Dictionary,
            
            TsDataType.Blob or TsDataType.Object => 
                encoding is TsEncoding.Plain,
            
            _ => false
        };
    }
}
