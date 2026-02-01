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
/// Enumeration of supported compression types in TSFile format.
/// </summary>
public enum CompressionType : byte
{
    /// <summary>No compression.</summary>
    Uncompressed = 0,
    
    /// <summary>Snappy compression.</summary>
    Snappy = 1,
    
    /// <summary>GZIP compression.</summary>
    Gzip = 2,
    
    /// <summary>LZ4 compression.</summary>
    Lz4 = 7,
    
    /// <summary>ZSTD compression.</summary>
    Zstd = 8,
    
    /// <summary>LZMA2 compression.</summary>
    Lzma2 = 9
}

/// <summary>
/// Extension methods for CompressionType enum.
/// </summary>
public static class CompressionTypeExtensions
{
    /// <summary>
    /// Deserializes a CompressionType from a byte value.
    /// </summary>
    public static CompressionType Deserialize(byte value)
    {
        return value switch
        {
            0 => CompressionType.Uncompressed,
            1 => CompressionType.Snappy,
            2 => CompressionType.Gzip,
            7 => CompressionType.Lz4,
            8 => CompressionType.Zstd,
            9 => CompressionType.Lzma2,
            _ => throw new ArgumentException($"Invalid CompressionType value: {value}")
        };
    }
    
    /// <summary>
    /// Gets the file extension for this compression type.
    /// </summary>
    public static string GetExtension(this CompressionType compressionType)
    {
        return compressionType switch
        {
            CompressionType.Uncompressed => "",
            CompressionType.Snappy => ".snappy",
            CompressionType.Gzip => ".gzip",
            CompressionType.Lz4 => ".lz4",
            CompressionType.Zstd => ".zstd",
            CompressionType.Lzma2 => ".lzma2",
            _ => ""
        };
    }
}
