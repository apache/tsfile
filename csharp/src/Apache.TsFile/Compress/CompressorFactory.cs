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

namespace Apache.TsFile.Compress;

/// <summary>
/// Factory for creating compressor and uncompressor instances.
/// </summary>
public static class CompressorFactory
{
    /// <summary>
    /// Gets a compressor instance for the specified compression type.
    /// </summary>
    public static ICompressor GetCompressor(CompressionType type)
    {
        return type switch
        {
            CompressionType.Uncompressed => new UnCompressor(),
            CompressionType.Snappy => new SnappyCompressor(),
            CompressionType.Gzip => new GzipCompressor(),
            CompressionType.Lz4 => new Lz4Compressor(),
            CompressionType.Zstd => new ZstdCompressor(),
            CompressionType.Lzma2 => new Lzma2Compressor(),
            _ => throw new NotSupportedException($"Unsupported compression type: {type}")
        };
    }
    
    /// <summary>
    /// Gets an uncompressor instance for the specified compression type.
    /// </summary>
    public static IUncompressor GetUncompressor(CompressionType type)
    {
        return type switch
        {
            CompressionType.Uncompressed => new UnCompressor(),
            CompressionType.Snappy => new SnappyCompressor(),
            CompressionType.Gzip => new GzipCompressor(),
            CompressionType.Lz4 => new Lz4Compressor(),
            CompressionType.Zstd => new ZstdCompressor(),
            CompressionType.Lzma2 => new Lzma2Compressor(),
            _ => throw new NotSupportedException($"Unsupported compression type: {type}")
        };
    }
}
