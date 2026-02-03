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
using SharpCompress.Compressors.Xz;

namespace Apache.TsFile.Compress;

/// <summary>
/// LZMA2 compressor implementation.
/// Note: Only decompression is supported. SharpCompress provides read-only XZ/LZMA2 support.
/// For compression, use ZSTD (recommended), LZ4 (fast), or GZIP (widely compatible) instead.
/// </summary>
public class Lzma2Compressor : ICompressor, IUncompressor
{
    public CompressionType Type => CompressionType.Lzma2;
    
    public byte[] Compress(byte[] data)
    {
        throw new NotSupportedException(
            "LZMA2 compression is not supported - only decompression is available. " +
            "SharpCompress library provides read-only XZ/LZMA2 support. " +
            "For writing data, use ZSTD (recommended), LZ4 (fast), or GZIP (widely compatible) instead.");
    }
    
    public byte[] Compress(byte[] data, int offset, int length)
    {
        throw new NotSupportedException(
            "LZMA2 compression is not supported - only decompression is available. " +
            "SharpCompress library provides read-only XZ/LZMA2 support. " +
            "For writing data, use ZSTD (recommended), LZ4 (fast), or GZIP (widely compatible) instead.");
    }
    
    public int Compress(byte[] data, int offset, int length, byte[] compressed)
    {
        throw new NotSupportedException(
            "LZMA2 compression is not supported - only decompression is available. " +
            "SharpCompress library provides read-only XZ/LZMA2 support. " +
            "For writing data, use ZSTD (recommended), LZ4 (fast), or GZIP (widely compatible) instead.");
    }
    
    public int GetMaxCompressedSize(int uncompressedSize)
    {
        // Conservative estimate for XZ/LZMA2 format
        return 100 + uncompressedSize;
    }
    
    public byte[] Uncompress(byte[] data)
    {
        return Uncompress(data, 0, data.Length);
    }
    
    public byte[] Uncompress(byte[] data, int offset, int length)
    {
        try
        {
            using var inputStream = new MemoryStream(data, offset, length);
            using var outputStream = new MemoryStream();
            using (var xzStream = new XZStream(inputStream))
            {
                xzStream.CopyTo(outputStream);
            }
            return outputStream.ToArray();
        }
        catch (Exception ex)
        {
            throw new InvalidDataException(
                $"Failed to decompress LZMA2 data. Data may be corrupted or not in XZ format. Error: {ex.Message}", 
                ex);
        }
    }
    
    public int Uncompress(byte[] data, int offset, int length, byte[] output, int outputOffset)
    {
        var result = Uncompress(data, offset, length);
        Array.Copy(result, 0, output, outputOffset, result.Length);
        return result.Length;
    }
}
