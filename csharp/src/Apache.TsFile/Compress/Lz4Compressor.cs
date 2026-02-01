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
using K4os.Compression.LZ4;

namespace Apache.TsFile.Compress;

/// <summary>
/// LZ4 compressor implementation.
/// </summary>
public class Lz4Compressor : ICompressor, IUncompressor
{
    public CompressionType Type => CompressionType.Lz4;
    
    public byte[] Compress(byte[] data)
    {
        return Compress(data, 0, data.Length);
    }
    
    public byte[] Compress(byte[] data, int offset, int length)
    {
        var maxCompressedSize = LZ4Codec.MaximumOutputSize(length);
        var compressed = new byte[maxCompressedSize + 4]; // +4 for original size
        
        // Store original size in first 4 bytes
        BitConverter.GetBytes(length).CopyTo(compressed, 0);
        
        var compressedSize = LZ4Codec.Encode(
            data, offset, length,
            compressed, 4, maxCompressedSize);
        
        var result = new byte[compressedSize + 4];
        Array.Copy(compressed, 0, result, 0, compressedSize + 4);
        return result;
    }
    
    public int Compress(byte[] data, int offset, int length, byte[] compressed)
    {
        var result = Compress(data, offset, length);
        Array.Copy(result, 0, compressed, 0, result.Length);
        return result.Length;
    }
    
    public int GetMaxCompressedSize(int uncompressedSize)
    {
        return LZ4Codec.MaximumOutputSize(uncompressedSize) + 4;
    }
    
    public byte[] Uncompress(byte[] data)
    {
        return Uncompress(data, 0, data.Length);
    }
    
    public byte[] Uncompress(byte[] data, int offset, int length)
    {
        // Read original size from first 4 bytes
        var originalSize = BitConverter.ToInt32(data, offset);
        var output = new byte[originalSize];
        
        LZ4Codec.Decode(
            data, offset + 4, length - 4,
            output, 0, originalSize);
        
        return output;
    }
    
    public int Uncompress(byte[] data, int offset, int length, byte[] output, int outputOffset)
    {
        var result = Uncompress(data, offset, length);
        Array.Copy(result, 0, output, outputOffset, result.Length);
        return result.Length;
    }
}
