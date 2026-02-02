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
using IronSnappy;

namespace Apache.TsFile.Compress;

/// <summary>
/// Snappy compressor implementation using IronSnappy (pure C# implementation).
/// Cross-platform compatible without native dependencies.
/// </summary>
public class SnappyCompressor : ICompressor, IUncompressor
{
    public CompressionType Type => CompressionType.Snappy;
    
    public byte[] Compress(byte[] data)
    {
        return Snappy.Encode(data);
    }
    
    public byte[] Compress(byte[] data, int offset, int length)
    {
        if (offset == 0 && length == data.Length)
            return Snappy.Encode(data);
        
        var temp = new byte[length];
        Array.Copy(data, offset, temp, 0, length);
        return Snappy.Encode(temp);
    }
    
    public int Compress(byte[] data, int offset, int length, byte[] compressed)
    {
        var result = Compress(data, offset, length);
        Array.Copy(result, 0, compressed, 0, result.Length);
        return result.Length;
    }
    
    public int GetMaxCompressedSize(int uncompressedSize)
    {
        // Snappy worst case is input size + 32 + input size / 6
        // This is the standard formula for Snappy compression
        return uncompressedSize + 32 + (uncompressedSize / 6);
    }
    
    public byte[] Uncompress(byte[] data)
    {
        return Snappy.Decode(data);
    }
    
    public byte[] Uncompress(byte[] data, int offset, int length)
    {
        if (offset == 0 && length == data.Length)
            return Snappy.Decode(data);
        
        var temp = new byte[length];
        Array.Copy(data, offset, temp, 0, length);
        return Snappy.Decode(temp);
    }
    
    public int Uncompress(byte[] data, int offset, int length, byte[] output, int outputOffset)
    {
        var result = Uncompress(data, offset, length);
        Array.Copy(result, 0, output, outputOffset, result.Length);
        return result.Length;
    }
}
