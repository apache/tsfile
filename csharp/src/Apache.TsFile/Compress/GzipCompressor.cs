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

using System.IO.Compression;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Compress;

/// <summary>
/// GZIP compressor implementation.
/// </summary>
public class GzipCompressor : ICompressor, IUncompressor
{
    public CompressionType Type => CompressionType.Gzip;
    
    public byte[] Compress(byte[] data)
    {
        return Compress(data, 0, data.Length);
    }
    
    public byte[] Compress(byte[] data, int offset, int length)
    {
        using var outputStream = new MemoryStream();
        using (var gzipStream = new GZipStream(outputStream, CompressionLevel.Fastest))
        {
            gzipStream.Write(data, offset, length);
        }
        return outputStream.ToArray();
    }
    
    public int Compress(byte[] data, int offset, int length, byte[] compressed)
    {
        var result = Compress(data, offset, length);
        Array.Copy(result, 0, compressed, 0, result.Length);
        return result.Length;
    }
    
    public int GetMaxCompressedSize(int uncompressedSize)
    {
        // GZIP worst case is roughly input size + 18 bytes header
        return uncompressedSize + 32;
    }
    
    public byte[] Uncompress(byte[] data)
    {
        return Uncompress(data, 0, data.Length);
    }
    
    public byte[] Uncompress(byte[] data, int offset, int length)
    {
        using var inputStream = new MemoryStream(data, offset, length);
        using var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
        using var outputStream = new MemoryStream();
        gzipStream.CopyTo(outputStream);
        return outputStream.ToArray();
    }
    
    public int Uncompress(byte[] data, int offset, int length, byte[] output, int outputOffset)
    {
        var result = Uncompress(data, offset, length);
        Array.Copy(result, 0, output, outputOffset, result.Length);
        return result.Length;
    }
}
