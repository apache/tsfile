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
/// LZMA2 compressor implementation.
/// Note: LZMA2 is currently not implemented due to library API complexity.
/// Use GZIP, LZ4, or ZSTD compression instead.
/// </summary>
public class Lzma2Compressor : ICompressor, IUncompressor
{
    public CompressionType Type => CompressionType.Lzma2;
    
    public byte[] Compress(byte[] data)
    {
        throw new NotImplementedException("LZMA2 compression is not yet implemented. Use GZIP, LZ4, or ZSTD instead.");
    }
    
    public byte[] Compress(byte[] data, int offset, int length)
    {
        throw new NotImplementedException("LZMA2 compression is not yet implemented. Use GZIP, LZ4, or ZSTD instead.");
    }
    
    public int Compress(byte[] data, int offset, int length, byte[] compressed)
    {
        throw new NotImplementedException("LZMA2 compression is not yet implemented. Use GZIP, LZ4, or ZSTD instead.");
    }
    
    public int GetMaxCompressedSize(int uncompressedSize)
    {
        return uncompressedSize + (uncompressedSize / 3) + 128;
    }
    
    public byte[] Uncompress(byte[] data)
    {
        throw new NotImplementedException("LZMA2 decompression is not yet implemented. Use GZIP, LZ4, or ZSTD instead.");
    }
    
    public byte[] Uncompress(byte[] data, int offset, int length)
    {
        throw new NotImplementedException("LZMA2 decompression is not yet implemented. Use GZIP, LZ4, or ZSTD instead.");
    }
    
    public int Uncompress(byte[] data, int offset, int length, byte[] output, int outputOffset)
    {
        throw new NotImplementedException("LZMA2 decompression is not yet implemented. Use GZIP, LZ4, or ZSTD instead.");
    }
}
