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
/// Interface for data compressors in TSFile format.
/// </summary>
public interface ICompressor
{
    /// <summary>
    /// Gets the compression type.
    /// </summary>
    CompressionType Type { get; }
    
    /// <summary>
    /// Compresses the input data.
    /// </summary>
    byte[] Compress(byte[] data);
    
    /// <summary>
    /// Compresses a portion of the input data.
    /// </summary>
    byte[] Compress(byte[] data, int offset, int length);
    
    /// <summary>
    /// Compresses data into a pre-allocated buffer.
    /// </summary>
    /// <returns>The number of bytes written to the compressed buffer.</returns>
    int Compress(byte[] data, int offset, int length, byte[] compressed);
    
    /// <summary>
    /// Gets the maximum size needed for compression output.
    /// </summary>
    int GetMaxCompressedSize(int uncompressedSize);
}

/// <summary>
/// Interface for data decompressors in TSFile format.
/// </summary>
public interface IUncompressor
{
    /// <summary>
    /// Gets the compression type.
    /// </summary>
    CompressionType Type { get; }
    
    /// <summary>
    /// Decompresses the input data.
    /// </summary>
    byte[] Uncompress(byte[] data);
    
    /// <summary>
    /// Decompresses a portion of the input data.
    /// </summary>
    byte[] Uncompress(byte[] data, int offset, int length);
    
    /// <summary>
    /// Decompresses data into a pre-allocated buffer.
    /// </summary>
    /// <returns>The number of bytes written to the output buffer.</returns>
    int Uncompress(byte[] data, int offset, int length, byte[] output, int outputOffset);
}
