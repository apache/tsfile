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

namespace Apache.TsFile.Encoding;

/// <summary>
/// Interface for data encoders.
/// </summary>
public interface IEncoder
{
    /// <summary>
    /// Encodes a boolean value.
    /// </summary>
    void Encode(bool value, MemoryStream stream);
    
    /// <summary>
    /// Encodes an integer value.
    /// </summary>
    void Encode(int value, MemoryStream stream);
    
    /// <summary>
    /// Encodes a long value.
    /// </summary>
    void Encode(long value, MemoryStream stream);
    
    /// <summary>
    /// Encodes a float value.
    /// </summary>
    void Encode(float value, MemoryStream stream);
    
    /// <summary>
    /// Encodes a double value.
    /// </summary>
    void Encode(double value, MemoryStream stream);
    
    /// <summary>
    /// Encodes a string value.
    /// </summary>
    void Encode(string value, MemoryStream stream);
    
    /// <summary>
    /// Encodes a byte array value.
    /// </summary>
    void Encode(byte[] value, MemoryStream stream);
    
    /// <summary>
    /// Flushes any buffered data to the stream.
    /// </summary>
    void Flush(MemoryStream stream);
    
    /// <summary>
    /// Gets the maximum size for one encoded item.
    /// </summary>
    int GetOneItemMaxSize();
    
    /// <summary>
    /// Gets the maximum byte size used by this encoder.
    /// </summary>
    long GetMaxByteSize();
}
