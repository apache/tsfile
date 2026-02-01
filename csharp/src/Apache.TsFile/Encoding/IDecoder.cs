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
/// Interface for data decoders.
/// </summary>
public interface IDecoder
{
    /// <summary>
    /// Reads a boolean value from the buffer.
    /// </summary>
    bool ReadBoolean(byte[] buffer, ref int offset);
    
    /// <summary>
    /// Reads an integer value from the buffer.
    /// </summary>
    int ReadInt(byte[] buffer, ref int offset);
    
    /// <summary>
    /// Reads a long value from the buffer.
    /// </summary>
    long ReadLong(byte[] buffer, ref int offset);
    
    /// <summary>
    /// Reads a float value from the buffer.
    /// </summary>
    float ReadFloat(byte[] buffer, ref int offset);
    
    /// <summary>
    /// Reads a double value from the buffer.
    /// </summary>
    double ReadDouble(byte[] buffer, ref int offset);
    
    /// <summary>
    /// Reads a string value from the buffer.
    /// </summary>
    string ReadString(byte[] buffer, ref int offset);
    
    /// <summary>
    /// Reads a byte array value from the buffer.
    /// </summary>
    byte[] ReadBytes(byte[] buffer, ref int offset);
    
    /// <summary>
    /// Checks if there is more data to read.
    /// </summary>
    bool HasNext(byte[] buffer, int offset);
    
    /// <summary>
    /// Resets the decoder state.
    /// </summary>
    void Reset();
}
