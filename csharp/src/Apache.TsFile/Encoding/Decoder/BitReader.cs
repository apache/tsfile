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

namespace Apache.TsFile.Encoding.Decoder;

/// <summary>
/// Bit-level reader for Gorilla decoding.
/// Reads individual bits and variable-length bit sequences.
/// </summary>
internal class BitReader
{
    private readonly byte[] _buffer;
    private int _byteOffset = 0;
    private int _bitOffset = 0;
    
    public BitReader(byte[] buffer)
    {
        _buffer = buffer;
    }
    
    /// <summary>
    /// Read a single bit (returns 0 or 1).
    /// </summary>
    public int ReadBit()
    {
        if (_byteOffset >= _buffer.Length)
        {
            throw new InvalidOperationException("No more bits to read");
        }
        
        int bit = (_buffer[_byteOffset] >> (7 - _bitOffset)) & 1;
        
        _bitOffset++;
        if (_bitOffset == 8)
        {
            _byteOffset++;
            _bitOffset = 0;
        }
        
        return bit;
    }
    
    /// <summary>
    /// Read multiple bits as a long value (MSB first).
    /// </summary>
    public long ReadBits(int numBits)
    {
        long result = 0;
        for (int i = 0; i < numBits; i++)
        {
            result = (result << 1) | (uint)ReadBit();
        }
        return result;
    }
    
    /// <summary>
    /// Check if there are more bits available to read.
    /// </summary>
    public bool HasNext()
    {
        return _byteOffset < _buffer.Length;
    }
    
    /// <summary>
    /// Reset the reader to the beginning.
    /// </summary>
    public void Reset()
    {
        _byteOffset = 0;
        _bitOffset = 0;
    }
}
