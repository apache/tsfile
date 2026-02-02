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

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// Bit-level writer for Gorilla encoding.
/// Writes individual bits and variable-length bit sequences.
/// </summary>
internal class BitWriter
{
    private readonly List<byte> _buffer = new();
    private byte _currentByte = 0;
    private int _bitOffset = 0;
    
    /// <summary>
    /// Write a single bit (0 or 1).
    /// </summary>
    public void WriteBit(int bit)
    {
        if (bit != 0)
        {
            _currentByte |= (byte)(1 << (7 - _bitOffset));
        }
        
        _bitOffset++;
        if (_bitOffset == 8)
        {
            _buffer.Add(_currentByte);
            _currentByte = 0;
            _bitOffset = 0;
        }
    }
    
    /// <summary>
    /// Write multiple bits from a value (MSB first).
    /// </summary>
    public void WriteBits(long value, int numBits)
    {
        for (int i = numBits - 1; i >= 0; i--)
        {
            WriteBit((int)((value >> i) & 1));
        }
    }
    
    /// <summary>
    /// Flush any remaining bits to the buffer (padding with zeros).
    /// </summary>
    public void Flush()
    {
        if (_bitOffset > 0)
        {
            _buffer.Add(_currentByte);
            _currentByte = 0;
            _bitOffset = 0;
        }
    }
    
    /// <summary>
    /// Get the written bytes.
    /// </summary>
    public byte[] ToArray()
    {
        Flush();
        return _buffer.ToArray();
    }
    
    /// <summary>
    /// Get the current number of bits written (including unflushed bits).
    /// </summary>
    public int GetBitCount()
    {
        return _buffer.Count * 8 + _bitOffset;
    }
}
