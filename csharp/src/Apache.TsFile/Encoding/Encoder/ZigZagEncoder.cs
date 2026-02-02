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

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// ZigZag encoder - converts signed integers to unsigned, then uses variable-length encoding.
/// Efficient for small absolute values.
/// Maps: -1→1, -2→3, 1→2, 2→4
/// </summary>
public class ZigZagEncoder : IEncoder
{
    private readonly List<int> _intValues = new();
    private readonly List<long> _longValues = new();
    private readonly TsDataType _dataType;
    
    public ZigZagEncoder(TsDataType dataType)
    {
        _dataType = dataType;
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("ZigZag encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        _intValues.Add(value);
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        _longValues.Add(value);
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        throw new NotSupportedException("ZigZag encoding does not support float values");
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        throw new NotSupportedException("ZigZag encoding does not support double values");
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("ZigZag encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("ZigZag encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_intValues.Count > 0)
        {
            EncodeIntegers(stream);
        }
        else if (_longValues.Count > 0)
        {
            EncodeLongs(stream);
        }
    }
    
    public int GetOneItemMaxSize()
    {
        return _dataType switch
        {
            TsDataType.Int32 => 9, // Max 5 bytes for count + 5 bytes for zigzag int
            TsDataType.Int64 or TsDataType.Timestamp => 14, // Max 5 bytes for count + 10 bytes for zigzag long
            _ => 9
        };
    }
    
    public long GetMaxByteSize()
    {
        return _intValues.Count * 9L + _longValues.Count * 14L;
    }
    
    private void EncodeIntegers(MemoryStream stream)
    {
        WriteVarInt(stream, _intValues.Count);
        foreach (var value in _intValues)
        {
            uint encoded = EncodeZigZag32(value);
            WriteVarUInt(stream, encoded);
        }
        _intValues.Clear();
    }
    
    private void EncodeLongs(MemoryStream stream)
    {
        WriteVarInt(stream, _longValues.Count);
        foreach (var value in _longValues)
        {
            ulong encoded = EncodeZigZag64(value);
            WriteVarULong(stream, encoded);
        }
        _longValues.Clear();
    }
    
    /// <summary>
    /// Encode signed int32 to unsigned using ZigZag encoding.
    /// Formula: (n << 1) ^ (n >> 31)
    /// </summary>
    private static uint EncodeZigZag32(int n)
    {
        return (uint)((n << 1) ^ (n >> 31));
    }
    
    /// <summary>
    /// Encode signed int64 to unsigned using ZigZag encoding.
    /// Formula: (n << 1) ^ (n >> 63)
    /// </summary>
    private static ulong EncodeZigZag64(long n)
    {
        return (ulong)((n << 1) ^ (n >> 63));
    }
    
    private static void WriteVarInt(Stream stream, int value)
    {
        WriteVarUInt(stream, (uint)value);
    }
    
    private static void WriteVarUInt(Stream stream, uint value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }
    
    private static void WriteVarULong(Stream stream, ulong value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }
}
