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

using System.Text;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// Plain encoder - encodes data without compression.
/// </summary>
public class PlainEncoder : IEncoder
{
    private const int MaxStringLength = 128;
    
    public void Encode(bool value, MemoryStream stream)
    {
        stream.WriteByte((byte)(value ? 1 : 0));
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        WriteInt(stream, value);
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        WriteLong(stream, value);
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        WriteInt(stream, BitConverter.SingleToInt32Bits(value));
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        WriteLong(stream, BitConverter.DoubleToInt64Bits(value));
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteVarInt(stream, bytes.Length);
        stream.Write(bytes, 0, bytes.Length);
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        WriteVarInt(stream, value.Length);
        stream.Write(value, 0, value.Length);
    }
    
    public void Flush(MemoryStream stream)
    {
        // Plain encoder doesn't buffer, so nothing to flush
    }
    
    public int GetOneItemMaxSize()
    {
        return 4 + MaxStringLength * 4; // Max for string/binary
    }
    
    public long GetMaxByteSize()
    {
        return 0; // No internal buffering
    }
    
    private static void WriteInt(Stream stream, int value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, 4);
    }
    
    private static void WriteLong(Stream stream, long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, 8);
    }
    
    private static void WriteVarInt(Stream stream, int value)
    {
        // Variable length integer encoding
        while ((value & ~0x7F) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)value);
    }
}
