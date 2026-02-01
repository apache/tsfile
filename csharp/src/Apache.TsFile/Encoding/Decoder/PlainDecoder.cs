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

namespace Apache.TsFile.Encoding.Decoder;

/// <summary>
/// Plain decoder - decodes data without decompression.
/// </summary>
public class PlainDecoder : IDecoder
{
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        return buffer[offset++] != 0;
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        var value = ReadIntBigEndian(buffer, offset);
        offset += 4;
        return value;
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        var value = ReadLongBigEndian(buffer, offset);
        offset += 8;
        return value;
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        var intBits = ReadInt(buffer, ref offset);
        return BitConverter.Int32BitsToSingle(intBits);
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        var longBits = ReadLong(buffer, ref offset);
        return BitConverter.Int64BitsToDouble(longBits);
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        var length = ReadVarInt(buffer, ref offset);
        var str = System.Text.Encoding.UTF8.GetString(buffer, offset, length);
        offset += length;
        return str;
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        var length = ReadVarInt(buffer, ref offset);
        var bytes = new byte[length];
        Array.Copy(buffer, offset, bytes, 0, length);
        offset += length;
        return bytes;
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return offset < buffer.Length;
    }
    
    public void Reset()
    {
        // Plain decoder has no state to reset
    }
    
    private static int ReadIntBigEndian(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24) 
             | (buffer[offset + 1] << 16) 
             | (buffer[offset + 2] << 8) 
             | buffer[offset + 3];
    }
    
    private static long ReadLongBigEndian(byte[] buffer, int offset)
    {
        return ((long)buffer[offset] << 56)
             | ((long)buffer[offset + 1] << 48)
             | ((long)buffer[offset + 2] << 40)
             | ((long)buffer[offset + 3] << 32)
             | ((long)buffer[offset + 4] << 24)
             | ((long)buffer[offset + 5] << 16)
             | ((long)buffer[offset + 6] << 8)
             | buffer[offset + 7];
    }
    
    private static int ReadVarInt(byte[] buffer, ref int offset)
    {
        int value = 0;
        int shift = 0;
        byte b;
        do
        {
            b = buffer[offset++];
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }
}
