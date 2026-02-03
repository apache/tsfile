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
using System.Buffers.Binary;

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// Bitmap encoder - encodes sparse integer values using bitmaps.
/// </summary>
public class BitmapEncoder : IEncoder
{
    private readonly List<int> _values = new();
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("Bitmap encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        _values.Add(value);
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        throw new NotSupportedException("Bitmap encoding does not support long values");
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        throw new NotSupportedException("Bitmap encoding does not support float values");
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        throw new NotSupportedException("Bitmap encoding does not support double values");
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("Bitmap encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("Bitmap encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_values.Count == 0)
        {
            Reset();
            return;
        }
        
        var byteCache = new MemoryStream();
        var uniqueValues = new HashSet<int>(_values);
        int byteNum = (_values.Count + 7) / 8;
        int len = _values.Count;
        
        foreach (int value in uniqueValues)
        {
            byte[] buffer = new byte[byteNum];
            for (int i = 0; i < len; i++)
            {
                if (_values[i] == value)
                {
                    int index = i / 8;
                    int offset = 7 - (i % 8);
                    buffer[index] |= (byte)(1 << offset);
                }
            }
            WriteUnsignedVarInt(value, byteCache);
            byteCache.Write(buffer, 0, buffer.Length);
        }
        
        WriteUnsignedVarInt((int)byteCache.Length, stream);
        WriteUnsignedVarInt(len, stream);
        byteCache.WriteTo(stream);
        Reset();
    }
    
    public int GetOneItemMaxSize()
    {
        return 1;
    }
    
    public long GetMaxByteSize()
    {
        return 4 + 4 + ((long)(_values.Count + 7) / 8 + 4) * _values.Count;
    }
    
    private void Reset()
    {
        _values.Clear();
    }
    
    private static void WriteUnsignedVarInt(int value, MemoryStream stream)
    {
        while ((value & 0xFFFFFF80) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)(value & 0x7F));
    }
}
