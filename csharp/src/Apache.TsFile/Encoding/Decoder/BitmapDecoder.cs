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

namespace Apache.TsFile.Encoding.Decoder;

/// <summary>
/// Bitmap decoder - decodes sparse integer values using bitmaps.
/// </summary>
public class BitmapDecoder : IDecoder
{
    private int _length;
    private int _number;
    private int _currentCount;
    private Dictionary<int, byte[]> _buffer = new();
    private byte[] _sourceBuffer = Array.Empty<byte>();
    private int _sourceOffset;
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Bitmap decoding does not support boolean values");
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        if (_currentCount == 0)
        {
            Reset();
            _sourceBuffer = buffer;
            _sourceOffset = offset;
            GetLengthAndNumber(ref offset);
            ReadNext(ref offset);
        }
        
        int result = 0;
        int index = (_number - _currentCount) / 8;
        int bitOffset = 7 - ((_number - _currentCount) % 8);
        
        foreach (var entry in _buffer)
        {
            byte[] tmp = entry.Value;
            if (((tmp[index] & 0xFF) & ((byte)1 << bitOffset)) != 0)
            {
                result = entry.Key;
            }
        }
        
        _currentCount--;
        return result;
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Bitmap decoding does not support long values");
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Bitmap decoding does not support float values");
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Bitmap decoding does not support double values");
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Bitmap decoding does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Bitmap decoding does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return _currentCount > 0 || offset < buffer.Length;
    }
    
    public void Reset()
    {
        _length = 0;
        _number = 0;
        _currentCount = 0;
        _buffer.Clear();
    }
    
    private void GetLengthAndNumber(ref int offset)
    {
        _length = ReadVarInt(_sourceBuffer, ref offset);
        _number = ReadVarInt(_sourceBuffer, ref offset);
    }
    
    private void ReadNext(ref int offset)
    {
        int len = (_number + 7) / 8;
        int dataLength = _length;
        int dataOffset = offset;
        
        while (dataOffset - offset < dataLength)
        {
            int value = ReadVarInt(_sourceBuffer, ref dataOffset);
            byte[] bitmapData = new byte[len];
            Array.Copy(_sourceBuffer, dataOffset, bitmapData, 0, len);
            dataOffset += len;
            _buffer[value] = bitmapData;
        }
        
        offset = dataOffset;
        _currentCount = _number;
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
