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
/// RLE (Run-Length Encoding) decoder using hybrid RLE + bit-packing approach.
/// </summary>
public class RleDecoder : IDecoder
{
    private const int BitPackedGroupSize = 8;
    
    private byte[]? _buffer;
    private int _bufferOffset;
    private int _bitWidth;
    private readonly Queue<int> _intQueue = new();
    private readonly Queue<long> _longQueue = new();
    private readonly TsDataType _dataType;
    private bool _isLong;
    
    public RleDecoder(TsDataType dataType)
    {
        _dataType = dataType;
        _isLong = dataType == TsDataType.Int64 || dataType == TsDataType.Double || dataType == TsDataType.Timestamp;
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        return ReadInt(buffer, ref offset) != 0;
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        EnsureData(buffer, ref offset);
        return _intQueue.Dequeue();
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        EnsureDataLong(buffer, ref offset);
        return _longQueue.Dequeue();
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        return BitConverter.Int32BitsToSingle(ReadInt(buffer, ref offset));
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        return BitConverter.Int64BitsToDouble(ReadLong(buffer, ref offset));
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("RLE decoder does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("RLE decoder does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        if (_isLong)
            return _longQueue.Count > 0 || offset < buffer.Length;
        return _intQueue.Count > 0 || offset < buffer.Length;
    }
    
    public void Reset()
    {
        _buffer = null;
        _bufferOffset = 0;
        _bitWidth = 0;
        _intQueue.Clear();
        _longQueue.Clear();
    }
    
    private void EnsureData(byte[] buffer, ref int offset)
    {
        if (_intQueue.Count > 0) return;
        
        if (_buffer == null || _bufferOffset >= _buffer.Length)
        {
            // Read next chunk
            int length = ReadInt32(buffer, ref offset);
            _bitWidth = buffer[offset++];
            _buffer = new byte[length - 1];
            Array.Copy(buffer, offset, _buffer, 0, _buffer.Length);
            offset += _buffer.Length;
            _bufferOffset = 0;
        }
        
        DecodeNextRun();
    }
    
    private void EnsureDataLong(byte[] buffer, ref int offset)
    {
        if (_longQueue.Count > 0) return;
        
        if (_buffer == null || _bufferOffset >= _buffer.Length)
        {
            int length = ReadInt32(buffer, ref offset);
            _bitWidth = buffer[offset++];
            _buffer = new byte[length - 1];
            Array.Copy(buffer, offset, _buffer, 0, _buffer.Length);
            offset += _buffer.Length;
            _bufferOffset = 0;
        }
        
        DecodeNextRunLong();
    }
    
    private void DecodeNextRun()
    {
        if (_buffer == null || _bufferOffset >= _buffer.Length) return;
        
        int header = ReadVarInt(_buffer, ref _bufferOffset);
        bool isRle = (header & 1) == 0;
        
        if (isRle)
        {
            // RLE run
            int count = header >> 1;
            int value = ReadPaddedInt(_buffer, ref _bufferOffset, _bitWidth);
            for (int i = 0; i < count; i++)
            {
                _intQueue.Enqueue(value);
            }
        }
        else
        {
            // Bit-packed run
            int groupCount = header >> 1;
            int lastNum = _buffer[_bufferOffset++];
            
            for (int g = 0; g < groupCount; g++)
            {
                int count = (g == groupCount - 1) ? lastNum : BitPackedGroupSize;
                UnpackInts(_buffer, ref _bufferOffset, _bitWidth, count);
            }
        }
    }
    
    private void DecodeNextRunLong()
    {
        if (_buffer == null || _bufferOffset >= _buffer.Length) return;
        
        int header = ReadVarInt(_buffer, ref _bufferOffset);
        bool isRle = (header & 1) == 0;
        
        if (isRle)
        {
            int count = header >> 1;
            long value = ReadPaddedLong(_buffer, ref _bufferOffset, _bitWidth);
            for (int i = 0; i < count; i++)
            {
                _longQueue.Enqueue(value);
            }
        }
        else
        {
            int groupCount = header >> 1;
            int lastNum = _buffer[_bufferOffset++];
            
            for (int g = 0; g < groupCount; g++)
            {
                int count = (g == groupCount - 1) ? lastNum : BitPackedGroupSize;
                UnpackLongs(_buffer, ref _bufferOffset, _bitWidth, count);
            }
        }
    }
    
    private void UnpackInts(byte[] buffer, ref int offset, int bitWidth, int count)
    {
        int byteWidth = (bitWidth + 7) / 8;
        
        for (int i = 0; i < count; i++)
        {
            var valueBytes = new byte[4];
            Array.Copy(buffer, offset, valueBytes, 4 - byteWidth, byteWidth);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(valueBytes);
            
            int value = BitConverter.ToInt32(valueBytes, 0);
            _intQueue.Enqueue(value);
            offset += byteWidth;
        }
    }
    
    private void UnpackLongs(byte[] buffer, ref int offset, int bitWidth, int count)
    {
        int byteWidth = (bitWidth + 7) / 8;
        
        for (int i = 0; i < count; i++)
        {
            var valueBytes = new byte[8];
            Array.Copy(buffer, offset, valueBytes, 8 - byteWidth, byteWidth);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(valueBytes);
            
            long value = BitConverter.ToInt64(valueBytes, 0);
            _longQueue.Enqueue(value);
            offset += byteWidth;
        }
    }
    
    private static int ReadInt32(byte[] buffer, ref int offset)
    {
        var bytes = new byte[4];
        Array.Copy(buffer, offset, bytes, 0, 4);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        offset += 4;
        return BitConverter.ToInt32(bytes, 0);
    }
    
    private static int ReadVarInt(byte[] buffer, ref int offset)
    {
        int result = 0;
        int shift = 0;
        
        while (true)
        {
            byte b = buffer[offset++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        
        return result;
    }
    
    private static int ReadPaddedInt(byte[] buffer, ref int offset, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = new byte[4];
        Array.Copy(buffer, offset, bytes, 4 - byteWidth, byteWidth);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        offset += byteWidth;
        return BitConverter.ToInt32(bytes, 0);
    }
    
    private static long ReadPaddedLong(byte[] buffer, ref int offset, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = new byte[8];
        Array.Copy(buffer, offset, bytes, 8 - byteWidth, byteWidth);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        offset += byteWidth;
        return BitConverter.ToInt64(bytes, 0);
    }
}
