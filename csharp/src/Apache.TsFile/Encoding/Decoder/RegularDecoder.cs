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
using System.Collections;

namespace Apache.TsFile.Encoding.Decoder;

/// <summary>
/// Regular decoder - decodes regular pattern data with missing points.
/// </summary>
public class RegularDecoder : IDecoder
{
    private readonly TsDataType _dataType;
    private int _readTotalCount;
    private int _nextReadIndex;
    private int _packNum;
    
    private int[]? _intData;
    private int _intFirstValue;
    private int _intPrevious;
    private int _intMinDeltaBase;
    
    private long[]? _longData;
    private long _longFirstValue;
    private long _longPrevious;
    private long _longMinDeltaBase;
    
    private bool _isMissingPoint;
    private BitArray? _bitmap;
    private int _bitmapIndex;
    
    private byte[] _sourceBuffer = Array.Empty<byte>();
    
    public RegularDecoder(TsDataType dataType)
    {
        _dataType = dataType;
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Regular decoding does not support boolean values");
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        if (_dataType != TsDataType.Int32)
            throw new NotSupportedException($"Regular decoder configured for {_dataType}, not Int32");
            
        if (_nextReadIndex == _readTotalCount)
        {
            _sourceBuffer = buffer;
            _isMissingPoint = buffer[offset++] != 0;
            if (_isMissingPoint)
            {
                ReadBitmap(ref offset);
            }
            return LoadIntBatch(ref offset);
        }
        
        if (_isMissingPoint)
        {
            _bitmapIndex++;
            return LoadIntWithBitmap();
        }
        
        return _intData![_nextReadIndex++];
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        if (_dataType != TsDataType.Int64 && _dataType != TsDataType.Timestamp)
            throw new NotSupportedException($"Regular decoder configured for {_dataType}, not Int64");
            
        if (_nextReadIndex == _readTotalCount)
        {
            _sourceBuffer = buffer;
            _isMissingPoint = buffer[offset++] != 0;
            if (_isMissingPoint)
            {
                ReadBitmap(ref offset);
            }
            return LoadLongBatch(ref offset);
        }
        
        if (_isMissingPoint)
        {
            _bitmapIndex++;
            return LoadLongWithBitmap();
        }
        
        return _longData![_nextReadIndex++];
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Regular decoding does not support float values");
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Regular decoding does not support double values");
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Regular decoding does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Regular decoding does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return (_nextReadIndex < _readTotalCount) || offset < buffer.Length;
    }
    
    public void Reset()
    {
        _readTotalCount = 0;
        _nextReadIndex = 0;
        _packNum = 0;
    }
    
    private void ReadBitmap(ref int offset)
    {
        int length = ReadIntBigEndian(_sourceBuffer, offset);
        offset += 4;
        byte[] byteArr = new byte[length];
        Array.Copy(_sourceBuffer, offset, byteArr, 0, length);
        offset += length;
        _bitmap = new BitArray(byteArr);
        _bitmapIndex = 0;
    }
    
    private int LoadIntBatch(ref int offset)
    {
        _packNum = ReadIntBigEndian(_sourceBuffer, offset);
        offset += 4;
        _intMinDeltaBase = ReadIntBigEndian(_sourceBuffer, offset);
        offset += 4;
        _intFirstValue = ReadIntBigEndian(_sourceBuffer, offset);
        offset += 4;
        
        _intData = new int[_packNum - 1];
        _readTotalCount = _isMissingPoint ? (_packNum - 2) : (_packNum - 1);
        _intPrevious = _intFirstValue;
        _nextReadIndex = 0;
        
        for (int i = 0; i < _intData.Length; i++)
        {
            _intData[i] = _intPrevious + _intMinDeltaBase;
            _intPrevious = _intData[i];
        }
        
        return _intFirstValue;
    }
    
    private int LoadIntWithBitmap()
    {
        while (_bitmap != null && !_bitmap[_bitmapIndex])
        {
            _bitmapIndex++;
        }
        _nextReadIndex = _bitmapIndex - 1;
        return _intData![_nextReadIndex];
    }
    
    private long LoadLongBatch(ref int offset)
    {
        _packNum = ReadIntBigEndian(_sourceBuffer, offset);
        offset += 4;
        _longMinDeltaBase = ReadLongBigEndian(_sourceBuffer, offset);
        offset += 8;
        _longFirstValue = ReadLongBigEndian(_sourceBuffer, offset);
        offset += 8;
        
        _longData = new long[_packNum - 1];
        _readTotalCount = _isMissingPoint ? (_packNum - 2) : (_packNum - 1);
        _longPrevious = _longFirstValue;
        _nextReadIndex = 0;
        
        for (int i = 0; i < _longData.Length; i++)
        {
            _longData[i] = _longPrevious + _longMinDeltaBase;
            _longPrevious = _longData[i];
        }
        
        return _longFirstValue;
    }
    
    private long LoadLongWithBitmap()
    {
        while (_bitmap != null && !_bitmap[_bitmapIndex])
        {
            _bitmapIndex++;
        }
        _nextReadIndex = _bitmapIndex - 1;
        return _longData![_nextReadIndex];
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
}
