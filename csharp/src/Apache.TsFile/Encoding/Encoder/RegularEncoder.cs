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
using System.Collections;

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// Regular encoder - optimized for regular pattern data with missing points.
/// </summary>
public class RegularEncoder : IEncoder
{
    private const int BlockDefaultSize = 128;
    private readonly TsDataType _dataType;
    private readonly int _blockSize;
    private int _writeIndex = -1;
    
    private int[]? _intData;
    private long[]? _longData;
    
    public RegularEncoder(TsDataType dataType) : this(dataType, BlockDefaultSize)
    {
    }
    
    public RegularEncoder(TsDataType dataType, int blockSize)
    {
        _dataType = dataType;
        _blockSize = blockSize;
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("Regular encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        if (_dataType != TsDataType.Int32)
            throw new NotSupportedException($"Regular encoder configured for {_dataType}, not Int32");
            
        if (_writeIndex == -1)
        {
            _intData = new int[_blockSize];
            _writeIndex = 0;
        }
        
        _intData![_writeIndex++] = value;
        if (_writeIndex == _blockSize)
        {
            FlushIntBlock(stream);
        }
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        if (_dataType != TsDataType.Int64 && _dataType != TsDataType.Timestamp)
            throw new NotSupportedException($"Regular encoder configured for {_dataType}, not Int64");
            
        if (_writeIndex == -1)
        {
            _longData = new long[_blockSize];
            _writeIndex = 0;
        }
        
        _longData![_writeIndex++] = value;
        if (_writeIndex == _blockSize)
        {
            FlushLongBlock(stream);
        }
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        throw new NotSupportedException("Regular encoding does not support float values");
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        throw new NotSupportedException("Regular encoding does not support double values");
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("Regular encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("Regular encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_writeIndex == -1) return;
        
        if (_dataType == TsDataType.Int32)
        {
            FlushIntBlock(stream);
        }
        else
        {
            FlushLongBlock(stream);
        }
    }
    
    public int GetOneItemMaxSize()
    {
        return _dataType == TsDataType.Int32 ? 4 : 8;
    }
    
    public long GetMaxByteSize()
    {
        int headerSize = _dataType == TsDataType.Int32 ? 20 : 28;
        int itemSize = _dataType == TsDataType.Int32 ? 4 : 8;
        return headerSize + (_writeIndex * 2 / 8) + (_writeIndex * itemSize);
    }
    
    private void FlushIntBlock(MemoryStream stream)
    {
        if (_writeIndex <= 0 || _intData == null) return;
        
        bool isMissingPoint = false;
        int minDeltaBase = int.MaxValue;
        int firstValue = _intData[0];
        int previousValue = firstValue;
        
        if (_writeIndex > 1)
        {
            minDeltaBase = _intData[1] - _intData[0];
            for (int i = 1; i < _writeIndex; i++)
            {
                int delta = _intData[i] - previousValue;
                if (delta != minDeltaBase)
                {
                    isMissingPoint = true;
                }
                if (delta < minDeltaBase)
                {
                    minDeltaBase = delta;
                }
                previousValue = _intData[i];
            }
        }
        
        int newBlockSize = _writeIndex;
        BitArray? bitmap = null;
        
        if (isMissingPoint && minDeltaBase > 0)
        {
            newBlockSize = ((_intData[_writeIndex - 1] - _intData[0]) / minDeltaBase) + 1;
            bitmap = CreateIntBitmap(_intData, _writeIndex, minDeltaBase, newBlockSize);
        }
        
        stream.WriteByte((byte)(isMissingPoint ? 1 : 0));
        
        if (isMissingPoint && bitmap != null)
        {
            byte[] bitmapBytes = new byte[(bitmap.Length + 7) / 8];
            bitmap.CopyTo(bitmapBytes, 0);
            WriteInt(stream, bitmapBytes.Length);
            stream.Write(bitmapBytes);
        }
        
        WriteInt(stream, newBlockSize);
        WriteInt(stream, minDeltaBase);
        WriteInt(stream, firstValue);
        
        _writeIndex = -1;
        _intData = null;
    }
    
    private void FlushLongBlock(MemoryStream stream)
    {
        if (_writeIndex <= 0 || _longData == null) return;
        
        bool isMissingPoint = false;
        long minDeltaBase = long.MaxValue;
        long firstValue = _longData[0];
        long previousValue = firstValue;
        
        if (_writeIndex > 1)
        {
            minDeltaBase = _longData[1] - _longData[0];
            for (int i = 1; i < _writeIndex; i++)
            {
                long delta = _longData[i] - previousValue;
                if (delta != minDeltaBase)
                {
                    isMissingPoint = true;
                }
                if (delta < minDeltaBase)
                {
                    minDeltaBase = delta;
                }
                previousValue = _longData[i];
            }
        }
        
        int newBlockSize = _writeIndex;
        BitArray? bitmap = null;
        
        if (isMissingPoint && minDeltaBase > 0)
        {
            newBlockSize = (int)((_longData[_writeIndex - 1] - _longData[0]) / minDeltaBase) + 1;
            bitmap = CreateLongBitmap(_longData, _writeIndex, minDeltaBase, newBlockSize);
        }
        
        stream.WriteByte((byte)(isMissingPoint ? 1 : 0));
        
        if (isMissingPoint && bitmap != null)
        {
            byte[] bitmapBytes = new byte[(bitmap.Length + 7) / 8];
            bitmap.CopyTo(bitmapBytes, 0);
            WriteInt(stream, bitmapBytes.Length);
            stream.Write(bitmapBytes);
        }
        
        WriteInt(stream, newBlockSize);
        WriteLong(stream, minDeltaBase);
        WriteLong(stream, firstValue);
        
        _writeIndex = -1;
        _longData = null;
    }
    
    private static BitArray CreateIntBitmap(int[] data, int dataTotal, int minDeltaBase, int newBlockSize)
    {
        var bitmap = new BitArray(newBlockSize, true);
        int offset = 0;
        
        for (int i = 1; i < dataTotal; i++)
        {
            int delta = data[i] - data[i - 1];
            if (delta != minDeltaBase)
            {
                int missingPointNum = (delta / minDeltaBase) - 1;
                for (int j = 0; j < missingPointNum; j++)
                {
                    bitmap[i + offset++] = false;
                }
            }
        }
        
        return bitmap;
    }
    
    private static BitArray CreateLongBitmap(long[] data, int dataTotal, long minDeltaBase, int newBlockSize)
    {
        var bitmap = new BitArray(newBlockSize, true);
        int offset = 0;
        
        for (int i = 1; i < dataTotal; i++)
        {
            long delta = data[i] - data[i - 1];
            if (delta != minDeltaBase)
            {
                int missingPointNum = (int)(delta / minDeltaBase) - 1;
                for (int j = 0; j < missingPointNum; j++)
                {
                    bitmap[i + offset++] = false;
                }
            }
        }
        
        return bitmap;
    }
    
    private static void WriteInt(MemoryStream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        stream.Write(bytes);
    }
    
    private static void WriteLong(MemoryStream stream, long value)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        stream.Write(bytes);
    }
}
