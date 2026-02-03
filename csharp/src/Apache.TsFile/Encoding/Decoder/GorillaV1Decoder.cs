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
/// Decoder for Gorilla V1 encoded data (floats and doubles).
/// </summary>
public class GorillaV1Decoder : IDecoder
{
    private readonly TsDataType _dataType;
    private bool _flag = false;
    private int _leadingZeroNum;
    private int _tailingZeroNum;
    private bool _isEnd;
    private int _bitBuffer = -1;
    private int _numberLeftInBuffer;
    private long _storedValue;
    private readonly int _bitWidth;
    private byte[] _sourceBuffer = Array.Empty<byte>();
    private int _sourceOffset;
    
    public GorillaV1Decoder(TsDataType dataType)
    {
        _dataType = dataType;
        _bitWidth = dataType switch
        {
            TsDataType.Float => 32,
            TsDataType.Double => 64,
            _ => throw new NotSupportedException($"GorillaV1 decoding does not support {dataType}")
        };
        Reset();
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("GorillaV1 decoding does not support boolean values");
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("GorillaV1 decoding does not support int values");
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("GorillaV1 decoding does not support long values");
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        _sourceBuffer = buffer;
        _sourceOffset = offset;
        long longValue = DecodeValue(32);
        offset = _sourceOffset;
        return BitConverter.Int32BitsToSingle((int)longValue);
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        _sourceBuffer = buffer;
        _sourceOffset = offset;
        long longValue = DecodeValue(64);
        offset = _sourceOffset;
        return BitConverter.Int64BitsToDouble(longValue);
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("GorillaV1 decoding does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("GorillaV1 decoding does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return offset < buffer.Length || !_isEnd;
    }
    
    public void Reset()
    {
        _flag = false;
        _isEnd = false;
        _numberLeftInBuffer = 0;
        _bitBuffer = -1;
    }
    
    private long DecodeValue(int bitWidth)
    {
        if (!_flag)
        {
            _storedValue = ReadLongFromBuffer(bitWidth);
            _flag = true;
            return _storedValue;
        }
        
        bool bit = ReadBit();
        if (!bit)
        {
            return _storedValue;
        }
        
        bool controlBit = ReadBit();
        long xor;
        
        if (!controlBit)
        {
            int significantBits = bitWidth - _leadingZeroNum - _tailingZeroNum;
            xor = ReadLongFromBuffer(significantBits);
            xor <<= _tailingZeroNum;
        }
        else
        {
            if (bitWidth == 32)
            {
                _leadingZeroNum = ReadIntFromBuffer(5);
                int significantBits = ReadIntFromBuffer(5);
                xor = ReadLongFromBuffer(significantBits);
                _tailingZeroNum = bitWidth - _leadingZeroNum - significantBits;
                xor <<= _tailingZeroNum;
            }
            else
            {
                _leadingZeroNum = ReadIntFromBuffer(6);
                int significantBits = ReadIntFromBuffer(6);
                xor = ReadLongFromBuffer(significantBits);
                _tailingZeroNum = bitWidth - _leadingZeroNum - significantBits;
                xor <<= _tailingZeroNum;
            }
        }
        
        long value = _storedValue ^ xor;
        _storedValue = value;
        return value;
    }
    
    private bool ReadBit()
    {
        if (_numberLeftInBuffer == 0 && !_isEnd)
        {
            FillBuffer();
        }
        if (_bitBuffer == -1)
        {
            throw new InvalidOperationException("Reading from empty buffer");
        }
        _numberLeftInBuffer--;
        return ((_bitBuffer >> _numberLeftInBuffer) & 1) == 1;
    }
    
    private void FillBuffer()
    {
        if (_sourceOffset < _sourceBuffer.Length)
        {
            _bitBuffer = _sourceBuffer[_sourceOffset++];
            _numberLeftInBuffer = 8;
        }
        else
        {
            _bitBuffer = -1;
            _numberLeftInBuffer = -1;
            _isEnd = true;
        }
    }
    
    private int ReadIntFromBuffer(int len)
    {
        int num = 0;
        for (int i = 0; i < len; i++)
        {
            int bit = ReadBit() ? 1 : 0;
            num |= bit << (len - 1 - i);
        }
        return num;
    }
    
    private long ReadLongFromBuffer(int len)
    {
        long num = 0;
        for (int i = 0; i < len; i++)
        {
            long bit = ReadBit() ? 1 : 0;
            num |= bit << (len - 1 - i);
        }
        return num;
    }
}
