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
/// Gorilla V1 encoder - legacy version of Gorilla encoding for floats/doubles.
/// Based on Facebook's Gorilla compression algorithm V1.
/// </summary>
public class GorillaV1Encoder : IEncoder
{
    private readonly TsDataType _dataType;
    private bool _flag = false;
    private int _leadingZeroNum;
    private int _tailingZeroNum;
    private byte _buffer;
    private int _numberLeftInBuffer;
    private readonly List<byte> _bytes = new();
    
    private long _storedValue;
    private readonly int _bitWidth;
    
    public GorillaV1Encoder(TsDataType dataType)
    {
        _dataType = dataType;
        _bitWidth = dataType switch
        {
            TsDataType.Float => 32,
            TsDataType.Double => 64,
            _ => throw new NotSupportedException($"GorillaV1 encoding does not support {dataType}")
        };
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("GorillaV1 encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        throw new NotSupportedException("GorillaV1 encoding does not support int values");
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        throw new NotSupportedException("GorillaV1 encoding does not support long values");
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        long longValue = BitConverter.SingleToInt32Bits(value);
        EncodeValue(longValue, 32);
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        long longValue = BitConverter.DoubleToInt64Bits(value);
        EncodeValue(longValue, 64);
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("GorillaV1 encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("GorillaV1 encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        ClearBuffer();
        stream.Write(_bytes.ToArray());
        Reset();
    }
    
    public int GetOneItemMaxSize()
    {
        return _bitWidth / 8 + 20;
    }
    
    public long GetMaxByteSize()
    {
        return _bytes.Count + 100;
    }
    
    private void EncodeValue(long value, int bitWidth)
    {
        if (!_flag)
        {
            // First value: write as-is
            WriteLongBits(value, bitWidth);
            _storedValue = value;
            _flag = true;
            return;
        }
        
        // XOR with previous value
        long xor = _storedValue ^ value;
        
        if (xor == 0)
        {
            // Value is same as previous: write single '0' bit
            WriteBit(false);
        }
        else
        {
            // Value changed: write '1' bit
            WriteBit(true);
            
            int leadingZeros = CountLeadingZeros(xor, bitWidth);
            int tailingZeros = CountTrailingZeros(xor, bitWidth);
            
            if (leadingZeros >= _leadingZeroNum && tailingZeros >= _tailingZeroNum)
            {
                // Use previous block: write '0' control bit
                WriteBit(false);
                int significantBits = bitWidth - _leadingZeroNum - _tailingZeroNum;
                WriteLongBits(xor >> _tailingZeroNum, significantBits);
            }
            else
            {
                // New block: write '1' control bit + leading + length + meaningful bits
                WriteBit(true);
                
                if (bitWidth == 32)
                {
                    WriteIntBits(leadingZeros, 5);
                    int significantBits = bitWidth - leadingZeros - tailingZeros;
                    WriteIntBits(significantBits, 5);
                    WriteLongBits(xor >> tailingZeros, significantBits);
                }
                else // 64-bit
                {
                    WriteIntBits(leadingZeros, 6);
                    int significantBits = bitWidth - leadingZeros - tailingZeros;
                    WriteIntBits(significantBits, 6);
                    WriteLongBits(xor >> tailingZeros, significantBits);
                }
                
                _leadingZeroNum = leadingZeros;
                _tailingZeroNum = tailingZeros;
            }
        }
        
        _storedValue = value;
    }
    
    private void WriteBit(bool bit)
    {
        _buffer <<= 1;
        if (bit)
        {
            _buffer |= 1;
        }
        
        _numberLeftInBuffer++;
        if (_numberLeftInBuffer == 8)
        {
            ClearBuffer();
        }
    }
    
    private void WriteIntBits(int value, int bits)
    {
        for (int i = bits - 1; i >= 0; i--)
        {
            WriteBit(((value >> i) & 1) != 0);
        }
    }
    
    private void WriteLongBits(long value, int bits)
    {
        for (int i = bits - 1; i >= 0; i--)
        {
            WriteBit(((value >> i) & 1) != 0);
        }
    }
    
    private void ClearBuffer()
    {
        if (_numberLeftInBuffer == 0)
        {
            return;
        }
        if (_numberLeftInBuffer > 0)
        {
            _buffer <<= (8 - _numberLeftInBuffer);
        }
        _bytes.Add(_buffer);
        _numberLeftInBuffer = 0;
        _buffer = 0;
    }
    
    private void Reset()
    {
        _flag = false;
        _numberLeftInBuffer = 0;
        _buffer = 0;
        _bytes.Clear();
    }
    
    private static int CountLeadingZeros(long value, int bitWidth)
    {
        if (value == 0) return bitWidth;
        
        int count = 0;
        long mask = 1L << (bitWidth - 1);
        
        while ((value & mask) == 0 && count < bitWidth)
        {
            count++;
            mask >>= 1;
        }
        
        return count;
    }
    
    private static int CountTrailingZeros(long value, int bitWidth)
    {
        if (value == 0) return bitWidth;
        
        int count = 0;
        while ((value & 1) == 0 && count < bitWidth)
        {
            count++;
            value >>= 1;
        }
        
        return count;
    }
}
