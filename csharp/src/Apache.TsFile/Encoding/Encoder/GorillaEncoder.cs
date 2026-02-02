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
/// Gorilla encoder - XOR-based compression for time-series floating-point data.
/// Lossless compression optimized for values that change slowly.
/// Based on Facebook's Gorilla compression algorithm.
/// </summary>
public class GorillaEncoder : IEncoder
{
    private readonly BitWriter _bitWriter = new();
    private long _previousValue;
    private int _previousLeadingZeros;
    private int _previousTrailingZeros;
    private bool _first = true;
    private readonly TsDataType _dataType;
    private readonly int _bitWidth;
    
    public GorillaEncoder(TsDataType dataType)
    {
        _dataType = dataType;
        _bitWidth = dataType switch
        {
            TsDataType.Float => 32,
            TsDataType.Double => 64,
            TsDataType.Int32 => 32,
            TsDataType.Int64 or TsDataType.Timestamp => 64,
            _ => 32
        };
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("Gorilla encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        EncodeValue(value, 32);
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        EncodeValue(value, 64);
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
        throw new NotSupportedException("Gorilla encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("Gorilla encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        var bytes = _bitWriter.ToArray();
        
        // Write length first
        WriteInt(stream, bytes.Length);
        
        // Write bit width
        stream.WriteByte((byte)_bitWidth);
        
        // Write encoded data
        stream.Write(bytes, 0, bytes.Length);
    }
    
    public int GetOneItemMaxSize()
    {
        return _bitWidth / 8 + 20; // Max size includes control bits
    }
    
    public long GetMaxByteSize()
    {
        return _bitWriter.GetBitCount() / 8 + 100;
    }
    
    private void EncodeValue(long value, int bitWidth)
    {
        if (_first)
        {
            // First value: write as-is
            _bitWriter.WriteBits(value, bitWidth);
            _previousValue = value;
            _first = false;
            return;
        }
        
        // XOR with previous value
        long xor = value ^ _previousValue;
        
        if (xor == 0)
        {
            // Value is same as previous: write single '0' bit
            _bitWriter.WriteBit(0);
        }
        else
        {
            // Value changed: write '1' bit
            _bitWriter.WriteBit(1);
            
            // Count leading and trailing zeros in XOR
            int leadingZeros = CountLeadingZeros(xor, bitWidth);
            int trailingZeros = CountTrailingZeros(xor, bitWidth);
            
            // Check if we can use previous block info
            if (leadingZeros >= _previousLeadingZeros && 
                trailingZeros >= _previousTrailingZeros &&
                _previousLeadingZeros > 0)
            {
                // Use previous block: write '0' + meaningful bits
                _bitWriter.WriteBit(0);
                int meaningfulBits = bitWidth - _previousLeadingZeros - _previousTrailingZeros;
                _bitWriter.WriteBits(xor >> _previousTrailingZeros, meaningfulBits);
            }
            else
            {
                // New block: write '1' + leading zeros + length + meaningful bits
                _bitWriter.WriteBit(1);
                _bitWriter.WriteBits(leadingZeros, 5); // 5 bits for leading zeros (0-32 or 0-64)
                
                int meaningfulBits = bitWidth - leadingZeros - trailingZeros;
                _bitWriter.WriteBits(meaningfulBits, 6); // 6 bits for length (0-64)
                _bitWriter.WriteBits(xor >> trailingZeros, meaningfulBits);
                
                _previousLeadingZeros = leadingZeros;
                _previousTrailingZeros = trailingZeros;
            }
        }
        
        _previousValue = value;
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
    
    private static void WriteInt(Stream stream, int value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, 4);
    }
}
