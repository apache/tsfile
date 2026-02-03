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

using System.Numerics;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// CHIMP encoder - Advanced XOR-based compression with flag encoding.
/// Based on the CHIMP compression algorithm.
/// This class includes code modified from Panagiotis Liakos chimp project.
/// Copyright: 2022- Panagiotis Liakos, Katia Papakonstantinopoulou and Yannis Kotidis
/// Project page: https://github.com/panagiotisl/chimp
/// License: http://www.apache.org/licenses/LICENSE-2.0
/// </summary>
public class ChimpEncoder : IEncoder
{
    private readonly TsDataType _dataType;
    private IChimpEncoderImpl _impl;
    
    public ChimpEncoder(TsDataType dataType)
    {
        _dataType = dataType;
        _impl = dataType switch
        {
            TsDataType.Int32 => new IntChimpEncoderImpl(),
            TsDataType.Int64 or TsDataType.Timestamp => new LongChimpEncoderImpl(),
            TsDataType.Float => new FloatChimpEncoderImpl(),
            TsDataType.Double => new DoubleChimpEncoderImpl(),
            _ => throw new NotSupportedException($"CHIMP encoding does not support {dataType}")
        };
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("CHIMP encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        _impl.EncodeInt(value);
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        _impl.EncodeLong(value);
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        _impl.EncodeFloat(value);
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        _impl.EncodeDouble(value);
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("CHIMP encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("CHIMP encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        _impl.Flush(stream);
    }
    
    public int GetOneItemMaxSize()
    {
        return _impl.GetOneItemMaxSize();
    }
    
    public long GetMaxByteSize()
    {
        return _impl.GetMaxByteSize();
    }
}

internal interface IChimpEncoderImpl
{
    void EncodeInt(int value);
    void EncodeLong(long value);
    void EncodeFloat(float value);
    void EncodeDouble(double value);
    void Flush(MemoryStream stream);
    int GetOneItemMaxSize();
    long GetMaxByteSize();
}

internal class IntChimpEncoderImpl : IChimpEncoderImpl
{
    private const int PREVIOUS_VALUES = 64;
    private const int PREVIOUS_VALUES_LOG2 = 6;
    private const int THRESHOLD = 5 + PREVIOUS_VALUES_LOG2;
    private const int SET_LSB = (1 << (THRESHOLD + 1)) - 1;
    private const int CASE_ZERO_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 2;
    private const int CASE_ONE_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 10;
    private const int VALUE_BITS_LENGTH_32BIT = 32;
    
    private static readonly short[] LEADING_REPRESENTATION = {
        0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
    };
    
    private static readonly short[] LEADING_ROUND = {
        0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 12, 12, 12, 12, 16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24
    };
    
    private readonly int[] _storedValues = new int[PREVIOUS_VALUES];
    private readonly int[] _indices = new int[1 << (THRESHOLD + 1)];
    private int _index = 0;
    private int _current = 0;
    private int _storedLeadingZeros = VALUE_BITS_LENGTH_32BIT + 1;
    private bool _firstValueWasWritten = false;
    
    private byte _buffer = 0;
    internal int _bitsLeft = 8;
    internal readonly MemoryStream _byteStream = new();
    
    public void EncodeInt(int value)
    {
        if (_firstValueWasWritten)
        {
            CompressValue(value);
        }
        else
        {
            WriteFirst(value);
            _firstValueWasWritten = true;
        }
    }
    
    public void EncodeLong(long value) => throw new NotSupportedException();
    public void EncodeFloat(float value) => throw new NotSupportedException();
    public void EncodeDouble(double value) => throw new NotSupportedException();
    
    private void WriteFirst(int value)
    {
        _storedValues[_current] = value;
        WriteBits((uint)value, VALUE_BITS_LENGTH_32BIT);
        _indices[value & SET_LSB] = _index;
    }
    
    private void CompressValue(int value)
    {
        int key = value & SET_LSB;
        int xor;
        int previousIndex;
        int trailingZeros = 0;
        int currIndex = _indices[key];
        
        if ((_index - currIndex) < PREVIOUS_VALUES)
        {
            int tempXor = value ^ _storedValues[currIndex % PREVIOUS_VALUES];
            trailingZeros = BitOperations.TrailingZeroCount((uint)tempXor);
            if (trailingZeros > THRESHOLD)
            {
                previousIndex = currIndex % PREVIOUS_VALUES;
                xor = tempXor;
            }
            else
            {
                previousIndex = _index % PREVIOUS_VALUES;
                xor = _storedValues[previousIndex] ^ value;
            }
        }
        else
        {
            previousIndex = _index % PREVIOUS_VALUES;
            xor = _storedValues[previousIndex] ^ value;
        }
        
        if (xor == 0)
        {
            WriteBits((uint)previousIndex, CASE_ZERO_METADATA_LENGTH);
            _storedLeadingZeros = VALUE_BITS_LENGTH_32BIT + 1;
        }
        else
        {
            int leadingZeros = LEADING_ROUND[BitOperations.LeadingZeroCount((uint)xor)];
            
            if (trailingZeros > THRESHOLD)
            {
                int significantBits = VALUE_BITS_LENGTH_32BIT - leadingZeros - trailingZeros;
                WriteBits(
                    (uint)(256L * (PREVIOUS_VALUES + previousIndex) + 32L * LEADING_REPRESENTATION[leadingZeros] + significantBits),
                    CASE_ONE_METADATA_LENGTH);
                WriteBits((uint)(xor >> trailingZeros), significantBits);
                _storedLeadingZeros = VALUE_BITS_LENGTH_32BIT + 1;
            }
            else if (leadingZeros == _storedLeadingZeros)
            {
                WriteBit();
                SkipBit();
                int significantBits = VALUE_BITS_LENGTH_32BIT - leadingZeros;
                WriteBits((uint)xor, significantBits);
            }
            else
            {
                _storedLeadingZeros = leadingZeros;
                int significantBits = VALUE_BITS_LENGTH_32BIT - leadingZeros;
                WriteBits((uint)(24L + LEADING_REPRESENTATION[leadingZeros]), 5);
                WriteBits((uint)xor, significantBits);
            }
        }
        
        _current = (_current + 1) % PREVIOUS_VALUES;
        _storedValues[_current] = value;
        _index++;
        _indices[key] = _index;
    }
    
    private void WriteBit()
    {
        _buffer |= (byte)(1 << (_bitsLeft - 1));
        _bitsLeft--;
        FlipByte();
    }
    
    private void SkipBit()
    {
        _bitsLeft--;
        FlipByte();
    }
    
    private void WriteBits(uint value, int bits)
    {
        while (bits > 0)
        {
            int shift = bits - _bitsLeft;
            if (shift >= 0)
            {
                _buffer |= (byte)((value >> shift) & ((1 << _bitsLeft) - 1));
                bits -= _bitsLeft;
                _bitsLeft = 0;
            }
            else
            {
                shift = _bitsLeft - bits;
                _buffer |= (byte)(value << shift);
                _bitsLeft -= bits;
                bits = 0;
            }
            FlipByte();
        }
    }
    
    internal void FlipByte()
    {
        if (_bitsLeft == 0)
        {
            _byteStream.WriteByte(_buffer);
            _buffer = 0;
            _bitsLeft = 8;
        }
    }
    
    public void Flush(MemoryStream stream)
    {
        EncodeInt(int.MinValue);
        _bitsLeft = 0;
        FlipByte();
        
        var bytes = _byteStream.ToArray();
        stream.Write(bytes, 0, bytes.Length);
        
        Reset();
    }
    
    internal void Reset()
    {
        _firstValueWasWritten = false;
        _storedLeadingZeros = VALUE_BITS_LENGTH_32BIT + 1;
        _buffer = 0;
        _bitsLeft = 8;
        _current = 0;
        _index = 0;
        Array.Clear(_indices, 0, _indices.Length);
        Array.Clear(_storedValues, 0, _storedValues.Length);
        _byteStream.SetLength(0);
    }
    
    public int GetOneItemMaxSize() => (2 + 5 + 6 + VALUE_BITS_LENGTH_32BIT) / 8 + 1;
    public long GetMaxByteSize() => _byteStream.Length + 10;
}

internal class LongChimpEncoderImpl : IChimpEncoderImpl
{
    private const int PREVIOUS_VALUES = 128;
    private const int PREVIOUS_VALUES_LOG2 = 7;
    private const int THRESHOLD = 6 + PREVIOUS_VALUES_LOG2;
    private const int SET_LSB = (1 << (THRESHOLD + 1)) - 1;
    private const int CASE_ZERO_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 2;
    private const int CASE_ONE_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 11;
    private const int VALUE_BITS_LENGTH_64BIT = 64;
    
    private static readonly short[] LEADING_REPRESENTATION = {
        0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
    };
    
    private static readonly short[] LEADING_ROUND = {
        0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 12, 12, 12, 12, 16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
        24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24
    };
    
    private readonly long[] _storedValues = new long[PREVIOUS_VALUES];
    private readonly int[] _indices = new int[1 << (THRESHOLD + 1)];
    private int _index = 0;
    private int _current = 0;
    private int _storedLeadingZeros = VALUE_BITS_LENGTH_64BIT + 1;
    private bool _firstValueWasWritten = false;
    
    private byte _buffer = 0;
    internal int _bitsLeft = 8;
    internal readonly MemoryStream _byteStream = new();
    
    public void EncodeLong(long value)
    {
        if (_firstValueWasWritten)
        {
            CompressValue(value);
        }
        else
        {
            WriteFirst(value);
            _firstValueWasWritten = true;
        }
    }
    
    public void EncodeInt(int value) => throw new NotSupportedException();
    public void EncodeFloat(float value) => throw new NotSupportedException();
    public void EncodeDouble(double value) => throw new NotSupportedException();
    
    private void WriteFirst(long value)
    {
        _storedValues[_current] = value;
        WriteBits((ulong)value, VALUE_BITS_LENGTH_64BIT);
        _indices[(int)value & SET_LSB] = _index;
    }
    
    private void CompressValue(long value)
    {
        int key = (int)value & SET_LSB;
        long xor;
        int previousIndex;
        int trailingZeros = 0;
        int currIndex = _indices[key];
        
        if ((_index - currIndex) < PREVIOUS_VALUES)
        {
            long tempXor = value ^ _storedValues[currIndex % PREVIOUS_VALUES];
            trailingZeros = BitOperations.TrailingZeroCount((ulong)tempXor);
            if (trailingZeros > THRESHOLD)
            {
                previousIndex = currIndex % PREVIOUS_VALUES;
                xor = tempXor;
            }
            else
            {
                previousIndex = _index % PREVIOUS_VALUES;
                xor = _storedValues[previousIndex] ^ value;
            }
        }
        else
        {
            previousIndex = _index % PREVIOUS_VALUES;
            xor = _storedValues[previousIndex] ^ value;
        }
        
        if (xor == 0)
        {
            WriteBits((ulong)previousIndex, CASE_ZERO_METADATA_LENGTH);
            _storedLeadingZeros = VALUE_BITS_LENGTH_64BIT + 1;
        }
        else
        {
            int leadingZeros = LEADING_ROUND[BitOperations.LeadingZeroCount((ulong)xor)];
            
            if (trailingZeros > THRESHOLD)
            {
                int significantBits = VALUE_BITS_LENGTH_64BIT - leadingZeros - trailingZeros;
                WriteBits(
                    (ulong)(512L * (PREVIOUS_VALUES + previousIndex) + 64L * LEADING_REPRESENTATION[leadingZeros] + significantBits),
                    CASE_ONE_METADATA_LENGTH);
                WriteBits((ulong)(xor >> trailingZeros), significantBits);
                _storedLeadingZeros = VALUE_BITS_LENGTH_64BIT + 1;
            }
            else if (leadingZeros == _storedLeadingZeros)
            {
                WriteBit();
                SkipBit();
                int significantBits = VALUE_BITS_LENGTH_64BIT - leadingZeros;
                WriteBits((ulong)xor, significantBits);
            }
            else
            {
                _storedLeadingZeros = leadingZeros;
                int significantBits = VALUE_BITS_LENGTH_64BIT - leadingZeros;
                WriteBits((ulong)(24L + LEADING_REPRESENTATION[leadingZeros]), 5);
                WriteBits((ulong)xor, significantBits);
            }
        }
        
        _current = (_current + 1) % PREVIOUS_VALUES;
        _storedValues[_current] = value;
        _index++;
        _indices[key] = _index;
    }
    
    private void WriteBit()
    {
        _buffer |= (byte)(1 << (_bitsLeft - 1));
        _bitsLeft--;
        FlipByte();
    }
    
    private void SkipBit()
    {
        _bitsLeft--;
        FlipByte();
    }
    
    private void WriteBits(ulong value, int bits)
    {
        while (bits > 0)
        {
            int shift = bits - _bitsLeft;
            if (shift >= 0)
            {
                _buffer |= (byte)((value >> shift) & ((1u << _bitsLeft) - 1));
                bits -= _bitsLeft;
                _bitsLeft = 0;
            }
            else
            {
                shift = _bitsLeft - bits;
                _buffer |= (byte)(value << shift);
                _bitsLeft -= bits;
                bits = 0;
            }
            FlipByte();
        }
    }
    
    internal void FlipByte()
    {
        if (_bitsLeft == 0)
        {
            _byteStream.WriteByte(_buffer);
            _buffer = 0;
            _bitsLeft = 8;
        }
    }
    
    public void Flush(MemoryStream stream)
    {
        EncodeLong(long.MinValue);
        _bitsLeft = 0;
        FlipByte();
        
        var bytes = _byteStream.ToArray();
        stream.Write(bytes, 0, bytes.Length);
        
        Reset();
    }
    
    internal void Reset()
    {
        _firstValueWasWritten = false;
        _storedLeadingZeros = VALUE_BITS_LENGTH_64BIT + 1;
        _buffer = 0;
        _bitsLeft = 8;
        _current = 0;
        _index = 0;
        Array.Clear(_indices, 0, _indices.Length);
        Array.Clear(_storedValues, 0, _storedValues.Length);
        _byteStream.SetLength(0);
    }
    
    public int GetOneItemMaxSize() => (2 + 6 + 7 + VALUE_BITS_LENGTH_64BIT) / 8 + 1;
    public long GetMaxByteSize() => _byteStream.Length + 10;
}

internal class FloatChimpEncoderImpl : IChimpEncoderImpl
{
    private readonly IntChimpEncoderImpl _impl = new();
    
    public void EncodeFloat(float value)
    {
        int bits = BitConverter.SingleToInt32Bits(value);
        _impl.EncodeInt(bits);
    }
    
    public void Flush(MemoryStream stream)
    {
        int endMarker = BitConverter.SingleToInt32Bits(float.NaN);
        _impl.EncodeInt(endMarker);
        _impl._bitsLeft = 0;
        _impl.FlipByte();
        
        var bytes = _impl._byteStream.ToArray();
        stream.Write(bytes, 0, bytes.Length);
        
        _impl.Reset();
    }
    
    public void EncodeInt(int value) => throw new NotSupportedException();
    public void EncodeLong(long value) => throw new NotSupportedException();
    public void EncodeDouble(double value) => throw new NotSupportedException();
    public int GetOneItemMaxSize() => _impl.GetOneItemMaxSize();
    public long GetMaxByteSize() => _impl.GetMaxByteSize();
}

internal class DoubleChimpEncoderImpl : IChimpEncoderImpl
{
    private readonly LongChimpEncoderImpl _impl = new();
    
    public void EncodeDouble(double value)
    {
        long bits = BitConverter.DoubleToInt64Bits(value);
        _impl.EncodeLong(bits);
    }
    
    public void Flush(MemoryStream stream)
    {
        long endMarker = BitConverter.DoubleToInt64Bits(double.NaN);
        _impl.EncodeLong(endMarker);
        _impl._bitsLeft = 0;
        _impl.FlipByte();
        
        var bytes = _impl._byteStream.ToArray();
        stream.Write(bytes, 0, bytes.Length);
        
        _impl.Reset();
    }
    
    public void EncodeInt(int value) => throw new NotSupportedException();
    public void EncodeLong(long value) => throw new NotSupportedException();
    public void EncodeFloat(float value) => throw new NotSupportedException();
    public int GetOneItemMaxSize() => _impl.GetOneItemMaxSize();
    public long GetMaxByteSize() => _impl.GetMaxByteSize();
}
