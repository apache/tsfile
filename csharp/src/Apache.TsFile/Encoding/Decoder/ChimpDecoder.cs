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
/// CHIMP decoder - Advanced XOR-based decompression.
/// This class includes code modified from Panagiotis Liakos chimp project.
/// Copyright: 2022- Panagiotis Liakos, Katia Papakonstantinopoulou and Yannis Kotidis
/// Project page: https://github.com/panagiotisl/chimp
/// License: http://www.apache.org/licenses/LICENSE-2.0
/// </summary>
public class ChimpDecoder : IDecoder
{
    private readonly TsDataType _dataType;
    private IChimpDecoderImpl _impl;
    
    public ChimpDecoder(TsDataType dataType)
    {
        _dataType = dataType;
        _impl = dataType switch
        {
            TsDataType.Int32 => new IntChimpDecoderImpl(),
            TsDataType.Int64 or TsDataType.Timestamp => new LongChimpDecoderImpl(),
            TsDataType.Float => new FloatChimpDecoderImpl(),
            TsDataType.Double => new DoubleChimpDecoderImpl(),
            _ => throw new NotSupportedException($"CHIMP decoding does not support {dataType}")
        };
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("CHIMP decoding does not support boolean values");
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        return _impl.ReadInt(buffer, ref offset);
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        return _impl.ReadLong(buffer, ref offset);
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        return _impl.ReadFloat(buffer, ref offset);
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        return _impl.ReadDouble(buffer, ref offset);
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("CHIMP decoding does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("CHIMP decoding does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return _impl.HasNext(buffer, offset);
    }
    
    public void Reset()
    {
        _impl.Reset();
    }
}

internal interface IChimpDecoderImpl
{
    int ReadInt(byte[] buffer, ref int offset);
    long ReadLong(byte[] buffer, ref int offset);
    float ReadFloat(byte[] buffer, ref int offset);
    double ReadDouble(byte[] buffer, ref int offset);
    bool HasNext(byte[] buffer, int offset);
    void Reset();
}

internal class IntChimpDecoderImpl : IChimpDecoderImpl
{
    private static readonly short[] LEADING_REPRESENTATION = { 0, 8, 12, 16, 18, 20, 22, 24 };
    private const int PREVIOUS_VALUES = 64;
    private const int PREVIOUS_VALUES_LOG2 = 6;
    private const int CASE_ONE_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 8;
    private const int VALUE_BITS_LENGTH_32BIT = 32;
    
    private int _storedValue = 0;
    private readonly int[] _storedValues = new int[PREVIOUS_VALUES];
    private int _current = 0;
    private bool _firstValueWasRead = false;
    private int _storedLeadingZeros = int.MaxValue;
    private int _storedTrailingZeros = 0;
    internal bool _hasNext = true;
    private int _cachedValue = 0;
    private int _endMarker = int.MinValue;
    
    private byte _buffer = 0;
    private int _bitsLeft = 0;
    private byte[]? _data;
    private int _dataOffset = 0;
    
    public IntChimpDecoderImpl() : this(int.MinValue) { }
    
    public IntChimpDecoderImpl(int endMarker)
    {
        _endMarker = endMarker;
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        if (_data == null)
        {
            _data = buffer;
            _dataOffset = offset;
        }
        
        int returnValue;
        if (!_firstValueWasRead)
        {
            FlipByte();
            _storedValue = (int)ReadLong(VALUE_BITS_LENGTH_32BIT);
            _storedValues[_current] = _storedValue;
            _firstValueWasRead = true;
            returnValue = _storedValue;
            CacheNext();
        }
        else
        {
            returnValue = _cachedValue;
            CacheNext();
        }
        
        offset = _dataOffset;
        return returnValue;
    }
    
    private void CacheNext()
    {
        if (_dataOffset >= (_data?.Length ?? 0))
        {
            _hasNext = false;
            return;
        }
        ReadNext();
        _cachedValue = _storedValues[_current];
        if (_cachedValue == _endMarker)
        {
            _hasNext = false;
        }
    }
    
    private void ReadNext()
    {
        byte controlBits = ReadNextNBits(2);
        int value;
        
        switch (controlBits)
        {
            case 3:
                _storedLeadingZeros = LEADING_REPRESENTATION[(int)ReadLong(3)];
                value = (int)ReadLong(VALUE_BITS_LENGTH_32BIT - _storedLeadingZeros);
                _storedValue = _storedValue ^ value;
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
            case 2:
                value = (int)ReadLong(VALUE_BITS_LENGTH_32BIT - _storedLeadingZeros);
                _storedValue = _storedValue ^ value;
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
            case 1:
                int fill = CASE_ONE_METADATA_LENGTH;
                int temp = (int)ReadLong(fill);
                int index = (temp >> (fill -= PREVIOUS_VALUES_LOG2)) & ((1 << PREVIOUS_VALUES_LOG2) - 1);
                _storedLeadingZeros = LEADING_REPRESENTATION[(temp >> (fill -= 3)) & ((1 << 3) - 1)];
                int significantBits = (temp >> (fill -= 5)) & ((1 << 5) - 1);
                _storedValue = _storedValues[index];
                if (significantBits == 0)
                {
                    significantBits = VALUE_BITS_LENGTH_32BIT;
                }
                _storedTrailingZeros = VALUE_BITS_LENGTH_32BIT - significantBits - _storedLeadingZeros;
                value = (int)ReadLong(VALUE_BITS_LENGTH_32BIT - _storedLeadingZeros - _storedTrailingZeros);
                value <<= _storedTrailingZeros;
                _storedValue = _storedValue ^ value;
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
            default:
                int previousIndex = (int)ReadLong(PREVIOUS_VALUES_LOG2);
                _storedValue = _storedValues[previousIndex];
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
        }
    }
    
    private byte ReadNextNBits(int n)
    {
        byte value = 0x00;
        for (int i = 0; i < n; i++)
        {
            value <<= 1;
            if (ReadBit())
            {
                value |= 0x01;
            }
        }
        return value;
    }
    
    private bool ReadBit()
    {
        bool bit = ((_buffer >> (_bitsLeft - 1)) & 1) == 1;
        _bitsLeft--;
        FlipByte();
        return bit;
    }
    
    private long ReadLong(int bits)
    {
        long value = 0;
        while (bits > 0)
        {
            if (bits > _bitsLeft || bits == 8)
            {
                byte d = (byte)(_buffer & ((1 << _bitsLeft) - 1));
                value = (value << _bitsLeft) + (d & 0xFF);
                bits -= _bitsLeft;
                _bitsLeft = 0;
            }
            else
            {
                byte d = (byte)((_buffer >> (_bitsLeft - bits)) & ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                _bitsLeft -= bits;
                bits = 0;
            }
            FlipByte();
        }
        return value;
    }
    
    private void FlipByte()
    {
        if (_bitsLeft == 0)
        {
            _buffer = _data![_dataOffset++];
            _bitsLeft = 8;
        }
    }
    
    public long ReadLong(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return _hasNext;
    }
    
    public void Reset()
    {
        _firstValueWasRead = false;
        _storedLeadingZeros = int.MaxValue;
        _storedTrailingZeros = 0;
        _hasNext = true;
        _buffer = 0;
        _bitsLeft = 0;
        _current = 0;
        _storedValue = 0;
        Array.Clear(_storedValues, 0, _storedValues.Length);
        _data = null;
        _dataOffset = 0;
    }
}

internal class LongChimpDecoderImpl : IChimpDecoderImpl
{
    private static readonly short[] LEADING_REPRESENTATION = { 0, 8, 12, 16, 18, 20, 22, 24 };
    private const int PREVIOUS_VALUES = 128;
    private const int PREVIOUS_VALUES_LOG2 = 7;
    private const int CASE_ONE_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 9;
    private const int VALUE_BITS_LENGTH_64BIT = 64;
    
    private long _storedValue = 0;
    private readonly long[] _storedValues = new long[PREVIOUS_VALUES];
    private int _current = 0;
    private bool _firstValueWasRead = false;
    private int _storedLeadingZeros = int.MaxValue;
    private int _storedTrailingZeros = 0;
    internal bool _hasNext = true;
    private long _cachedValue = 0;
    private long _endMarker = long.MinValue;
    
    private byte _buffer = 0;
    private int _bitsLeft = 0;
    private byte[]? _data;
    private int _dataOffset = 0;
    
    public LongChimpDecoderImpl() : this(long.MinValue) { }
    
    public LongChimpDecoderImpl(long endMarker)
    {
        _endMarker = endMarker;
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        if (_data == null)
        {
            _data = buffer;
            _dataOffset = offset;
        }
        
        long returnValue;
        if (!_firstValueWasRead)
        {
            FlipByte();
            _storedValue = ReadLongValue(VALUE_BITS_LENGTH_64BIT);
            _storedValues[_current] = _storedValue;
            _firstValueWasRead = true;
            returnValue = _storedValue;
            CacheNext();
        }
        else
        {
            returnValue = _cachedValue;
            CacheNext();
        }
        
        offset = _dataOffset;
        return returnValue;
    }
    
    private void CacheNext()
    {
        if (_dataOffset >= (_data?.Length ?? 0))
        {
            _hasNext = false;
            return;
        }
        ReadNext();
        _cachedValue = _storedValues[_current];
        if (_cachedValue == _endMarker)
        {
            _hasNext = false;
        }
    }
    
    private void ReadNext()
    {
        byte controlBits = ReadNextNBits(2);
        long value;
        
        switch (controlBits)
        {
            case 3:
                _storedLeadingZeros = LEADING_REPRESENTATION[(int)ReadLongValue(3)];
                value = ReadLongValue(VALUE_BITS_LENGTH_64BIT - _storedLeadingZeros);
                _storedValue = _storedValue ^ value;
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
            case 2:
                value = ReadLongValue(VALUE_BITS_LENGTH_64BIT - _storedLeadingZeros);
                _storedValue = _storedValue ^ value;
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
            case 1:
                int fill = CASE_ONE_METADATA_LENGTH;
                int temp = (int)ReadLongValue(fill);
                int index = (temp >> (fill -= PREVIOUS_VALUES_LOG2)) & ((1 << PREVIOUS_VALUES_LOG2) - 1);
                _storedLeadingZeros = LEADING_REPRESENTATION[(temp >> (fill -= 3)) & ((1 << 3) - 1)];
                int significantBits = (temp >> (fill -= 6)) & ((1 << 6) - 1);
                _storedValue = _storedValues[index];
                if (significantBits == 0)
                {
                    significantBits = VALUE_BITS_LENGTH_64BIT;
                }
                _storedTrailingZeros = VALUE_BITS_LENGTH_64BIT - significantBits - _storedLeadingZeros;
                value = ReadLongValue(VALUE_BITS_LENGTH_64BIT - _storedLeadingZeros - _storedTrailingZeros);
                value <<= _storedTrailingZeros;
                _storedValue = _storedValue ^ value;
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
            default:
                int previousIndex = (int)ReadLongValue(PREVIOUS_VALUES_LOG2);
                _storedValue = _storedValues[previousIndex];
                _current = (_current + 1) % PREVIOUS_VALUES;
                _storedValues[_current] = _storedValue;
                break;
        }
    }
    
    private byte ReadNextNBits(int n)
    {
        byte value = 0x00;
        for (int i = 0; i < n; i++)
        {
            value <<= 1;
            if (ReadBit())
            {
                value |= 0x01;
            }
        }
        return value;
    }
    
    private bool ReadBit()
    {
        bool bit = ((_buffer >> (_bitsLeft - 1)) & 1) == 1;
        _bitsLeft--;
        FlipByte();
        return bit;
    }
    
    private long ReadLongValue(int bits)
    {
        long value = 0;
        while (bits > 0)
        {
            if (bits > _bitsLeft || bits == 8)
            {
                byte d = (byte)(_buffer & ((1 << _bitsLeft) - 1));
                value = (value << _bitsLeft) + (d & 0xFF);
                bits -= _bitsLeft;
                _bitsLeft = 0;
            }
            else
            {
                byte d = (byte)((_buffer >> (_bitsLeft - bits)) & ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                _bitsLeft -= bits;
                bits = 0;
            }
            FlipByte();
        }
        return value;
    }
    
    private void FlipByte()
    {
        if (_bitsLeft == 0)
        {
            _buffer = _data![_dataOffset++];
            _bitsLeft = 8;
        }
    }
    
    public int ReadInt(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return _hasNext;
    }
    
    public void Reset()
    {
        _firstValueWasRead = false;
        _storedLeadingZeros = int.MaxValue;
        _storedTrailingZeros = 0;
        _hasNext = true;
        _buffer = 0;
        _bitsLeft = 0;
        _current = 0;
        _storedValue = 0;
        Array.Clear(_storedValues, 0, _storedValues.Length);
        _data = null;
        _dataOffset = 0;
    }
}

internal class FloatChimpDecoderImpl : IChimpDecoderImpl
{
    private readonly IntChimpDecoderImpl _impl;
    
    public FloatChimpDecoderImpl()
    {
        _impl = new IntChimpDecoderImpl(BitConverter.SingleToInt32Bits(float.NaN));
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        int bits = _impl.ReadInt(buffer, ref offset);
        return BitConverter.Int32BitsToSingle(bits);
    }
    
    public int ReadInt(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public long ReadLong(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public bool HasNext(byte[] buffer, int offset) => _impl.HasNext(buffer, offset);
    public void Reset() => _impl.Reset();
}

internal class DoubleChimpDecoderImpl : IChimpDecoderImpl
{
    private readonly LongChimpDecoderImpl _impl;
    
    public DoubleChimpDecoderImpl()
    {
        _impl = new LongChimpDecoderImpl(BitConverter.DoubleToInt64Bits(double.NaN));
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        long bits = _impl.ReadLong(buffer, ref offset);
        return BitConverter.Int64BitsToDouble(bits);
    }
    
    public int ReadInt(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public long ReadLong(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public bool HasNext(byte[] buffer, int offset) => _impl.HasNext(buffer, offset);
    public void Reset() => _impl.Reset();
}
