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

using Apache.TsFile.Encoding.BitPacking;
using Apache.TsFile.Encoding.Fire;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding.Decoder;

public class SprintzDecoder : IDecoder
{
    private readonly TsDataType _dataType;
    private ISprintzDecoderImpl _impl;
    
    public SprintzDecoder(TsDataType dataType)
    {
        _dataType = dataType;
        _impl = dataType switch
        {
            TsDataType.Int32 => new IntSprintzDecoderImpl(),
            TsDataType.Int64 or TsDataType.Timestamp => new LongSprintzDecoderImpl(),
            TsDataType.Float => new FloatSprintzDecoderImpl(),
            TsDataType.Double => new DoubleSprintzDecoderImpl(),
            _ => throw new NotSupportedException($"SPRINTZ decoding does not support {dataType}")
        };
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("SPRINTZ decoding does not support boolean values");
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
        throw new NotSupportedException("SPRINTZ decoding does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("SPRINTZ decoding does not support byte array values");
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

internal interface ISprintzDecoderImpl
{
    int ReadInt(byte[] buffer, ref int offset);
    long ReadLong(byte[] buffer, ref int offset);
    float ReadFloat(byte[] buffer, ref int offset);
    double ReadDouble(byte[] buffer, ref int offset);
    bool HasNext(byte[] buffer, int offset);
    void Reset();
}

internal class IntSprintzDecoderImpl : ISprintzDecoderImpl
{
    private const int BlockSize = 8;
    private const string PredictScheme = "fire";
    
    private readonly int[] _currentBuffer = new int[BlockSize + 1];
    private readonly IntFire _firePred = new(2);
    private bool _isBlockReaded = false;
    private int _currentCount = 0;
    private int _decodeSize = 0;
    private int _bitWidth = 0;
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        if (!_isBlockReaded)
        {
            DecodeBlock(buffer, ref offset);
        }
        
        int currentValue = _currentBuffer[_currentCount++];
        if (_currentCount == _decodeSize)
        {
            _isBlockReaded = false;
            _currentCount = 0;
        }
        return currentValue;
    }
    
    public long ReadLong(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        int minLength = 4 + 1;
        return (_isBlockReaded && _currentCount < BlockSize) || (buffer.Length - offset >= minLength);
    }
    
    public void Reset()
    {
        _isBlockReaded = false;
        _currentCount = 0;
        _decodeSize = 0;
        Array.Fill(_currentBuffer, 0);
    }
    
    private void DecodeBlock(byte[] buffer, ref int offset)
    {
        _bitWidth = ReadIntLittleEndianPaddedOnBitWidth(buffer, ref offset, 1);
        if ((_bitWidth & (1 << 7)) != 0)
        {
            _decodeSize = _bitWidth & ~(1 << 7);
            var decoder = new RleDecoder(TsDataType.Int32);
            for (int i = 0; i < _decodeSize; i++)
            {
                _currentBuffer[i] = decoder.ReadInt(buffer, ref offset);
            }
        }
        else
        {
            _decodeSize = BlockSize + 1;
            int preValue = ReadUnsignedVarInt(buffer, ref offset);
            _currentBuffer[0] = preValue;
            var packer = new IntPacker(_bitWidth);
            var packcle = new byte[_bitWidth];
            Array.Copy(buffer, offset, packcle, 0, _bitWidth);
            offset += _bitWidth;
            var tmpBuffer = new int[8];
            packer.Unpack8Values(packcle, 0, tmpBuffer);
            for (int i = 0; i < 8; i++)
            {
                _currentBuffer[i + 1] = tmpBuffer[i];
            }
            Recalculate();
        }
        _isBlockReaded = true;
    }
    
    private void Recalculate()
    {
        for (int i = 1; i <= BlockSize; i++)
        {
            if (_currentBuffer[i] % 2 == 0)
            {
                _currentBuffer[i] = -_currentBuffer[i] / 2;
            }
            else
            {
                _currentBuffer[i] = (_currentBuffer[i] + 1) / 2;
            }
        }
        
        if (PredictScheme == "delta")
        {
            for (int i = 1; i < _currentBuffer.Length; i++)
            {
                _currentBuffer[i] += _currentBuffer[i - 1];
            }
        }
        else if (PredictScheme == "fire")
        {
            _firePred.Reset();
            for (int i = 1; i <= BlockSize; i++)
            {
                int pred = _firePred.Predict(_currentBuffer[i - 1]);
                int err = _currentBuffer[i];
                _currentBuffer[i] = pred + err;
                _firePred.Train(_currentBuffer[i - 1], _currentBuffer[i], err);
            }
        }
    }
    
    private static int ReadIntLittleEndianPaddedOnBitWidth(byte[] buffer, ref int offset, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = new byte[4];
        Array.Copy(buffer, offset, bytes, 0, byteWidth);
        offset += byteWidth;
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt32(bytes, 0);
    }
    
    private static int ReadUnsignedVarInt(byte[] buffer, ref int offset)
    {
        int value = 0;
        int i = 0;
        int b;
        do
        {
            b = buffer[offset++];
            value |= (b & 0x7F) << i;
            i += 7;
        } while ((b & 0x80) != 0);
        return value;
    }
}

internal class LongSprintzDecoderImpl : ISprintzDecoderImpl
{
    private const int BlockSize = 8;
    private const string PredictScheme = "fire";
    
    private readonly long[] _currentBuffer = new long[BlockSize + 1];
    private readonly LongFire _firePred = new(3);
    private bool _isBlockReaded = false;
    private int _currentCount = 0;
    private int _decodeSize = 0;
    private int _bitWidth = 0;
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        if (!_isBlockReaded)
        {
            DecodeBlock(buffer, ref offset);
        }
        
        long currentValue = _currentBuffer[_currentCount++];
        if (_currentCount == _decodeSize)
        {
            _isBlockReaded = false;
            _currentCount = 0;
        }
        return currentValue;
    }
    
    public int ReadInt(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return (_isBlockReaded && _currentCount < BlockSize) || (buffer.Length > offset);
    }
    
    public void Reset()
    {
        _isBlockReaded = false;
        _currentCount = 0;
        _decodeSize = 0;
        Array.Fill(_currentBuffer, 0L);
    }
    
    private void DecodeBlock(byte[] buffer, ref int offset)
    {
        _bitWidth = ReadIntLittleEndianPaddedOnBitWidth(buffer, ref offset, 1);
        if ((_bitWidth & (1 << 7)) != 0)
        {
            _decodeSize = _bitWidth & ~(1 << 7);
            var decoder = new RleDecoder(TsDataType.Int64);
            for (int i = 0; i < _decodeSize; i++)
            {
                _currentBuffer[i] = decoder.ReadLong(buffer, ref offset);
            }
        }
        else
        {
            _decodeSize = BlockSize + 1;
            long preValue = BitConverter.ToInt64(buffer, offset);
            offset += 8;
            _currentBuffer[0] = preValue;
            var packer = new LongPacker(_bitWidth);
            var packcle = new byte[_bitWidth];
            Array.Copy(buffer, offset, packcle, 0, _bitWidth);
            offset += _bitWidth;
            var tmpBuffer = new long[8];
            packer.Unpack8Values(packcle, 0, tmpBuffer);
            for (int i = 0; i < 8; i++)
            {
                _currentBuffer[i + 1] = tmpBuffer[i];
            }
            Recalculate();
        }
        _isBlockReaded = true;
    }
    
    private void Recalculate()
    {
        for (int i = 1; i <= BlockSize; i++)
        {
            if (_currentBuffer[i] % 2 == 0)
            {
                _currentBuffer[i] = -_currentBuffer[i] / 2;
            }
            else
            {
                _currentBuffer[i] = (_currentBuffer[i] + 1) / 2;
            }
        }
        
        if (PredictScheme == "delta")
        {
            for (int i = 1; i < _currentBuffer.Length; i++)
            {
                _currentBuffer[i] += _currentBuffer[i - 1];
            }
        }
        else if (PredictScheme == "fire")
        {
            _firePred.Reset();
            for (int i = 1; i <= BlockSize; i++)
            {
                long pred = _firePred.Predict(_currentBuffer[i - 1]);
                long err = _currentBuffer[i];
                _currentBuffer[i] = pred + err;
                _firePred.Train(_currentBuffer[i - 1], _currentBuffer[i], err);
            }
        }
    }
    
    private static int ReadIntLittleEndianPaddedOnBitWidth(byte[] buffer, ref int offset, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = new byte[4];
        Array.Copy(buffer, offset, bytes, 0, byteWidth);
        offset += byteWidth;
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt32(bytes, 0);
    }
}

internal class FloatSprintzDecoderImpl : ISprintzDecoderImpl
{
    private const int BlockSize = 8;
    private const string PredictScheme = "fire";
    
    private readonly float[] _currentBuffer = new float[BlockSize + 1];
    private readonly int[] _convertBuffer = new int[BlockSize];
    private readonly IntFire _firePred = new(2);
    private bool _isBlockReaded = false;
    private int _currentCount = 0;
    private int _decodeSize = 0;
    private int _bitWidth = 0;
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        if (!_isBlockReaded)
        {
            DecodeBlock(buffer, ref offset);
        }
        
        float currentValue = _currentBuffer[_currentCount++];
        if (_currentCount == _decodeSize)
        {
            _isBlockReaded = false;
            _currentCount = 0;
        }
        return currentValue;
    }
    
    public int ReadInt(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public long ReadLong(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        int minLength = 4 + 1;
        return (_isBlockReaded && _currentCount < BlockSize) || (buffer.Length - offset >= minLength);
    }
    
    public void Reset()
    {
        _isBlockReaded = false;
        _currentCount = 0;
        _decodeSize = 0;
        Array.Fill(_currentBuffer, 0f);
        Array.Fill(_convertBuffer, 0);
    }
    
    private void DecodeBlock(byte[] buffer, ref int offset)
    {
        _bitWidth = ReadIntLittleEndianPaddedOnBitWidth(buffer, ref offset, 1);
        if ((_bitWidth & (1 << 7)) != 0)
        {
            _decodeSize = _bitWidth & ~(1 << 7);
            var decoder = new PlainDecoder();
            for (int i = 0; i < _decodeSize; i++)
            {
                _currentBuffer[i] = decoder.ReadFloat(buffer, ref offset);
            }
        }
        else
        {
            _decodeSize = BlockSize + 1;
            float preValue = BitConverter.ToSingle(buffer, offset);
            offset += 4;
            _currentBuffer[0] = preValue;
            var packer = new IntPacker(_bitWidth);
            var packcle = new byte[_bitWidth];
            Array.Copy(buffer, offset, packcle, 0, _bitWidth);
            offset += _bitWidth;
            var tmpBuffer = new int[8];
            packer.Unpack8Values(packcle, 0, tmpBuffer);
            for (int i = 0; i < 8; i++)
            {
                _convertBuffer[i] = tmpBuffer[i];
            }
            Recalculate();
        }
        _isBlockReaded = true;
    }
    
    private void Recalculate()
    {
        for (int i = 0; i < BlockSize; i++)
        {
            if (_convertBuffer[i] % 2 == 0)
            {
                _convertBuffer[i] = -_convertBuffer[i] / 2;
            }
            else
            {
                _convertBuffer[i] = (_convertBuffer[i] + 1) / 2;
            }
        }
        
        if (PredictScheme == "delta")
        {
            _convertBuffer[0] = _convertBuffer[0] + BitConverter.SingleToInt32Bits(_currentBuffer[0]);
            _currentBuffer[1] = BitConverter.Int32BitsToSingle(_convertBuffer[0]);
            for (int i = 1; i < BlockSize; i++)
            {
                _convertBuffer[i] = _convertBuffer[i] + _convertBuffer[i - 1];
                _currentBuffer[i + 1] = BitConverter.Int32BitsToSingle(_convertBuffer[i]);
            }
        }
        else if (PredictScheme == "fire")
        {
            _firePred.Reset();
            int prev = BitConverter.SingleToInt32Bits(_currentBuffer[0]);
            for (int i = 0; i < BlockSize; i++)
            {
                int pred = _firePred.Predict(prev);
                int err = _convertBuffer[i];
                int val = pred + err;
                _currentBuffer[i + 1] = BitConverter.Int32BitsToSingle(val);
                _firePred.Train(prev, val, err);
                prev = val;
            }
        }
    }
    
    private static int ReadIntLittleEndianPaddedOnBitWidth(byte[] buffer, ref int offset, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = new byte[4];
        Array.Copy(buffer, offset, bytes, 0, byteWidth);
        offset += byteWidth;
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt32(bytes, 0);
    }
}

internal class DoubleSprintzDecoderImpl : ISprintzDecoderImpl
{
    private const int BlockSize = 8;
    private const string PredictScheme = "fire";
    
    private readonly double[] _currentBuffer = new double[BlockSize + 1];
    private readonly long[] _convertBuffer = new long[BlockSize];
    private readonly LongFire _firePred = new(3);
    private bool _isBlockReaded = false;
    private int _currentCount = 0;
    private int _decodeSize = 0;
    private int _bitWidth = 0;
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        if (!_isBlockReaded)
        {
            DecodeBlock(buffer, ref offset);
        }
        
        double currentValue = _currentBuffer[_currentCount++];
        if (_currentCount == _decodeSize)
        {
            _isBlockReaded = false;
            _currentCount = 0;
        }
        return currentValue;
    }
    
    public int ReadInt(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public long ReadLong(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        int minLength = 8 + 1;
        return (_isBlockReaded && _currentCount < BlockSize) || (buffer.Length - offset >= minLength);
    }
    
    public void Reset()
    {
        _isBlockReaded = false;
        _currentCount = 0;
        _decodeSize = 0;
        Array.Fill(_currentBuffer, 0.0);
        Array.Fill(_convertBuffer, 0L);
    }
    
    private void DecodeBlock(byte[] buffer, ref int offset)
    {
        _bitWidth = ReadIntLittleEndianPaddedOnBitWidth(buffer, ref offset, 1);
        if ((_bitWidth & (1 << 7)) != 0)
        {
            _decodeSize = _bitWidth & ~(1 << 7);
            var decoder = new PlainDecoder();
            for (int i = 0; i < _decodeSize; i++)
            {
                _currentBuffer[i] = decoder.ReadDouble(buffer, ref offset);
            }
        }
        else
        {
            _decodeSize = BlockSize + 1;
            double preValue = BitConverter.ToDouble(buffer, offset);
            offset += 8;
            _currentBuffer[0] = preValue;
            var packer = new LongPacker(_bitWidth);
            var packcle = new byte[_bitWidth];
            Array.Copy(buffer, offset, packcle, 0, _bitWidth);
            offset += _bitWidth;
            var tmpBuffer = new long[8];
            packer.Unpack8Values(packcle, 0, tmpBuffer);
            for (int i = 0; i < 8; i++)
            {
                _convertBuffer[i] = tmpBuffer[i];
            }
            Recalculate();
        }
        _isBlockReaded = true;
    }
    
    private void Recalculate()
    {
        for (int i = 0; i < BlockSize; i++)
        {
            if (_convertBuffer[i] % 2 == 0)
            {
                _convertBuffer[i] = -_convertBuffer[i] / 2;
            }
            else
            {
                _convertBuffer[i] = (_convertBuffer[i] + 1) / 2;
            }
        }
        
        if (PredictScheme == "delta")
        {
            _convertBuffer[0] = _convertBuffer[0] + BitConverter.DoubleToInt64Bits(_currentBuffer[0]);
            _currentBuffer[1] = BitConverter.Int64BitsToDouble(_convertBuffer[0]);
            for (int i = 1; i < BlockSize; i++)
            {
                _convertBuffer[i] = _convertBuffer[i] + _convertBuffer[i - 1];
                _currentBuffer[i + 1] = BitConverter.Int64BitsToDouble(_convertBuffer[i]);
            }
        }
        else if (PredictScheme == "fire")
        {
            _firePred.Reset();
            long prev = BitConverter.DoubleToInt64Bits(_currentBuffer[0]);
            for (int i = 0; i < BlockSize; i++)
            {
                long pred = _firePred.Predict(prev);
                long err = _convertBuffer[i];
                long val = pred + err;
                _currentBuffer[i + 1] = BitConverter.Int64BitsToDouble(val);
                _firePred.Train(prev, val, err);
                prev = val;
            }
        }
    }
    
    private static int ReadIntLittleEndianPaddedOnBitWidth(byte[] buffer, ref int offset, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = new byte[4];
        Array.Copy(buffer, offset, bytes, 0, byteWidth);
        offset += byteWidth;
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt32(bytes, 0);
    }
}
