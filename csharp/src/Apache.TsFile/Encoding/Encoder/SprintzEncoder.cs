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

namespace Apache.TsFile.Encoding.Encoder;

public class SprintzEncoder : IEncoder
{
    private readonly TsDataType _dataType;
    private ISprintzEncoderImpl _impl;
    
    public SprintzEncoder(TsDataType dataType)
    {
        _dataType = dataType;
        _impl = dataType switch
        {
            TsDataType.Int32 => new IntSprintzEncoderImpl(),
            TsDataType.Int64 or TsDataType.Timestamp => new LongSprintzEncoderImpl(),
            TsDataType.Float => new FloatSprintzEncoderImpl(),
            TsDataType.Double => new DoubleSprintzEncoderImpl(),
            _ => throw new NotSupportedException($"SPRINTZ encoding does not support {dataType}")
        };
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("SPRINTZ encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        _impl.EncodeInt(value, stream);
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        _impl.EncodeLong(value, stream);
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        _impl.EncodeFloat(value, stream);
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        _impl.EncodeDouble(value, stream);
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("SPRINTZ encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("SPRINTZ encoding does not support byte array values");
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

internal interface ISprintzEncoderImpl
{
    void EncodeInt(int value, MemoryStream stream);
    void EncodeLong(long value, MemoryStream stream);
    void EncodeFloat(float value, MemoryStream stream);
    void EncodeDouble(double value, MemoryStream stream);
    void Flush(MemoryStream stream);
    int GetOneItemMaxSize();
    long GetMaxByteSize();
}

internal class IntSprintzEncoderImpl : ISprintzEncoderImpl
{
    private const int BlockSize = 8;
    private const int GroupMax = 16;
    private const string PredictMethod = "fire";
    
    private readonly List<int> _values = new();
    private readonly MemoryStream _byteCache = new();
    private readonly IntFire _firePred = new(2);
    private int _groupNum = 0;
    private bool _isFirstCached = false;
    
    public void EncodeInt(int value, MemoryStream stream)
    {
        if (!_isFirstCached)
        {
            _values.Add(value);
            _isFirstCached = true;
            return;
        }
        
        _values.Add(value);
        
        if (_values.Count == BlockSize + 1)
        {
            int pre = _values[0];
            _firePred.Reset();
            for (int i = 1; i <= BlockSize; i++)
            {
                int tmp = _values[i];
                _values[i] = Predict(_values[i], pre);
                pre = tmp;
            }
            BitPack();
            _isFirstCached = false;
            _values.Clear();
            _groupNum++;
            if (_groupNum == GroupMax)
            {
                Flush(stream);
            }
        }
    }
    
    public void EncodeLong(long value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeFloat(float value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeDouble(double value, MemoryStream stream) => throw new NotSupportedException();
    
    private int Predict(int value, int preValue)
    {
        int pred;
        if (PredictMethod == "delta")
        {
            pred = value - preValue;
        }
        else if (PredictMethod == "fire")
        {
            int prediction = _firePred.Predict(preValue);
            int err = value - prediction;
            _firePred.Train(preValue, value, err);
            pred = err;
        }
        else
        {
            pred = value - preValue;
        }
        
        if (pred <= 0)
        {
            pred = -2 * pred;
        }
        else
        {
            pred = 2 * pred - 1;
        }
        return pred;
    }
    
    private void BitPack()
    {
        int preValue = _values[0];
        _values.RemoveAt(0);
        int bitWidth = GetIntMaxBitWidth(_values);
        var packer = new IntPacker(bitWidth);
        var bytes = new byte[bitWidth];
        var tmpBuffer = new int[BlockSize];
        for (int i = 0; i < BlockSize; i++)
        {
            tmpBuffer[i] = _values[i];
        }
        packer.Pack8Values(tmpBuffer, 0, bytes);
        WriteIntLittleEndianPaddedOnBitWidth(bitWidth, _byteCache, 1);
        WriteUnsignedVarInt(preValue, _byteCache);
        _byteCache.Write(bytes, 0, bytes.Length);
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_byteCache.Length > 0)
        {
            _byteCache.WriteTo(stream);
            _byteCache.SetLength(0);
        }
        if (_values.Count > 0)
        {
            int size = _values.Count;
            size |= (1 << 7);
            WriteIntLittleEndianPaddedOnBitWidth(size, stream, 1);
            var encoder = new RleEncoder(TsDataType.Int32);
            foreach (var val in _values)
            {
                encoder.Encode(val, stream);
            }
            encoder.Flush(stream);
        }
        Reset();
    }
    
    private void Reset()
    {
        _byteCache.SetLength(0);
        _isFirstCached = false;
        _groupNum = 0;
        _values.Clear();
    }
    
    public int GetOneItemMaxSize() => 1 + (1 + BlockSize) * 4;
    public long GetMaxByteSize() => 1 + (_values.Count + 1) * 4L;
    
    private static int GetIntMaxBitWidth(List<int> list)
    {
        if (list.Count == 0) return 1;
        int max = 0;
        foreach (var val in list)
        {
            if (val > max) max = val;
        }
        if (max == 0) return 1;
        int bits = 1;
        while ((1 << bits) <= max && bits < 32)
        {
            bits++;
        }
        return bits;
    }
    
    private static void WriteIntLittleEndianPaddedOnBitWidth(int value, Stream stream, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = BitConverter.GetBytes(value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, byteWidth);
    }
    
    private static void WriteUnsignedVarInt(int value, Stream stream)
    {
        while ((value & 0xFFFFFF80) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)(value & 0x7F));
    }
}

internal class LongSprintzEncoderImpl : ISprintzEncoderImpl
{
    private const int BlockSize = 8;
    private const int GroupMax = 16;
    private const string PredictMethod = "fire";
    
    private readonly List<long> _values = new();
    private readonly MemoryStream _byteCache = new();
    private readonly LongFire _firePred = new(3);
    private int _groupNum = 0;
    private bool _isFirstCached = false;
    
    public void EncodeLong(long value, MemoryStream stream)
    {
        if (!_isFirstCached)
        {
            _values.Add(value);
            _isFirstCached = true;
            return;
        }
        
        _values.Add(value);
        
        if (_values.Count == BlockSize + 1)
        {
            long pre = _values[0];
            _firePred.Reset();
            for (int i = 1; i <= BlockSize; i++)
            {
                long tmp = _values[i];
                _values[i] = Predict(_values[i], pre);
                pre = tmp;
            }
            BitPack();
            _isFirstCached = false;
            _values.Clear();
            _groupNum++;
            if (_groupNum == GroupMax)
            {
                Flush(stream);
            }
        }
    }
    
    public void EncodeInt(int value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeFloat(float value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeDouble(double value, MemoryStream stream) => throw new NotSupportedException();
    
    private long Predict(long value, long preValue)
    {
        long pred;
        if (PredictMethod == "delta")
        {
            pred = value - preValue;
        }
        else if (PredictMethod == "fire")
        {
            long prediction = _firePred.Predict(preValue);
            long err = value - prediction;
            _firePred.Train(preValue, value, err);
            pred = err;
        }
        else
        {
            pred = value - preValue;
        }
        
        if (pred <= 0)
        {
            pred = -2 * pred;
        }
        else
        {
            pred = 2 * pred - 1;
        }
        return pred;
    }
    
    private void BitPack()
    {
        long preValue = _values[0];
        _values.RemoveAt(0);
        int bitWidth = GetLongMaxBitWidth(_values);
        var packer = new LongPacker(bitWidth);
        var bytes = new byte[bitWidth];
        var tmpBuffer = new long[BlockSize];
        for (int i = 0; i < BlockSize; i++)
        {
            tmpBuffer[i] = _values[i];
        }
        packer.Pack8Values(tmpBuffer, 0, bytes);
        WriteIntLittleEndianPaddedOnBitWidth(bitWidth, _byteCache, 1);
        var preValueBytes = BitConverter.GetBytes(preValue);
        _byteCache.Write(preValueBytes, 0, 8);
        _byteCache.Write(bytes, 0, bytes.Length);
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_byteCache.Length > 0)
        {
            _byteCache.WriteTo(stream);
            _byteCache.SetLength(0);
        }
        if (_values.Count > 0)
        {
            int size = _values.Count;
            size |= (1 << 7);
            WriteIntLittleEndianPaddedOnBitWidth(size, stream, 1);
            var encoder = new RleEncoder(TsDataType.Int64);
            foreach (var val in _values)
            {
                encoder.Encode(val, stream);
            }
            encoder.Flush(stream);
        }
        Reset();
    }
    
    private void Reset()
    {
        _byteCache.SetLength(0);
        _isFirstCached = false;
        _groupNum = 0;
        _values.Clear();
    }
    
    public int GetOneItemMaxSize() => 1 + (1 + BlockSize) * 8;
    public long GetMaxByteSize() => 1 + (1L + _values.Count) * 8;
    
    private static int GetLongMaxBitWidth(List<long> list)
    {
        if (list.Count == 0) return 1;
        long max = 0;
        foreach (var val in list)
        {
            if (val > max) max = val;
        }
        if (max == 0) return 1;
        int bits = 1;
        while ((1L << bits) <= max && bits < 64)
        {
            bits++;
        }
        return bits;
    }
    
    private static void WriteIntLittleEndianPaddedOnBitWidth(int value, Stream stream, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = BitConverter.GetBytes(value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, byteWidth);
    }
}

internal class FloatSprintzEncoderImpl : ISprintzEncoderImpl
{
    private const int BlockSize = 8;
    private const int GroupMax = 16;
    private const string PredictMethod = "fire";
    
    private readonly List<float> _values = new();
    private readonly MemoryStream _byteCache = new();
    private readonly IntFire _firePred = new(2);
    private readonly int[] _convertBuffer = new int[BlockSize];
    private int _groupNum = 0;
    private bool _isFirstCached = false;
    
    public void EncodeFloat(float value, MemoryStream stream)
    {
        if (!_isFirstCached)
        {
            _values.Add(value);
            _isFirstCached = true;
            return;
        }
        
        _values.Add(value);
        
        if (_values.Count == BlockSize + 1)
        {
            _firePred.Reset();
            for (int i = 1; i <= BlockSize; i++)
            {
                _convertBuffer[i - 1] = Predict(_values[i], _values[i - 1]);
            }
            BitPack();
            _isFirstCached = false;
            _values.Clear();
            _groupNum++;
            if (_groupNum == GroupMax)
            {
                Flush(stream);
            }
        }
    }
    
    public void EncodeInt(int value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeLong(long value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeDouble(double value, MemoryStream stream) => throw new NotSupportedException();
    
    private int Predict(float value, float preValue)
    {
        int pred;
        if (PredictMethod == "delta")
        {
            pred = BitConverter.SingleToInt32Bits(value) - BitConverter.SingleToInt32Bits(preValue);
        }
        else if (PredictMethod == "fire")
        {
            int prev = BitConverter.SingleToInt32Bits(preValue);
            int val = BitConverter.SingleToInt32Bits(value);
            int prediction = _firePred.Predict(prev);
            int err = val - prediction;
            _firePred.Train(prev, val, err);
            pred = err;
        }
        else
        {
            pred = BitConverter.SingleToInt32Bits(value) - BitConverter.SingleToInt32Bits(preValue);
        }
        
        if (pred <= 0)
        {
            pred = -2 * pred;
        }
        else
        {
            pred = 2 * pred - 1;
        }
        return pred;
    }
    
    private void BitPack()
    {
        float preValue = _values[0];
        _values.RemoveAt(0);
        var convertBufferList = new List<int>(_convertBuffer);
        int bitWidth = GetIntMaxBitWidth(convertBufferList);
        var packer = new IntPacker(bitWidth);
        var bytes = new byte[bitWidth];
        packer.Pack8Values(_convertBuffer, 0, bytes);
        WriteIntLittleEndianPaddedOnBitWidth(bitWidth, _byteCache, 1);
        var preValueBytes = BitConverter.GetBytes(preValue);
        _byteCache.Write(preValueBytes, 0, 4);
        _byteCache.Write(bytes, 0, bytes.Length);
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_byteCache.Length > 0)
        {
            _byteCache.WriteTo(stream);
            _byteCache.SetLength(0);
        }
        if (_values.Count > 0)
        {
            int size = _values.Count;
            size |= (1 << 7);
            WriteIntLittleEndianPaddedOnBitWidth(size, stream, 1);
            var encoder = new PlainEncoder();
            foreach (var val in _values)
            {
                encoder.Encode(val, stream);
            }
            encoder.Flush(stream);
        }
        Reset();
    }
    
    private void Reset()
    {
        _byteCache.SetLength(0);
        _isFirstCached = false;
        _groupNum = 0;
        _values.Clear();
    }
    
    public int GetOneItemMaxSize() => 1 + (1 + BlockSize) * 4;
    public long GetMaxByteSize() => 1 + (_values.Count + 1) * 4L;
    
    private static int GetIntMaxBitWidth(List<int> list)
    {
        if (list.Count == 0) return 1;
        int max = 0;
        foreach (var val in list)
        {
            if (val > max) max = val;
        }
        if (max == 0) return 1;
        int bits = 1;
        while ((1 << bits) <= max && bits < 32)
        {
            bits++;
        }
        return bits;
    }
    
    private static void WriteIntLittleEndianPaddedOnBitWidth(int value, Stream stream, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = BitConverter.GetBytes(value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, byteWidth);
    }
}

internal class DoubleSprintzEncoderImpl : ISprintzEncoderImpl
{
    private const int BlockSize = 8;
    private const int GroupMax = 16;
    private const string PredictMethod = "fire";
    
    private readonly List<double> _values = new();
    private readonly MemoryStream _byteCache = new();
    private readonly LongFire _firePred = new(3);
    private readonly long[] _convertBuffer = new long[BlockSize];
    private int _groupNum = 0;
    private bool _isFirstCached = false;
    
    public void EncodeDouble(double value, MemoryStream stream)
    {
        if (!_isFirstCached)
        {
            _values.Add(value);
            _isFirstCached = true;
            return;
        }
        
        _values.Add(value);
        
        if (_values.Count == BlockSize + 1)
        {
            _firePred.Reset();
            for (int i = 1; i <= BlockSize; i++)
            {
                _convertBuffer[i - 1] = Predict(_values[i], _values[i - 1]);
            }
            BitPack();
            _isFirstCached = false;
            _values.Clear();
            _groupNum++;
            if (_groupNum == GroupMax)
            {
                Flush(stream);
            }
        }
    }
    
    public void EncodeInt(int value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeLong(long value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeFloat(float value, MemoryStream stream) => throw new NotSupportedException();
    
    private long Predict(double value, double preValue)
    {
        long pred;
        if (PredictMethod == "delta")
        {
            pred = BitConverter.DoubleToInt64Bits(value) - BitConverter.DoubleToInt64Bits(preValue);
        }
        else if (PredictMethod == "fire")
        {
            long prev = BitConverter.DoubleToInt64Bits(preValue);
            long val = BitConverter.DoubleToInt64Bits(value);
            long prediction = _firePred.Predict(prev);
            long err = val - prediction;
            _firePred.Train(prev, val, err);
            pred = err;
        }
        else
        {
            pred = BitConverter.DoubleToInt64Bits(value) - BitConverter.DoubleToInt64Bits(preValue);
        }
        
        if (pred <= 0)
        {
            pred = -2 * pred;
        }
        else
        {
            pred = 2 * pred - 1;
        }
        return pred;
    }
    
    private void BitPack()
    {
        double preValue = _values[0];
        _values.RemoveAt(0);
        var convertBufferList = new List<long>(_convertBuffer);
        int bitWidth = GetLongMaxBitWidth(convertBufferList);
        var packer = new LongPacker(bitWidth);
        var bytes = new byte[bitWidth];
        packer.Pack8Values(_convertBuffer, 0, bytes);
        WriteIntLittleEndianPaddedOnBitWidth(bitWidth, _byteCache, 1);
        var preValueBytes = BitConverter.GetBytes(preValue);
        _byteCache.Write(preValueBytes, 0, 8);
        _byteCache.Write(bytes, 0, bytes.Length);
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_byteCache.Length > 0)
        {
            _byteCache.WriteTo(stream);
            _byteCache.SetLength(0);
        }
        if (_values.Count > 0)
        {
            int size = _values.Count;
            size |= (1 << 7);
            WriteIntLittleEndianPaddedOnBitWidth(size, stream, 1);
            var encoder = new PlainEncoder();
            foreach (var val in _values)
            {
                encoder.Encode(val, stream);
            }
            encoder.Flush(stream);
        }
        Reset();
    }
    
    private void Reset()
    {
        _byteCache.SetLength(0);
        _isFirstCached = false;
        _groupNum = 0;
        _values.Clear();
    }
    
    public int GetOneItemMaxSize() => 1 + (1 + BlockSize) * 8;
    public long GetMaxByteSize() => 1 + (long)(_values.Count + 1) * 8;
    
    private static int GetLongMaxBitWidth(List<long> list)
    {
        if (list.Count == 0) return 1;
        long max = 0;
        foreach (var val in list)
        {
            if (val > max) max = val;
        }
        if (max == 0) return 1;
        int bits = 1;
        while ((1L << bits) <= max && bits < 64)
        {
            bits++;
        }
        return bits;
    }
    
    private static void WriteIntLittleEndianPaddedOnBitWidth(int value, Stream stream, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = BitConverter.GetBytes(value);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, byteWidth);
    }
}
