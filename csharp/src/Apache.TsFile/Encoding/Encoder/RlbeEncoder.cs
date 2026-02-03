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

public class RlbeEncoder : IEncoder
{
    private readonly TsDataType _dataType;
    private IRlbeEncoderImpl _impl;
    
    public RlbeEncoder(TsDataType dataType)
    {
        _dataType = dataType;
        _impl = dataType switch
        {
            TsDataType.Int32 => new IntRlbeEncoderImpl(),
            TsDataType.Int64 or TsDataType.Timestamp => new LongRlbeEncoderImpl(),
            TsDataType.Float => new FloatRlbeEncoderImpl(),
            TsDataType.Double => new DoubleRlbeEncoderImpl(),
            _ => throw new NotSupportedException($"RLBE encoding does not support {dataType}")
        };
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("RLBE encoding does not support boolean values");
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
        throw new NotSupportedException("RLBE encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("RLBE encoding does not support byte array values");
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

internal interface IRlbeEncoderImpl
{
    void EncodeInt(int value, MemoryStream stream);
    void EncodeLong(long value, MemoryStream stream);
    void EncodeFloat(float value, MemoryStream stream);
    void EncodeDouble(double value, MemoryStream stream);
    void Flush(MemoryStream stream);
    int GetOneItemMaxSize();
    long GetMaxByteSize();
}

internal class IntRlbeEncoderImpl : IRlbeEncoderImpl
{
    private const int BlockSize = 10000;
    
    private readonly int[] _diffValue = new int[BlockSize + 1];
    private readonly int[] _lengthCode = new int[BlockSize + 1];
    private readonly int[] _lengRle = new int[BlockSize + 1];
    private int _previousValue;
    private int _writeIndex = -1;
    private byte _byteBuffer;
    private int _numberLeftInBuffer;
    
    public IntRlbeEncoderImpl()
    {
        Reset();
    }
    
    public void EncodeInt(int value, MemoryStream stream)
    {
        if (_writeIndex == -1)
        {
            _diffValue[++_writeIndex] = value;
            _lengthCode[_writeIndex] = CalcBinaryLength(value);
            _previousValue = value;
            return;
        }
        
        _diffValue[++_writeIndex] = value - _previousValue;
        _lengthCode[_writeIndex] = CalcBinaryLength(_diffValue[_writeIndex]);
        _previousValue = value;
        
        if (_writeIndex == BlockSize - 1)
        {
            Flush(stream);
        }
    }
    
    public void EncodeLong(long value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeFloat(float value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeDouble(double value, MemoryStream stream) => throw new NotSupportedException();
    
    public void Flush(MemoryStream stream)
    {
        FlushBlock(stream);
    }
    
    private void FlushBlock(MemoryStream stream)
    {
        if (_writeIndex == -1)
        {
            return;
        }
        
        WriteWriteIndex(stream);
        RleOnLengthCode();
        
        for (int i = 0; i <= _writeIndex; i++)
        {
            if (_lengRle[i] > 0)
            {
                FlushSegment(i, stream);
            }
        }
        
        ClearBuffer(stream);
        Reset();
    }
    
    private void FlushSegment(int i, MemoryStream stream)
    {
        for (int bit = 5; bit >= 0; bit--)
        {
            if ((_lengthCode[i] & (1 << bit)) > 0)
            {
                WriteBit(true, stream);
            }
            else
            {
                WriteBit(false, stream);
            }
        }
        
        int fib = CalcFibonacci(_lengRle[i]);
        int fiblen = CalcBinaryLength(fib);
        for (int fibBit = 0; fibBit < fiblen; fibBit++)
        {
            if ((fib & (1 << fibBit)) > 0)
            {
                WriteBit(true, stream);
            }
            else
            {
                WriteBit(false, stream);
            }
        }
        WriteBit(true, stream);
        
        int idx = i;
        do
        {
            int tempDifflen = CalcBinaryLength(_diffValue[idx]);
            for (int k = tempDifflen - 1; k >= 0; k--)
            {
                if ((_diffValue[idx] & (1 << k)) > 0)
                {
                    WriteBit(true, stream);
                }
                else
                {
                    WriteBit(false, stream);
                }
            }
            idx++;
        } while (idx <= _writeIndex && _lengRle[idx] == 0);
    }
    
    private void RleOnLengthCode()
    {
        int i = 0;
        while (i <= _writeIndex)
        {
            int j = i;
            int temprlecal = 0;
            while (j <= _writeIndex && _lengthCode[j] == _lengthCode[i])
            {
                j++;
                temprlecal++;
            }
            _lengRle[i] = temprlecal;
            i = j;
        }
    }
    
    private void WriteWriteIndex(MemoryStream stream)
    {
        for (int i = 31; i >= 0; i--)
        {
            if ((_writeIndex + 1 & (1 << i)) > 0)
            {
                WriteBit(true, stream);
            }
            else
            {
                WriteBit(false, stream);
            }
        }
    }
    
    private void WriteBit(bool b, MemoryStream stream)
    {
        _byteBuffer <<= 1;
        if (b)
        {
            _byteBuffer |= 1;
        }
        
        _numberLeftInBuffer++;
        if (_numberLeftInBuffer == 8)
        {
            ClearBuffer(stream);
        }
    }
    
    private void ClearBuffer(MemoryStream stream)
    {
        if (_numberLeftInBuffer == 0)
        {
            return;
        }
        if (_numberLeftInBuffer > 0)
        {
            _byteBuffer <<= (8 - _numberLeftInBuffer);
        }
        stream.WriteByte(_byteBuffer);
        _numberLeftInBuffer = 0;
        _byteBuffer = 0;
    }
    
    private static int CalcBinaryLength(int val)
    {
        if (val == 0)
        {
            return 1;
        }
        int i = 32;
        while (((1 << (i - 1)) & val) == 0 && i > 0)
        {
            i--;
        }
        return i;
    }
    
    private static int CalcFibonacci(int val)
    {
        int[] fib = new int[BlockSize * 2 + 1];
        fib[0] = 1;
        fib[1] = 1;
        int i;
        for (i = 2; fib[i - 1] <= val; i++)
        {
            fib[i] = fib[i - 1] + fib[i - 2];
        }
        
        i--;
        int valfib = 0;
        while (val > 0)
        {
            while (fib[i] > val && i >= 1)
            {
                i--;
            }
            valfib |= (1 << (i - 1));
            val -= fib[i];
        }
        return valfib;
    }
    
    private void Reset()
    {
        _writeIndex = -1;
        Array.Fill(_diffValue, 0);
        Array.Fill(_lengthCode, 0);
        Array.Fill(_lengRle, 0);
        _byteBuffer = 0;
        _numberLeftInBuffer = 0;
    }
    
    public int GetOneItemMaxSize() => 4 * 4;
    public long GetMaxByteSize() => 5L * 4 * BlockSize;
}

internal class LongRlbeEncoderImpl : IRlbeEncoderImpl
{
    private const int BlockSize = 10000;
    
    private readonly long[] _diffValue = new long[BlockSize + 1];
    private readonly int[] _lengthCode = new int[BlockSize + 1];
    private readonly long[] _lengRle = new long[BlockSize + 1];
    private long _previousValue;
    private int _writeIndex = -1;
    private byte _byteBuffer;
    private int _numberLeftInBuffer;
    
    public LongRlbeEncoderImpl()
    {
        Reset();
    }
    
    public void EncodeLong(long value, MemoryStream stream)
    {
        if (_writeIndex == -1)
        {
            _diffValue[++_writeIndex] = value;
            _lengthCode[_writeIndex] = CalcBinaryLength(value);
            _previousValue = value;
            return;
        }
        
        _diffValue[++_writeIndex] = value - _previousValue;
        _lengthCode[_writeIndex] = CalcBinaryLength(_diffValue[_writeIndex]);
        _previousValue = value;
        
        if (_writeIndex == BlockSize - 1)
        {
            Flush(stream);
        }
    }
    
    public void EncodeInt(int value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeFloat(float value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeDouble(double value, MemoryStream stream) => throw new NotSupportedException();
    
    public void Flush(MemoryStream stream)
    {
        FlushBlock(stream);
    }
    
    private void FlushBlock(MemoryStream stream)
    {
        if (_writeIndex == -1)
        {
            return;
        }
        
        WriteWriteIndex(stream);
        RleOnLengthCode();
        
        for (int i = 0; i <= _writeIndex; i++)
        {
            if (_lengRle[i] > 0)
            {
                FlushSegment(i, stream);
            }
        }
        
        ClearBuffer(stream);
        Reset();
    }
    
    private void FlushSegment(int i, MemoryStream stream)
    {
        for (int bit = 6; bit >= 0; bit--)
        {
            if ((_lengthCode[i] & (1 << bit)) > 0)
            {
                WriteBit(true, stream);
            }
            else
            {
                WriteBit(false, stream);
            }
        }
        
        long fib = CalcFibonacci(_lengRle[i]);
        int fiblen = CalcBinaryLength(fib);
        for (int fibBit = 0; fibBit < fiblen; fibBit++)
        {
            if ((fib & (1L << fibBit)) > 0)
            {
                WriteBit(true, stream);
            }
            else
            {
                WriteBit(false, stream);
            }
        }
        WriteBit(true, stream);
        
        int idx = i;
        do
        {
            int tempDifflen = CalcBinaryLength(_diffValue[idx]);
            for (int k = tempDifflen - 1; k >= 0; k--)
            {
                if ((_diffValue[idx] & (1L << k)) > 0)
                {
                    WriteBit(true, stream);
                }
                else
                {
                    WriteBit(false, stream);
                }
            }
            idx++;
        } while (idx <= _writeIndex && _lengRle[idx] == 0);
    }
    
    private void RleOnLengthCode()
    {
        int i = 0;
        while (i <= _writeIndex)
        {
            int j = i;
            int temprlecal = 0;
            while (j <= _writeIndex && _lengthCode[j] == _lengthCode[i])
            {
                j++;
                temprlecal++;
            }
            _lengRle[i] = temprlecal;
            i = j;
        }
    }
    
    private void WriteWriteIndex(MemoryStream stream)
    {
        for (int i = 31; i >= 0; i--)
        {
            if ((_writeIndex + 1 & (1 << i)) > 0)
            {
                WriteBit(true, stream);
            }
            else
            {
                WriteBit(false, stream);
            }
        }
    }
    
    private void WriteBit(bool b, MemoryStream stream)
    {
        _byteBuffer <<= 1;
        if (b)
        {
            _byteBuffer |= 1;
        }
        
        _numberLeftInBuffer++;
        if (_numberLeftInBuffer == 8)
        {
            ClearBuffer(stream);
        }
    }
    
    private void ClearBuffer(MemoryStream stream)
    {
        if (_numberLeftInBuffer == 0)
        {
            return;
        }
        if (_numberLeftInBuffer > 0)
        {
            _byteBuffer <<= (8 - _numberLeftInBuffer);
        }
        stream.WriteByte(_byteBuffer);
        _numberLeftInBuffer = 0;
        _byteBuffer = 0;
    }
    
    private static int CalcBinaryLength(long val)
    {
        if (val == 0)
        {
            return 1;
        }
        int i = 64;
        while (((1L << (i - 1)) & val) == 0 && i > 0)
        {
            i--;
        }
        return i;
    }
    
    private static long CalcFibonacci(long val)
    {
        long[] fib = new long[BlockSize * 2 + 1];
        fib[0] = 1;
        fib[1] = 1;
        int i;
        for (i = 2; fib[i - 1] <= val; i++)
        {
            fib[i] = fib[i - 1] + fib[i - 2];
        }
        
        i--;
        long valfib = 0;
        while (val > 0)
        {
            while (fib[i] > val && i >= 1)
            {
                i--;
            }
            valfib |= (1L << (i - 1));
            val -= fib[i];
        }
        return valfib;
    }
    
    private void Reset()
    {
        _writeIndex = -1;
        Array.Fill(_diffValue, 0L);
        Array.Fill(_lengthCode, 0);
        Array.Fill(_lengRle, 0L);
        _byteBuffer = 0;
        _numberLeftInBuffer = 0;
    }
    
    public int GetOneItemMaxSize() => 4 * 4 * 2;
    public long GetMaxByteSize() => 5L * 4 * BlockSize * 2;
}

internal class FloatRlbeEncoderImpl : IRlbeEncoderImpl
{
    private readonly IntRlbeEncoderImpl _impl = new();
    
    public void EncodeFloat(float value, MemoryStream stream)
    {
        _impl.EncodeInt(BitConverter.SingleToInt32Bits(value), stream);
    }
    
    public void EncodeInt(int value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeLong(long value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeDouble(double value, MemoryStream stream) => throw new NotSupportedException();
    public void Flush(MemoryStream stream) => _impl.Flush(stream);
    public int GetOneItemMaxSize() => _impl.GetOneItemMaxSize();
    public long GetMaxByteSize() => _impl.GetMaxByteSize();
}

internal class DoubleRlbeEncoderImpl : IRlbeEncoderImpl
{
    private readonly LongRlbeEncoderImpl _impl = new();
    
    public void EncodeDouble(double value, MemoryStream stream)
    {
        _impl.EncodeLong(BitConverter.DoubleToInt64Bits(value), stream);
    }
    
    public void EncodeInt(int value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeLong(long value, MemoryStream stream) => throw new NotSupportedException();
    public void EncodeFloat(float value, MemoryStream stream) => throw new NotSupportedException();
    public void Flush(MemoryStream stream) => _impl.Flush(stream);
    public int GetOneItemMaxSize() => _impl.GetOneItemMaxSize();
    public long GetMaxByteSize() => _impl.GetMaxByteSize();
}
