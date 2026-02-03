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

public class RlbeDecoder : IDecoder
{
    private readonly TsDataType _dataType;
    private IRlbeDecoderImpl _impl;
    
    public RlbeDecoder(TsDataType dataType)
    {
        _dataType = dataType;
        _impl = dataType switch
        {
            TsDataType.Int32 => new IntRlbeDecoderImpl(),
            TsDataType.Int64 or TsDataType.Timestamp => new LongRlbeDecoderImpl(),
            TsDataType.Float => new FloatRlbeDecoderImpl(),
            TsDataType.Double => new DoubleRlbeDecoderImpl(),
            _ => throw new NotSupportedException($"RLBE decoding does not support {dataType}")
        };
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("RLBE decoding does not support boolean values");
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
        throw new NotSupportedException("RLBE decoding does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("RLBE decoding does not support byte array values");
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

internal interface IRlbeDecoderImpl
{
    int ReadInt(byte[] buffer, ref int offset);
    long ReadLong(byte[] buffer, ref int offset);
    float ReadFloat(byte[] buffer, ref int offset);
    double ReadDouble(byte[] buffer, ref int offset);
    bool HasNext(byte[] buffer, int offset);
    void Reset();
}

internal class IntRlbeDecoderImpl : IRlbeDecoderImpl
{
    private int _blocksize;
    private int[] _data = Array.Empty<int>();
    private int _writeindex = -1;
    private int _readindex = -1;
    private int[] _fibonacci = Array.Empty<int>();
    private byte _byteBuffer;
    private int _numberLeftInBuffer;
    
    public IntRlbeDecoderImpl()
    {
        _numberLeftInBuffer = 0;
        _byteBuffer = 0;
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        if (_readindex < _writeindex)
        {
            return _data[++_readindex];
        }
        else
        {
            ReadT(buffer, ref offset);
            return _data[++_readindex];
        }
    }
    
    public long ReadLong(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return (offset < buffer.Length || _readindex < _writeindex);
    }
    
    public void Reset()
    {
        _writeindex = -1;
        _readindex = -1;
        _numberLeftInBuffer = 0;
        _byteBuffer = 0;
    }
    
    private void ReadT(byte[] buffer, ref int offset)
    {
        ReadHead(buffer, ref offset);
        while (_writeindex < _blocksize - 1)
        {
            int seglength = 0;
            int runlength = 0;
            
            for (int bit = 5; bit >= 0; bit--)
            {
                seglength |= (ReadBit(buffer, ref offset) << bit);
            }
            
            int now = ReadBit(buffer, ref offset);
            int next = ReadBit(buffer, ref offset);
            
            int fibIdx = 1;
            while (true)
            {
                if (fibIdx > 1)
                    _fibonacci[fibIdx] = _fibonacci[fibIdx - 1] + _fibonacci[fibIdx - 2];
                if (now == 1)
                    runlength += _fibonacci[fibIdx];
                if (now == 1 && next == 1)
                    break;
                fibIdx++;
                now = next;
                next = ReadBit(buffer, ref offset);
            }
            
            for (int i = 1; i <= runlength; i++)
            {
                int readinttemp = 0;
                for (int k = seglength - 1; k >= 0; k--)
                {
                    readinttemp += (ReadBit(buffer, ref offset) << k);
                }
                if (seglength == 32)
                    readinttemp -= (1 << 31);
                    
                if (_writeindex == -1)
                {
                    _data[++_writeindex] = readinttemp;
                }
                else
                {
                    ++_writeindex;
                    _data[_writeindex] = _data[_writeindex - 1] + readinttemp;
                }
            }
        }
    }
    
    private void ReadHead(byte[] buffer, ref int offset)
    {
        if (_writeindex >= 0 && _data != null)
        {
            for (int i = 0; i <= _writeindex; i++)
            {
                _data[i] = 0;
            }
        }
        _writeindex = -1;
        _readindex = -1;
        ClearBuffer(buffer, ref offset);
        ReadBlockSize(buffer, ref offset);
        _data = new int[_blocksize * 2 + 1];
        _fibonacci = new int[_blocksize * 2 + 1];
        for (int i = 0; i < _blocksize * 2; i++)
        {
            _data[i] = 0;
            _fibonacci[i] = 0;
        }
        _fibonacci[0] = 1;
        _fibonacci[1] = 1;
    }
    
    private int ReadBit(byte[] buffer, ref int offset)
    {
        if (_numberLeftInBuffer == 0)
        {
            LoadBuffer(buffer, ref offset);
            _numberLeftInBuffer = 8;
        }
        int top = ((_byteBuffer >> 7) & 1);
        _byteBuffer <<= 1;
        _numberLeftInBuffer--;
        return top;
    }
    
    private void LoadBuffer(byte[] buffer, ref int offset)
    {
        _byteBuffer = buffer[offset++];
    }
    
    private void ClearBuffer(byte[] buffer, ref int offset)
    {
        while (_numberLeftInBuffer > 0)
        {
            ReadBit(buffer, ref offset);
        }
    }
    
    private void ReadBlockSize(byte[] buffer, ref int offset)
    {
        _blocksize = 0;
        for (int i = 31; i >= 0; i--)
        {
            if (ReadBit(buffer, ref offset) == 1)
            {
                _blocksize |= (1 << i);
            }
        }
    }
}

internal class LongRlbeDecoderImpl : IRlbeDecoderImpl
{
    private int _blocksize;
    private long[] _data = Array.Empty<long>();
    private int _writeindex = -1;
    private int _readindex = -1;
    private long[] _fibonacci = Array.Empty<long>();
    private byte _byteBuffer;
    private int _numberLeftInBuffer;
    
    public LongRlbeDecoderImpl()
    {
        _numberLeftInBuffer = 0;
        _byteBuffer = 0;
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        if (_readindex < _writeindex)
        {
            return _data[++_readindex];
        }
        else
        {
            ReadT(buffer, ref offset);
            return _data[++_readindex];
        }
    }
    
    public int ReadInt(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public float ReadFloat(byte[] buffer, ref int offset) => throw new NotSupportedException();
    public double ReadDouble(byte[] buffer, ref int offset) => throw new NotSupportedException();
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return (offset < buffer.Length || _readindex < _writeindex);
    }
    
    public void Reset()
    {
        _writeindex = -1;
        _readindex = -1;
        _numberLeftInBuffer = 0;
        _byteBuffer = 0;
    }
    
    private void ReadT(byte[] buffer, ref int offset)
    {
        ReadHead(buffer, ref offset);
        while (_writeindex < _blocksize - 1)
        {
            int seglength = 0;
            long runlength = 0;
            
            for (int bit = 6; bit >= 0; bit--)
            {
                seglength |= (ReadBit(buffer, ref offset) << bit);
            }
            
            int now = ReadBit(buffer, ref offset);
            int next = ReadBit(buffer, ref offset);
            
            int fibIdx = 1;
            while (true)
            {
                if (fibIdx > 1)
                    _fibonacci[fibIdx] = _fibonacci[fibIdx - 1] + _fibonacci[fibIdx - 2];
                if (now == 1)
                    runlength += _fibonacci[fibIdx];
                if (now == 1 && next == 1)
                    break;
                fibIdx++;
                now = next;
                next = ReadBit(buffer, ref offset);
            }
            
            for (int i = 1; i <= runlength; i++)
            {
                long readlongtemp = 0;
                for (int k = seglength - 1; k >= 0; k--)
                {
                    readlongtemp += ((long)ReadBit(buffer, ref offset) << k);
                }
                if (seglength == 64)
                    readlongtemp -= (1L << 63);
                    
                if (_writeindex == -1)
                {
                    _data[++_writeindex] = readlongtemp;
                }
                else
                {
                    ++_writeindex;
                    _data[_writeindex] = _data[_writeindex - 1] + readlongtemp;
                }
            }
        }
    }
    
    private void ReadHead(byte[] buffer, ref int offset)
    {
        if (_writeindex >= 0 && _data != null)
        {
            for (int i = 0; i <= _writeindex; i++)
            {
                _data[i] = 0;
            }
        }
        _writeindex = -1;
        _readindex = -1;
        ClearBuffer(buffer, ref offset);
        ReadBlockSize(buffer, ref offset);
        _data = new long[_blocksize * 2 + 1];
        _fibonacci = new long[_blocksize * 2 + 1];
        for (int i = 0; i < _blocksize * 2; i++)
        {
            _data[i] = 0;
            _fibonacci[i] = 0;
        }
        _fibonacci[0] = 1;
        _fibonacci[1] = 1;
    }
    
    private int ReadBit(byte[] buffer, ref int offset)
    {
        if (_numberLeftInBuffer == 0)
        {
            LoadBuffer(buffer, ref offset);
            _numberLeftInBuffer = 8;
        }
        int top = ((_byteBuffer >> 7) & 1);
        _byteBuffer <<= 1;
        _numberLeftInBuffer--;
        return top;
    }
    
    private void LoadBuffer(byte[] buffer, ref int offset)
    {
        _byteBuffer = buffer[offset++];
    }
    
    private void ClearBuffer(byte[] buffer, ref int offset)
    {
        while (_numberLeftInBuffer > 0)
        {
            ReadBit(buffer, ref offset);
        }
    }
    
    private void ReadBlockSize(byte[] buffer, ref int offset)
    {
        _blocksize = 0;
        for (int i = 31; i >= 0; i--)
        {
            if (ReadBit(buffer, ref offset) == 1)
            {
                _blocksize |= (1 << i);
            }
        }
    }
}

internal class FloatRlbeDecoderImpl : IRlbeDecoderImpl
{
    private readonly IntRlbeDecoderImpl _impl = new();
    
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

internal class DoubleRlbeDecoderImpl : IRlbeDecoderImpl
{
    private readonly LongRlbeDecoderImpl _impl = new();
    
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
