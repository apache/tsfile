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
/// Gorilla decoder - decodes XOR-compressed time-series data.
/// </summary>
public class GorillaDecoder : IDecoder
{
    private BitReader? _bitReader;
    private long _previousValue;
    private int _previousLeadingZeros;
    private int _previousTrailingZeros;
    private bool _first = true;
    private readonly TsDataType _dataType;
    private readonly int _bitWidth;
    private readonly Queue<float> _floatQueue = new();
    private readonly Queue<double> _doubleQueue = new();
    private readonly Queue<int> _intQueue = new();
    private readonly Queue<long> _longQueue = new();
    
    public GorillaDecoder(TsDataType dataType)
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
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Gorilla decoder does not support boolean values");
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        EnsureData(buffer, ref offset);
        return _intQueue.Dequeue();
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        EnsureData(buffer, ref offset);
        return _longQueue.Dequeue();
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        EnsureData(buffer, ref offset);
        return _floatQueue.Dequeue();
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        EnsureData(buffer, ref offset);
        return _doubleQueue.Dequeue();
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Gorilla decoder does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("Gorilla decoder does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return _floatQueue.Count > 0 || _doubleQueue.Count > 0 || 
               _intQueue.Count > 0 || _longQueue.Count > 0 || 
               offset < buffer.Length;
    }
    
    public void Reset()
    {
        _bitReader = null;
        _previousValue = 0;
        _previousLeadingZeros = 0;
        _previousTrailingZeros = 0;
        _first = true;
        _floatQueue.Clear();
        _doubleQueue.Clear();
        _intQueue.Clear();
        _longQueue.Clear();
    }
    
    private void EnsureData(byte[] buffer, ref int offset)
    {
        if (HasData()) return;
        
        if (_bitReader == null)
        {
            // Read length
            int length = ReadInt32(buffer, ref offset);
            
            // Read bit width (though we already have it from constructor)
            int storedBitWidth = buffer[offset++];
            
            // Read encoded data
            byte[] encodedData = new byte[length];
            Array.Copy(buffer, offset, encodedData, 0, length);
            offset += length;
            
            _bitReader = new BitReader(encodedData);
            _first = true;
        }
        
        // Decode one value at a time
        if (_bitReader.HasNext())
        {
            try
            {
                long decoded = DecodeValue();
                
                // Queue the decoded value based on data type
                switch (_dataType)
                {
                    case TsDataType.Float:
                        _floatQueue.Enqueue(BitConverter.Int32BitsToSingle((int)decoded));
                        break;
                    case TsDataType.Double:
                        _doubleQueue.Enqueue(BitConverter.Int64BitsToDouble(decoded));
                        break;
                    case TsDataType.Int32:
                        _intQueue.Enqueue((int)decoded);
                        break;
                    case TsDataType.Int64:
                    case TsDataType.Timestamp:
                        _longQueue.Enqueue(decoded);
                        break;
                }
            }
            catch (InvalidOperationException)
            {
                // No more bits to read
            }
        }
    }
    
    private long DecodeValue()
    {
        if (_first)
        {
            // First value: read full width
            _previousValue = _bitReader!.ReadBits(_bitWidth);
            _first = false;
            return _previousValue;
        }
        
        // Read control bit
        int controlBit = _bitReader!.ReadBit();
        
        if (controlBit == 0)
        {
            // Value same as previous
            return _previousValue;
        }
        
        // Read second control bit
        int blockBit = _bitReader.ReadBit();
        
        long xor;
        if (blockBit == 0)
        {
            // Use previous block info
            int meaningfulBits = _bitWidth - _previousLeadingZeros - _previousTrailingZeros;
            long meaningfulValue = _bitReader.ReadBits(meaningfulBits);
            xor = meaningfulValue << _previousTrailingZeros;
        }
        else
        {
            // New block
            int leadingZeros = (int)_bitReader.ReadBits(5);
            int meaningfulBits = (int)_bitReader.ReadBits(6);
            
            int trailingZeros = _bitWidth - leadingZeros - meaningfulBits;
            
            // Safety check
            if (meaningfulBits < 0 || meaningfulBits > _bitWidth || trailingZeros < 0)
            {
                throw new InvalidOperationException($"Invalid Gorilla encoding: leadingZeros={leadingZeros}, meaningfulBits={meaningfulBits}, bitWidth={_bitWidth}");
            }
            
            long meaningfulValue = meaningfulBits > 0 ? _bitReader.ReadBits(meaningfulBits) : 0;
            xor = meaningfulValue << trailingZeros;
            
            _previousLeadingZeros = leadingZeros;
            _previousTrailingZeros = trailingZeros;
        }
        
        // XOR with previous to get actual value
        _previousValue = _previousValue ^ xor;
        return _previousValue;
    }
    
    private bool HasData()
    {
        return _floatQueue.Count > 0 || _doubleQueue.Count > 0 || 
               _intQueue.Count > 0 || _longQueue.Count > 0;
    }
    
    private static int ReadInt32(byte[] buffer, ref int offset)
    {
        var bytes = new byte[4];
        Array.Copy(buffer, offset, bytes, 0, 4);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        offset += 4;
        return BitConverter.ToInt32(bytes, 0);
    }
}
