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
/// ZigZag decoder - decodes variable-length encoded unsigned integers back to signed integers.
/// </summary>
public class ZigZagDecoder : IDecoder
{
    private readonly Queue<int> _intQueue = new();
    private readonly Queue<long> _longQueue = new();
    private readonly TsDataType _dataType;
    private readonly bool _isLong;
    
    public ZigZagDecoder(TsDataType dataType)
    {
        _dataType = dataType;
        _isLong = dataType == TsDataType.Int64 || dataType == TsDataType.Timestamp;
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("ZigZag decoder does not support boolean values");
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        EnsureIntData(buffer, ref offset);
        return _intQueue.Dequeue();
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        EnsureLongData(buffer, ref offset);
        return _longQueue.Dequeue();
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("ZigZag decoder does not support float values");
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("ZigZag decoder does not support double values");
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("ZigZag decoder does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("ZigZag decoder does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        if (_isLong)
            return _longQueue.Count > 0 || offset < buffer.Length;
        return _intQueue.Count > 0 || offset < buffer.Length;
    }
    
    public void Reset()
    {
        _intQueue.Clear();
        _longQueue.Clear();
    }
    
    private void EnsureIntData(byte[] buffer, ref int offset)
    {
        if (_intQueue.Count > 0) return;
        
        // Read count
        int count = (int)ReadVarUInt(buffer, ref offset);
        
        // Read all values
        for (int i = 0; i < count; i++)
        {
            uint encoded = ReadVarUInt(buffer, ref offset);
            int decoded = DecodeZigZag32(encoded);
            _intQueue.Enqueue(decoded);
        }
    }
    
    private void EnsureLongData(byte[] buffer, ref int offset)
    {
        if (_longQueue.Count > 0) return;
        
        // Read count
        int count = (int)ReadVarUInt(buffer, ref offset);
        
        // Read all values
        for (int i = 0; i < count; i++)
        {
            ulong encoded = ReadVarULong(buffer, ref offset);
            long decoded = DecodeZigZag64(encoded);
            _longQueue.Enqueue(decoded);
        }
    }
    
    /// <summary>
    /// Decode unsigned int32 back to signed using ZigZag decoding.
    /// Formula: (n >>> 1) ^ -(n & 1)
    /// </summary>
    private static int DecodeZigZag32(uint n)
    {
        return (int)(n >> 1) ^ -(int)(n & 1);
    }
    
    /// <summary>
    /// Decode unsigned int64 back to signed using ZigZag decoding.
    /// Formula: (n >>> 1) ^ -(n & 1)
    /// </summary>
    private static long DecodeZigZag64(ulong n)
    {
        return (long)(n >> 1) ^ -(long)(n & 1);
    }
    
    private static uint ReadVarUInt(byte[] buffer, ref int offset)
    {
        uint result = 0;
        int shift = 0;
        
        while (true)
        {
            byte b = buffer[offset++];
            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        
        return result;
    }
    
    private static ulong ReadVarULong(byte[] buffer, ref int offset)
    {
        ulong result = 0;
        int shift = 0;
        
        while (true)
        {
            byte b = buffer[offset++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        
        return result;
    }
}
