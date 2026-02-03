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
/// DIFF decoder - First-order delta decoding (deprecated/legacy).
/// </summary>
public class DiffDecoder : IDecoder
{
    private readonly TsDataType _dataType;
    private bool _first = true;
    private int _previousInt;
    private long _previousLong;
    
    public DiffDecoder(TsDataType dataType)
    {
        _dataType = dataType;
    }
    
    public bool ReadBoolean(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("DIFF decoding does not support boolean values");
    }
    
    public int ReadInt(byte[] buffer, ref int offset)
    {
        if (_first)
        {
            _previousInt = ReadIntBigEndian(buffer, offset);
            offset += 4;
            _first = false;
            return _previousInt;
        }
        else
        {
            int delta = ReadIntBigEndian(buffer, offset);
            offset += 4;
            _previousInt += delta;
            return _previousInt;
        }
    }
    
    public long ReadLong(byte[] buffer, ref int offset)
    {
        if (_first)
        {
            _previousLong = ReadLongBigEndian(buffer, offset);
            offset += 8;
            _first = false;
            return _previousLong;
        }
        else
        {
            long delta = ReadLongBigEndian(buffer, offset);
            offset += 8;
            _previousLong += delta;
            return _previousLong;
        }
    }
    
    public float ReadFloat(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("DIFF decoding does not support float values");
    }
    
    public double ReadDouble(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("DIFF decoding does not support double values");
    }
    
    public string ReadString(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("DIFF decoding does not support string values");
    }
    
    public byte[] ReadBytes(byte[] buffer, ref int offset)
    {
        throw new NotSupportedException("DIFF decoding does not support byte array values");
    }
    
    public bool HasNext(byte[] buffer, int offset)
    {
        return offset < buffer.Length;
    }
    
    public void Reset()
    {
        _first = true;
    }
    
    private static int ReadIntBigEndian(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24) 
             | (buffer[offset + 1] << 16) 
             | (buffer[offset + 2] << 8) 
             | buffer[offset + 3];
    }
    
    private static long ReadLongBigEndian(byte[] buffer, int offset)
    {
        return ((long)buffer[offset] << 56)
             | ((long)buffer[offset + 1] << 48)
             | ((long)buffer[offset + 2] << 40)
             | ((long)buffer[offset + 3] << 32)
             | ((long)buffer[offset + 4] << 24)
             | ((long)buffer[offset + 5] << 16)
             | ((long)buffer[offset + 6] << 8)
             | buffer[offset + 7];
    }
}
