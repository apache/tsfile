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
using System.Buffers.Binary;

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// DIFF encoder - First-order delta encoding (deprecated/legacy).
/// </summary>
public class DiffEncoder : IEncoder
{
    private readonly TsDataType _dataType;
    private bool _first = true;
    private int _previousInt;
    private long _previousLong;
    
    public DiffEncoder(TsDataType dataType)
    {
        _dataType = dataType;
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        throw new NotSupportedException("DIFF encoding does not support boolean values");
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        if (_first)
        {
            WriteInt(stream, value);
            _previousInt = value;
            _first = false;
        }
        else
        {
            int delta = value - _previousInt;
            WriteInt(stream, delta);
            _previousInt = value;
        }
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        if (_first)
        {
            WriteLong(stream, value);
            _previousLong = value;
            _first = false;
        }
        else
        {
            long delta = value - _previousLong;
            WriteLong(stream, delta);
            _previousLong = value;
        }
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        throw new NotSupportedException("DIFF encoding does not support float values");
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        throw new NotSupportedException("DIFF encoding does not support double values");
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("DIFF encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("DIFF encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        _first = true;
    }
    
    public int GetOneItemMaxSize()
    {
        return _dataType == TsDataType.Int32 ? 4 : 8;
    }
    
    public long GetMaxByteSize()
    {
        return 1000;
    }
    
    private static void WriteInt(MemoryStream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        stream.Write(bytes);
    }
    
    private static void WriteLong(MemoryStream stream, long value)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        stream.Write(bytes);
    }
}
