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

using Apache.TsFile.Encoding;
using Apache.TsFile.Encoding.Encoder;
using Apache.TsFile.Encoding.Decoder;
using Apache.TsFile.Enums;
using Xunit;

namespace Apache.TsFile.Tests;

public class EncodingTests
{
    [Fact]
    public void PlainEncoder_Boolean_EncodesAndDecodes()
    {
        var encoder = new PlainEncoder();
        var decoder = new PlainDecoder();
        
        using var stream = new MemoryStream();
        
        encoder.Encode(true, stream);
        encoder.Encode(false, stream);
        encoder.Encode(true, stream);
        
        var data = stream.ToArray();
        int offset = 0;
        
        Assert.True(decoder.ReadBoolean(data, ref offset));
        Assert.False(decoder.ReadBoolean(data, ref offset));
        Assert.True(decoder.ReadBoolean(data, ref offset));
    }
    
    [Fact]
    public void PlainEncoder_Int32_EncodesAndDecodes()
    {
        var encoder = new PlainEncoder();
        var decoder = new PlainDecoder();
        
        using var stream = new MemoryStream();
        
        encoder.Encode(42, stream);
        encoder.Encode(-100, stream);
        encoder.Encode(2147483647, stream);
        
        var data = stream.ToArray();
        int offset = 0;
        
        Assert.Equal(42, decoder.ReadInt(data, ref offset));
        Assert.Equal(-100, decoder.ReadInt(data, ref offset));
        Assert.Equal(2147483647, decoder.ReadInt(data, ref offset));
    }
    
    [Fact]
    public void PlainEncoder_Int64_EncodesAndDecodes()
    {
        var encoder = new PlainEncoder();
        var decoder = new PlainDecoder();
        
        using var stream = new MemoryStream();
        
        encoder.Encode(9223372036854775807L, stream);
        encoder.Encode(-9223372036854775808L, stream);
        
        var data = stream.ToArray();
        int offset = 0;
        
        Assert.Equal(9223372036854775807L, decoder.ReadLong(data, ref offset));
        Assert.Equal(-9223372036854775808L, decoder.ReadLong(data, ref offset));
    }
    
    [Fact]
    public void PlainEncoder_Float_EncodesAndDecodes()
    {
        var encoder = new PlainEncoder();
        var decoder = new PlainDecoder();
        
        using var stream = new MemoryStream();
        
        encoder.Encode(3.14f, stream);
        encoder.Encode(-2.718f, stream);
        
        var data = stream.ToArray();
        int offset = 0;
        
        Assert.Equal(3.14f, decoder.ReadFloat(data, ref offset), 5);
        Assert.Equal(-2.718f, decoder.ReadFloat(data, ref offset), 5);
    }
    
    [Fact]
    public void PlainEncoder_Double_EncodesAndDecodes()
    {
        var encoder = new PlainEncoder();
        var decoder = new PlainDecoder();
        
        using var stream = new MemoryStream();
        
        encoder.Encode(3.141592653589793, stream);
        encoder.Encode(-2.718281828459045, stream);
        
        var data = stream.ToArray();
        int offset = 0;
        
        Assert.Equal(3.141592653589793, decoder.ReadDouble(data, ref offset), 10);
        Assert.Equal(-2.718281828459045, decoder.ReadDouble(data, ref offset), 10);
    }
    
    [Fact]
    public void PlainEncoder_String_EncodesAndDecodes()
    {
        var encoder = new PlainEncoder();
        var decoder = new PlainDecoder();
        
        using var stream = new MemoryStream();
        
        encoder.Encode("Hello, World!", stream);
        encoder.Encode("TSFile", stream);
        
        var data = stream.ToArray();
        int offset = 0;
        
        Assert.Equal("Hello, World!", decoder.ReadString(data, ref offset));
        Assert.Equal("TSFile", decoder.ReadString(data, ref offset));
    }
    
    [Fact]
    public void EncoderFactory_CreatesCorrectEncoder()
    {
        var encoder = EncoderFactory.CreateEncoder(TsEncoding.Plain, TsDataType.Int32);
        Assert.IsType<PlainEncoder>(encoder);
    }
    
    [Fact]
    public void DecoderFactory_CreatesCorrectDecoder()
    {
        var decoder = DecoderFactory.CreateDecoder(TsEncoding.Plain, TsDataType.Int32);
        Assert.IsType<PlainDecoder>(decoder);
    }
}
