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

public class RlbeEncodingTests
{
    [Fact]
    public void RlbeEncoder_Int32Sequence_SuccessfulRoundTrip()
    {
        var encoder = new RlbeEncoder(TsDataType.Int32);
        var decoder = new RlbeDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 1000, 1001, 1002, 1003, 1004, 1005 };
        
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<int>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadInt(encoded, ref offset));
        }
        
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void RlbeEncoder_Int64Sequence_SuccessfulRoundTrip()
    {
        var encoder = new RlbeEncoder(TsDataType.Int64);
        var decoder = new RlbeDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 1000000L, 1000100L, 1000200L, 1000300L };
        
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<long>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadLong(encoded, ref offset));
        }
        
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void RlbeEncoder_FloatSequence_SuccessfulRoundTrip()
    {
        var encoder = new RlbeEncoder(TsDataType.Float);
        var decoder = new RlbeDecoder(TsDataType.Float);
        var stream = new MemoryStream();
        
        var testData = new float[] { 20.0f, 20.1f, 20.2f, 20.3f, 20.4f };
        
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<float>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadFloat(encoded, ref offset));
        }
        
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i], 5);
        }
    }
    
    [Fact]
    public void RlbeEncoder_DoubleSequence_SuccessfulRoundTrip()
    {
        var encoder = new RlbeEncoder(TsDataType.Double);
        var decoder = new RlbeDecoder(TsDataType.Double);
        var stream = new MemoryStream();
        
        var testData = new double[] { 100.0, 100.1, 100.2, 100.3 };
        
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<double>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadDouble(encoded, ref offset));
        }
        
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i], 10);
        }
    }
    
    [Fact]
    public void RlbeEncoder_RepeatingValues_SuccessfulRoundTrip()
    {
        var encoder = new RlbeEncoder(TsDataType.Int32);
        var decoder = new RlbeDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 100, 101, 101, 101, 102, 102, 103 };
        
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<int>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadInt(encoded, ref offset));
        }
        
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void RlbeEncoder_Factory_CreatesCorrectEncoder()
    {
        var encoder = EncoderFactory.CreateEncoder(TsEncoding.Rlbe, TsDataType.Int32);
        Assert.IsType<RlbeEncoder>(encoder);
        
        var decoder = DecoderFactory.CreateDecoder(TsEncoding.Rlbe, TsDataType.Int32);
        Assert.IsType<RlbeDecoder>(decoder);
    }
}
