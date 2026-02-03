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

public class SprintzEncodingTests
{
    [Fact]
    public void SprintzEncoder_Int32Sequence_SuccessfulRoundTrip()
    {
        var encoder = new SprintzEncoder(TsDataType.Int32);
        var decoder = new SprintzDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008 };
        
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
    public void SprintzEncoder_Int64Sequence_SuccessfulRoundTrip()
    {
        var encoder = new SprintzEncoder(TsDataType.Int64);
        var decoder = new SprintzDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 1000000L, 1000100L, 1000200L, 1000300L, 1000400L };
        
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
    public void SprintzEncoder_FloatSequence_SuccessfulRoundTrip()
    {
        var encoder = new SprintzEncoder(TsDataType.Float);
        var decoder = new SprintzDecoder(TsDataType.Float);
        var stream = new MemoryStream();
        
        var testData = new float[] { 20.0f, 20.1f, 20.2f, 20.3f, 20.4f, 20.5f };
        
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
    public void SprintzEncoder_DoubleSequence_SuccessfulRoundTrip()
    {
        var encoder = new SprintzEncoder(TsDataType.Double);
        var decoder = new SprintzDecoder(TsDataType.Double);
        var stream = new MemoryStream();
        
        var testData = new double[] { 100.0, 100.1, 100.2, 100.3, 100.4 };
        
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
    public void SprintzEncoder_LargeSequence_SuccessfulRoundTrip()
    {
        var encoder = new SprintzEncoder(TsDataType.Int32);
        var decoder = new SprintzDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new List<int>();
        for (int i = 0; i < 50; i++)
        {
            testData.Add(1000 + i);
        }
        
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<int>();
        for (int i = 0; i < testData.Count; i++)
        {
            decoded.Add(decoder.ReadInt(encoded, ref offset));
        }
        
        Assert.Equal(testData.Count, decoded.Count);
        for (int i = 0; i < testData.Count; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
        
        int plainSize = testData.Count * 4;
        Assert.True(encoded.Length <= plainSize);
    }
    
    [Fact]
    public void SprintzEncoder_Factory_CreatesCorrectEncoder()
    {
        var encoder = EncoderFactory.CreateEncoder(TsEncoding.Sprintz, TsDataType.Int32);
        Assert.IsType<SprintzEncoder>(encoder);
        
        var decoder = DecoderFactory.CreateDecoder(TsEncoding.Sprintz, TsDataType.Int32);
        Assert.IsType<SprintzDecoder>(decoder);
    }
}
