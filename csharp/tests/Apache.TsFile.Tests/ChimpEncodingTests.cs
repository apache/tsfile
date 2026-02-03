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

public class ChimpEncodingTests
{
    [Fact]
    public void ChimpEncoder_Int32Similar_SuccessfulRoundTrip()
    {
        var encoder = new ChimpEncoder(TsDataType.Int32);
        var decoder = new ChimpDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 1000, 1001, 1002, 1001, 1000, 999, 1000, 1001 };
        
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
    public void ChimpEncoder_Int64Timestamp_SuccessfulRoundTrip()
    {
        var encoder = new ChimpEncoder(TsDataType.Int64);
        var decoder = new ChimpDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 1000000L, 1000100L, 1000200L, 1000150L, 1000100L };
        
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
    public void ChimpEncoder_FloatSimilar_SuccessfulRoundTrip()
    {
        var encoder = new ChimpEncoder(TsDataType.Float);
        var decoder = new ChimpDecoder(TsDataType.Float);
        var stream = new MemoryStream();
        
        var testData = new float[] { 23.5f, 23.6f, 23.7f, 23.6f, 23.5f, 23.4f, 23.5f };
        
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
    public void ChimpEncoder_DoubleSimilar_SuccessfulRoundTrip()
    {
        var encoder = new ChimpEncoder(TsDataType.Double);
        var decoder = new ChimpDecoder(TsDataType.Double);
        var stream = new MemoryStream();
        
        var testData = new double[] { 100.1, 100.2, 100.3, 100.2, 100.1, 100.0, 100.1 };
        
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
    public void ChimpEncoder_FloatConstant_SuccessfulRoundTrip()
    {
        var encoder = new ChimpEncoder(TsDataType.Float);
        var decoder = new ChimpDecoder(TsDataType.Float);
        var stream = new MemoryStream();
        
        var testData = new float[] { 25.0f, 25.0f, 25.0f, 25.0f, 25.0f };
        
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
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void ChimpEncoder_LargeDataset_SuccessfulRoundTrip()
    {
        var encoder = new ChimpEncoder(TsDataType.Float);
        var decoder = new ChimpDecoder(TsDataType.Float);
        var stream = new MemoryStream();
        
        var testData = new List<float>();
        float baseValue = 20.0f;
        for (int i = 0; i < 100; i++)
        {
            testData.Add(baseValue + (float)Math.Sin(i * 0.1) * 2.0f);
        }
        
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<float>();
        for (int i = 0; i < testData.Count; i++)
        {
            decoded.Add(decoder.ReadFloat(encoded, ref offset));
        }
        
        Assert.Equal(testData.Count, decoded.Count);
        for (int i = 0; i < testData.Count; i++)
        {
            Assert.Equal(testData[i], decoded[i], 5);
        }
        
        int plainSize = testData.Count * 4;
        Assert.True(encoded.Length <= plainSize);
    }
    
    [Fact]
    public void ChimpEncoder_Factory_CreatesCorrectEncoder()
    {
        var encoder = EncoderFactory.CreateEncoder(TsEncoding.Chimp, TsDataType.Float);
        Assert.IsType<ChimpEncoder>(encoder);
        
        var decoder = DecoderFactory.CreateDecoder(TsEncoding.Chimp, TsDataType.Float);
        Assert.IsType<ChimpDecoder>(decoder);
    }
}
