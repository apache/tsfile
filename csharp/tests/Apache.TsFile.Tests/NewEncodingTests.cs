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

public class NewEncodingTests
{
    [Fact]
    public void GorillaV1Encoder_Float_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new GorillaV1Encoder(TsDataType.Float);
        var decoder = new GorillaV1Decoder(TsDataType.Float);
        var stream = new MemoryStream();
        
        var testData = new float[] { 23.5f, 23.6f, 23.7f, 23.6f, 23.5f };
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<float>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadFloat(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i], 5);
        }
    }
    
    [Fact]
    public void GorillaV1Encoder_Double_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new GorillaV1Encoder(TsDataType.Double);
        var decoder = new GorillaV1Decoder(TsDataType.Double);
        var stream = new MemoryStream();
        
        var testData = new double[] { 100.1, 100.2, 100.3, 100.2, 100.1 };
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<double>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadDouble(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i], 10);
        }
    }
    
    [Fact]
    public void BitmapEncoder_Int_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new BitmapEncoder();
        var decoder = new BitmapDecoder();
        var stream = new MemoryStream();
        
        var testData = new int[] { 1, 2, 1, 3, 2, 1, 3, 2 };
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<int>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadInt(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void RegularEncoder_Int_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new RegularEncoder(TsDataType.Int32);
        var decoder = new RegularDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 100, 110, 120, 130, 140 }; // Regular pattern
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<int>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadInt(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void RegularEncoder_Long_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new RegularEncoder(TsDataType.Int64);
        var decoder = new RegularDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 1000L, 2000L, 3000L, 4000L, 5000L };
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<long>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadLong(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void DiffEncoder_Int_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new DiffEncoder(TsDataType.Int32);
        var decoder = new DiffDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 100, 105, 110, 108, 112 };
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<int>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadInt(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void DiffEncoder_Long_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new DiffEncoder(TsDataType.Int64);
        var decoder = new DiffDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 1000L, 1100L, 1150L, 1200L };
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<long>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadLong(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void EncoderFactory_CreatesCorrectEncoders()
    {
        // Test that factory creates the correct encoder types
        Assert.IsType<GorillaV1Encoder>(EncoderFactory.CreateEncoder(TsEncoding.GorillaV1, TsDataType.Float));
        Assert.IsType<BitmapEncoder>(EncoderFactory.CreateEncoder(TsEncoding.Bitmap, TsDataType.Int32));
        Assert.IsType<RegularEncoder>(EncoderFactory.CreateEncoder(TsEncoding.Regular, TsDataType.Int32));
        Assert.IsType<DiffEncoder>(EncoderFactory.CreateEncoder(TsEncoding.Diff, TsDataType.Int32));
        Assert.IsType<ChimpEncoder>(EncoderFactory.CreateEncoder(TsEncoding.Chimp, TsDataType.Int32));
        Assert.IsType<SprintzEncoder>(EncoderFactory.CreateEncoder(TsEncoding.Sprintz, TsDataType.Int32));
        Assert.IsType<RlbeEncoder>(EncoderFactory.CreateEncoder(TsEncoding.Rlbe, TsDataType.Int32));
        Assert.IsType<PlainEncoder>(EncoderFactory.CreateEncoder(TsEncoding.Freq, TsDataType.Int32));
    }
    
    [Fact]
    public void DecoderFactory_CreatesCorrectDecoders()
    {
        // Test that factory creates the correct decoder types
        Assert.IsType<GorillaV1Decoder>(DecoderFactory.CreateDecoder(TsEncoding.GorillaV1, TsDataType.Float));
        Assert.IsType<BitmapDecoder>(DecoderFactory.CreateDecoder(TsEncoding.Bitmap, TsDataType.Int32));
        Assert.IsType<RegularDecoder>(DecoderFactory.CreateDecoder(TsEncoding.Regular, TsDataType.Int32));
        Assert.IsType<DiffDecoder>(DecoderFactory.CreateDecoder(TsEncoding.Diff, TsDataType.Int32));
        Assert.IsType<ChimpDecoder>(DecoderFactory.CreateDecoder(TsEncoding.Chimp, TsDataType.Int32));
        Assert.IsType<SprintzDecoder>(DecoderFactory.CreateDecoder(TsEncoding.Sprintz, TsDataType.Int32));
        Assert.IsType<RlbeDecoder>(DecoderFactory.CreateDecoder(TsEncoding.Rlbe, TsDataType.Int32));
        Assert.IsType<PlainDecoder>(DecoderFactory.CreateDecoder(TsEncoding.Freq, TsDataType.Int32));
    }
}
