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

public class ZigZagEncodingTests
{
    [Fact]
    public void ZigZagEncoder_Int32Positive_SuccessfulRoundTrip()
    {
        // Arrange - positive small integers
        var encoder = new ZigZagEncoder(TsDataType.Int32);
        var decoder = new ZigZagDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 0, 1, 2, 3, 4, 5, 10, 100, 1000 };
        
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
    public void ZigZagEncoder_Int32Negative_SuccessfulRoundTrip()
    {
        // Arrange - negative small integers
        var encoder = new ZigZagEncoder(TsDataType.Int32);
        var decoder = new ZigZagDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { -1, -2, -3, -4, -5, -10, -100, -1000 };
        
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
    public void ZigZagEncoder_Int32Mixed_SuccessfulRoundTrip()
    {
        // Arrange - mixed positive and negative
        var encoder = new ZigZagEncoder(TsDataType.Int32);
        var decoder = new ZigZagDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 0, -1, 1, -2, 2, -3, 3, -100, 100, -1000, 1000 };
        
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
    public void ZigZagEncoder_Int64Positive_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new ZigZagEncoder(TsDataType.Int64);
        var decoder = new ZigZagDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 0L, 1L, 2L, 100L, 1000L, 10000L, 1000000L };
        
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
    public void ZigZagEncoder_Int64Negative_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new ZigZagEncoder(TsDataType.Int64);
        var decoder = new ZigZagDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { -1L, -2L, -100L, -1000L, -10000L, -1000000L };
        
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
    public void ZigZagEncoder_Int64Mixed_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new ZigZagEncoder(TsDataType.Int64);
        var decoder = new ZigZagDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 0L, -1L, 1L, -100L, 100L, -10000L, 10000L };
        
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
    public void ZigZagEncoder_LargeDataset_SuccessfulRoundTrip()
    {
        // Arrange - large dataset with varied values
        var encoder = new ZigZagEncoder(TsDataType.Int32);
        var decoder = new ZigZagDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new List<int>();
        for (int i = -500; i <= 500; i++)
        {
            testData.Add(i);
        }
        
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
        for (int i = 0; i < testData.Count; i++)
        {
            decoded.Add(decoder.ReadInt(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Count, decoded.Count);
        for (int i = 0; i < testData.Count; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
        
        // Verify compression (should be better than plain for small values)
        int plainSize = testData.Count * 4; // 4 bytes per int
        Assert.True(encoded.Length < plainSize); // Should compress
    }
    
    [Fact]
    public void ZigZagEncoder_ExtremeValues_SuccessfulRoundTrip()
    {
        // Arrange - extreme values
        var encoder = new ZigZagEncoder(TsDataType.Int32);
        var decoder = new ZigZagDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { int.MinValue, int.MaxValue, 0, -1, 1 };
        
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
    public void ZigZagEncoder_Factory_CreatesCorrectEncoder()
    {
        // Test that factory creates ZigZag encoder
        var encoder = EncoderFactory.CreateEncoder(TsEncoding.ZigZag, TsDataType.Int32);
        Assert.IsType<ZigZagEncoder>(encoder);
        
        var decoder = DecoderFactory.CreateDecoder(TsEncoding.ZigZag, TsDataType.Int32);
        Assert.IsType<ZigZagDecoder>(decoder);
    }
}
