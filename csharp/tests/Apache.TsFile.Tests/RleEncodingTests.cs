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

public class RleEncodingTests
{
    [Fact]
    public void RleEncoder_BooleanRepeated_SuccessfulRoundTrip()
    {
        // Arrange - repeated boolean values
        var encoder = new RleEncoder(TsDataType.Boolean);
        var decoder = new RleDecoder(TsDataType.Boolean);
        var stream = new MemoryStream();
        
        var testData = new bool[] { true, true, true, true, true, true, true, true, false, false, false, false };
        
        // Act - Encode
        foreach (var value in testData)
        {
            encoder.Encode(value, stream);
        }
        encoder.Flush(stream);
        
        // Act - Decode
        var encoded = stream.ToArray();
        int offset = 0;
        var decoded = new List<bool>();
        for (int i = 0; i < testData.Length; i++)
        {
            decoded.Add(decoder.ReadBoolean(encoded, ref offset));
        }
        
        // Assert
        Assert.Equal(testData.Length, decoded.Count);
        for (int i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i], decoded[i]);
        }
    }
    
    [Fact]
    public void RleEncoder_Int32Repeated_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new RleEncoder(TsDataType.Int32);
        var decoder = new RleDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 100, 100, 100, 100, 100, 100, 100, 100, 200, 300, 400 };
        
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
    public void RleEncoder_Int64Repeated_SuccessfulRoundTrip()
    {
        // Arrange
        var encoder = new RleEncoder(TsDataType.Int64);
        var decoder = new RleDecoder(TsDataType.Int64);
        var stream = new MemoryStream();
        
        var testData = new long[] { 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 2000L, 3000L };
        
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
    public void RleEncoder_Int32Varied_SuccessfulRoundTrip()
    {
        // Arrange - varied values (triggers bit-packing)
        var encoder = new RleEncoder(TsDataType.Int32);
        var decoder = new RleDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        
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
    public void RleEncoder_Int32Mixed_SuccessfulRoundTrip()
    {
        // Arrange - mix of repeated and varied
        var encoder = new RleEncoder(TsDataType.Int32);
        var decoder = new RleDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] 
        { 
            5, 5, 5, 5, 5, 5, 5, 5,  // RLE run
            1, 2, 3, 4,                // Bit-packed
            10, 10, 10, 10, 10, 10, 10, 10, // RLE run
            7, 8, 9                    // Bit-packed
        };
        
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
    public void RleEncoder_Int32Negative_SuccessfulRoundTrip()
    {
        // Arrange - test negative values
        var encoder = new RleEncoder(TsDataType.Int32);
        var decoder = new RleDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new int[] { -100, -100, -100, -100, -100, -100, -100, -100, -50, -25 };
        
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
    public void RleEncoder_LargeDataset_SuccessfulRoundTrip()
    {
        // Arrange - large dataset
        var encoder = new RleEncoder(TsDataType.Int32);
        var decoder = new RleDecoder(TsDataType.Int32);
        var stream = new MemoryStream();
        
        var testData = new List<int>();
        // Add 1000 repeated values
        for (int i = 0; i < 1000; i++)
        {
            testData.Add(42);
        }
        // Add some varied values
        for (int i = 0; i < 100; i++)
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
        
        // Verify compression (should be much smaller than plain encoding)
        int plainSize = testData.Count * 4; // 4 bytes per int
        Assert.True(encoded.Length < plainSize * 0.5); // At least 50% compression
    }
    
    [Fact]
    public void RleEncoder_Factory_CreatesCorrectEncoder()
    {
        // Test that factory creates RLE encoder
        var encoder = EncoderFactory.CreateEncoder(TsEncoding.Rle, TsDataType.Int32);
        Assert.IsType<RleEncoder>(encoder);
        
        var decoder = DecoderFactory.CreateDecoder(TsEncoding.Rle, TsDataType.Int32);
        Assert.IsType<RleDecoder>(decoder);
    }
}
