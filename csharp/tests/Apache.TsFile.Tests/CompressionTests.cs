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

using Apache.TsFile.Compress;
using Apache.TsFile.Enums;
using Xunit;

namespace Apache.TsFile.Tests;

public class CompressionTests
{
    private readonly byte[] _testData = System.Text.Encoding.UTF8.GetBytes(
        "This is a test string that should be compressed efficiently. " +
        "This is a test string that should be compressed efficiently. " +
        "This is a test string that should be compressed efficiently.");
    
    [Fact]
    public void UnCompressor_CompressAndUncompress_ReturnsOriginalData()
    {
        var compressor = new UnCompressor();
        
        var compressed = compressor.Compress(_testData);
        var uncompressed = compressor.Uncompress(compressed);
        
        Assert.Equal(_testData, uncompressed);
    }
    
    [Fact]
    public void GzipCompressor_CompressAndUncompress_ReturnsOriginalData()
    {
        var compressor = new GzipCompressor();
        
        var compressed = compressor.Compress(_testData);
        var uncompressed = compressor.Uncompress(compressed);
        
        Assert.Equal(_testData, uncompressed);
        Assert.True(compressed.Length < _testData.Length);
    }
    
    [Fact]
    public void SnappyCompressor_CompressAndUncompress_ReturnsOriginalData()
    {
        var compressor = new SnappyCompressor();
        
        var compressed = compressor.Compress(_testData);
        var uncompressed = compressor.Uncompress(compressed);
        
        Assert.Equal(_testData, uncompressed);
    }
    
    [Fact]
    public void Lz4Compressor_CompressAndUncompress_ReturnsOriginalData()
    {
        var compressor = new Lz4Compressor();
        
        var compressed = compressor.Compress(_testData);
        var uncompressed = compressor.Uncompress(compressed);
        
        Assert.Equal(_testData, uncompressed);
    }
    
    [Fact]
    public void ZstdCompressor_CompressAndUncompress_ReturnsOriginalData()
    {
        var compressor = new ZstdCompressor();
        
        var compressed = compressor.Compress(_testData);
        var uncompressed = compressor.Uncompress(compressed);
        
        Assert.Equal(_testData, uncompressed);
    }
    
    [Fact]
    public void Lzma2Compressor_ThrowsNotImplemented()
    {
        var compressor = new Lzma2Compressor();
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        
        Assert.Throws<NotImplementedException>(() => compressor.Compress(testData));
        Assert.Throws<NotImplementedException>(() => compressor.Uncompress(testData));
    }
    
    [Fact]
    public void CompressorFactory_CreatesCorrectCompressor()
    {
        var gzipCompressor = CompressorFactory.GetCompressor(CompressionType.Gzip);
        Assert.IsType<GzipCompressor>(gzipCompressor);
        
        var snappyCompressor = CompressorFactory.GetCompressor(CompressionType.Snappy);
        Assert.IsType<SnappyCompressor>(snappyCompressor);
        
        var lz4Compressor = CompressorFactory.GetCompressor(CompressionType.Lz4);
        Assert.IsType<Lz4Compressor>(lz4Compressor);
        
        var zstdCompressor = CompressorFactory.GetCompressor(CompressionType.Zstd);
        Assert.IsType<ZstdCompressor>(zstdCompressor);
    }
}
