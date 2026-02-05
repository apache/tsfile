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

namespace Apache.TsFile.Common;

/// <summary>
/// TSFile format constants.
/// </summary>
public static class TsFileConstants
{
    /// <summary>
    /// Magic string for TSFile format (6 bytes): "TsFile"
    /// </summary>
    public static readonly byte[] MagicString = { 0x54, 0x73, 0x46, 0x69, 0x6C, 0x65 };
    
    /// <summary>
    /// Current version number (version this implementation writes)
    /// </summary>
    public const byte Version = 3;

    /// <summary>
    /// Default file version for new files (V4 is the default)
    /// </summary>
    public const byte DefaultFileVersion = 4;

    /// <summary>
    /// Java TSFile version 4 (temporary compatibility for reading Java files)
    /// </summary>
    public const byte JavaVersion4 = 4;
    
    /// <summary>
    /// Default chunk size threshold (128MB)
    /// </summary>
    public const int DefaultChunkSize = 128 * 1024 * 1024;
    
    /// <summary>
    /// Default page size (64KB)
    /// </summary>
    public const int DefaultPageSize = 64 * 1024;
    
    /// <summary>
    /// Marker byte for chunk header
    /// </summary>
    public const byte ChunkHeaderMarker = 0x01;
    
    /// <summary>
    /// Marker byte for chunk group footer
    /// </summary>
    public const byte ChunkGroupFooterMarker = 0x00;
    
    /// <summary>
    /// Separator for measurement names
    /// </summary>
    public const char PathSeparator = '.';
    
    /// <summary>
    /// Maximum expected bloom filter size for v4 parsing (1 MB)
    /// </summary>
    public const int MaxBloomFilterSize = 1_000_000;
    
    /// <summary>
    /// Maximum expected properties count for v4 parsing
    /// </summary>
    public const int MaxPropertiesCount = 100;
    
    /// <summary>
    /// Default skip size for unknown device ID structures in v4
    /// </summary>
    public const int DeviceIdSkipSize = 20;
}
