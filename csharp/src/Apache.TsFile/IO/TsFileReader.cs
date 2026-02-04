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

using Apache.TsFile.Common;
using Apache.TsFile.Compress;
using Apache.TsFile.Encoding;
using Apache.TsFile.Enums;
using Apache.TsFile.Schema;

namespace Apache.TsFile.IO;

/// <summary>
/// Reader for reading TSFile format files.
/// </summary>
public class TsFileReader : IDisposable
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly BinaryReader _reader;
    private Dictionary<string, TableSchema>? _schemas;
    private long _metadataOffset;
    private byte _fileVersion;
    private bool _disposed;
    
    /// <summary>
    /// Initializes a new instance of the TsFileReader class.
    /// </summary>
    public TsFileReader(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
        
        if (!File.Exists(filePath))
            throw new FileNotFoundException("TSFile not found", filePath);
        
        _filePath = filePath;
        _fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        _reader = new BinaryReader(_fileStream);
        
        ValidateHeader();
        ReadMetadata();
    }
    
    /// <summary>
    /// Gets the available table schemas in this file.
    /// </summary>
    public IReadOnlyDictionary<string, TableSchema> Schemas => _schemas!;
    
    /// <summary>
    /// Queries data from the file.
    /// </summary>
    public QueryResult Query(string deviceName, string[]? measurements = null, 
        long? startTime = null, long? endTime = null)
    {
        if (!_schemas!.TryGetValue(deviceName, out var schema))
            throw new ArgumentException($"Device {deviceName} not found in file");
        
        var result = new QueryResult(deviceName, schema);
        
        // Read chunks for this device
        _fileStream.Position = TsFileConstants.MagicString.Length + 1; // Skip header
        
        while (_fileStream.Position < _metadataOffset)
        {
            var marker = _reader.ReadByte();
            if (marker != TsFileConstants.ChunkHeaderMarker)
                break;
            
            var chunkDeviceName = _reader.ReadString();
            
            if (chunkDeviceName == deviceName)
            {
                ReadChunk(result, schema, measurements, startTime, endTime);
            }
            else
            {
                // Skip this chunk
                SkipChunk(schema);
            }
        }
        
        return result;
    }
    
    /// <summary>
    /// Queries all data for a device.
    /// </summary>
    public Tablet QueryAll(string deviceName)
    {
        var result = Query(deviceName);
        return result.ToTablet();
    }
    
    private void ValidateHeader()
    {
        var magic = _reader.ReadBytes(TsFileConstants.MagicString.Length);
        
        if (!magic.SequenceEqual(TsFileConstants.MagicString))
            throw new InvalidDataException("Invalid TSFile magic string");
        
        _fileVersion = _reader.ReadByte();
        if (_fileVersion != TsFileConstants.Version && _fileVersion != TsFileConstants.JavaVersion4)
            throw new InvalidDataException($"Unsupported TSFile version: {_fileVersion}");
    }
    
    private void ReadMetadata()
    {
        if (_fileVersion == TsFileConstants.JavaVersion4)
        {
            ReadMetadataV4();
        }
        else
        {
            ReadMetadataV3();
        }
    }
    
    private void ReadMetadataV3()
    {
        // v3 format: [metadata_offset: 8 bytes][MAGIC: 6 bytes]
        _fileStream.Seek(-8 - TsFileConstants.MagicString.Length, SeekOrigin.End);
        _metadataOffset = _reader.ReadInt64();
        
        // Validate footer magic string
        var footerMagic = _reader.ReadBytes(TsFileConstants.MagicString.Length);
        if (!footerMagic.SequenceEqual(TsFileConstants.MagicString))
            throw new InvalidDataException("Invalid TSFile footer magic string");
        
        // Read schemas from metadata
        _fileStream.Position = _metadataOffset;
        var schemaCount = _reader.ReadInt32();
        
        _schemas = new Dictionary<string, TableSchema>();
        for (int i = 0; i < schemaCount; i++)
        {
            var schema = TableSchema.Deserialize(_reader);
            _schemas[schema.TableName] = schema;
        }
    }
    
    private void ReadMetadataV4()
    {
        // v4 format: [TsFileMetadata_size: 4 bytes][MAGIC: 6 bytes]
        // The metadata offset is stored inside TsFileMetadata
        
        // TODO: Full v4 support requires proper MetadataIndexNode deserialization
        // The current implementation is incomplete and may not work for all v4 files
        // See: https://github.com/apache/tsfile for v4 format specification
        
        throw new NotImplementedException(
            "TSFile v4 format is not yet fully supported in C# implementation. " +
            "V4 files use a complex metadata structure with recursive MetadataIndexNode trees " +
            "that require complete deserialization support. " +
            "For interoperability, please use v3 format files or implement full v4 support.");
        
        /* Partial implementation for reference:
        // Read TsFileMetadata size from footer
        _fileStream.Seek(-4 - TsFileConstants.MagicString.Length, SeekOrigin.End);
        var metadataSize = ReadInt32BigEndian(_reader);
        
        // Validate footer magic string
        var footerMagic = _reader.ReadBytes(TsFileConstants.MagicString.Length);
        if (!footerMagic.SequenceEqual(TsFileConstants.MagicString))
            throw new InvalidDataException("Invalid TSFile footer magic string");
        
        // Calculate position of TsFileMetadata start
        var metadataEndPos = _fileStream.Length - 4 - TsFileConstants.MagicString.Length;
        var metadataStartPos = metadataEndPos - metadataSize;
        
        // Read TsFileMetadata
        _fileStream.Position = metadataStartPos;
        ReadTsFileMetadataV4();
        */
    }
    
    private void ReadTsFileMetadataV4()
    {
        // Save starting position
        var startPos = _fileStream.Position;
        
        // Read table index node map (skip - we'll just read schemas)
        var tableIndexNodeNum = ReadVarInt();
        
        // Instead of parsing the complex MetadataIndexNode structure,
        // we'll skip to the table schemas section
        // by reading forward until we find the expected schema count
        
        // For a more robust solution, we'd need to properly parse MetadataIndexNode
        // For now, let's try a simpler approach: skip the index section and find schemas
        
        // Skip the entire metadata index tree structure
        // This is complex, so for basic v4 support, we'll use a heuristic:
        // The table schema section comes after the index nodes
        
        // Try to find table schemas by scanning forward
        // This is not ideal but works for basic interop
        bool foundSchemas = false;
        int maxScanBytes = 100000; // Safety limit
        int scanned = 0;
        
        // Reset to start and skip the index section more carefully
        _fileStream.Position = startPos;
        
        // Skip table index nodes - read them but don't store
        ReadVarInt(); // table count again
        for (int i = 0; i < tableIndexNodeNum; i++)
        {
            ReadVarIntString(); // table name
            // Skip the MetadataIndexNode by reading its serialized form
            SkipMetadataIndexNodeSimple();
        }
        
        // Now read table schemas
        var tableSchemaNum = ReadVarInt();
        _schemas = new Dictionary<string, TableSchema>();
        
        for (int i = 0; i < tableSchemaNum; i++)
        {
            var tableName = ReadVarIntString();
            var schema = ReadTableSchemaV4(tableName);
            _schemas[tableName] = schema;
        }
        
        // Read metadata offset (stored inside TsFileMetadata in v4)
        _metadataOffset = _reader.ReadInt64();
        
        // Skip bloom filter and properties for now
    }
    
    private void SkipMetadataIndexNodeSimple()
    {
        // MetadataIndexNode is a tree structure that can be recursive
        // For v4 basic support, we need to carefully skip it
        // Format: [entries][endOffset][nodeType]
        
        var childrenSize = ReadVarInt();
        
        for (int i = 0; i < childrenSize; i++)
        {
            // Each entry: [name][offset]
            ReadVarIntString(); // skip name  
            _reader.ReadInt64(); // skip offset (big-endian)
        }
        
        _reader.ReadInt64(); // skip endOffset
        _reader.ReadByte(); // skip nodeType
    }
    
    private TableSchema ReadTableSchemaV4(string tableName)
    {
        // Simplified v4 table schema reading
        // The actual format is complex, but for basic interop we can read device and measurement info
        
        var schema = new TableSchema(tableName);
        
        // Read columnSchemas list size
        var columnCount = ReadVarInt();
        
        for (int i = 0; i < columnCount; i++)
        {
            var columnName = ReadVarIntString();
            var dataType = (TsDataType)_reader.ReadByte();
            var encoding = (TsEncoding)_reader.ReadByte();
            var compression = (CompressionType)_reader.ReadByte();
            
            schema.AddMeasurement(new MeasurementSchema(columnName, dataType, encoding, compression));
        }
        
        return schema;
    }
    
    private void SkipMetadataIndexNode()
    {
        // Skip the MetadataIndexNode structure (legacy method - kept for reference)
        var childrenSize = ReadVarInt();
        for (int i = 0; i < childrenSize; i++)
        {
            ReadVarIntString(); // skip name
            _reader.ReadInt64(); // skip offset
        }
        _reader.ReadInt64(); // skip endOffset
        _reader.ReadByte(); // skip nodeType
    }
    
    private int ReadVarInt()
    {
        // Read variable-length integer (similar to ReadWriteForEncodingUtils.readUnsignedVarInt)
        int result = 0;
        int shift = 0;
        byte b;
        
        do
        {
            b = _reader.ReadByte();
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        
        return result;
    }
    
    private string ReadVarIntString()
    {
        // Read variable-length string (similar to ReadWriteIOUtils.readVarIntString)
        var length = ReadVarInt();
        var bytes = _reader.ReadBytes(length);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }
    
    private int ReadInt32BigEndian(BinaryReader reader)
    {
        var bytes = reader.ReadBytes(4);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt32(bytes, 0);
    }
    
    private void ReadChunk(QueryResult result, TableSchema schema, 
        string[]? measurements, long? startTime, long? endTime)
    {
        var measurementFilter = measurements?.ToHashSet() ?? null;
        
        // Read each measurement column
        for (int i = 0; i < schema.Measurements.Count; i++)
        {
            var measurementName = _reader.ReadString();
            var dataType = (TsDataType)_reader.ReadByte();
            var encoding = (TsEncoding)_reader.ReadByte();
            var compression = (CompressionType)_reader.ReadByte();
            
            var compressedSize = _reader.ReadInt32();
            var originalSize = _reader.ReadInt32();
            var compressedData = _reader.ReadBytes(compressedSize);
            
            // Decompress and decode if this measurement is requested
            if (measurementFilter == null || measurementFilter.Contains(measurementName))
            {
                var uncompressor = CompressorFactory.GetUncompressor(compression);
                var decodedData = uncompressor.Uncompress(compressedData);
                
                var decoder = DecoderFactory.CreateDecoder(encoding, dataType);
                var values = DecodeColumn(decoder, dataType, decodedData);
                
                result.AddMeasurementData(measurementName, values);
            }
        }
        
        // Read timestamps
        var timestampSize = _reader.ReadInt32();
        var timestampData = _reader.ReadBytes(timestampSize);
        var rowCount = _reader.ReadInt32();
        
        var timestamps = new long[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            timestamps[i] = ReadInt64BigEndian(timestampData, i * 8);
        }
        
        // Filter by time range if specified
        if (startTime.HasValue || endTime.HasValue)
        {
            var filteredTimestamps = new List<long>();
            var filteredIndices = new List<int>();
            
            for (int i = 0; i < timestamps.Length; i++)
            {
                if ((!startTime.HasValue || timestamps[i] >= startTime.Value) &&
                    (!endTime.HasValue || timestamps[i] <= endTime.Value))
                {
                    filteredTimestamps.Add(timestamps[i]);
                    filteredIndices.Add(i);
                }
            }
            
            result.AddTimestamps(filteredTimestamps.ToArray(), filteredIndices);
        }
        else
        {
            result.AddTimestamps(timestamps, null);
        }
    }
    
    private void SkipChunk(TableSchema schema)
    {
        for (int i = 0; i < schema.Measurements.Count; i++)
        {
            _reader.ReadString(); // measurement name
            _reader.ReadByte(); // data type
            _reader.ReadByte(); // encoding
            _reader.ReadByte(); // compression
            
            var compressedSize = _reader.ReadInt32();
            _reader.ReadInt32(); // original size
            _reader.ReadBytes(compressedSize); // skip data
        }
        
        var timestampSize = _reader.ReadInt32();
        _reader.ReadBytes(timestampSize); // skip timestamps
        _reader.ReadInt32(); // row count
    }
    
    private static List<object> DecodeColumn(IDecoder decoder, TsDataType dataType, byte[] data)
    {
        var values = new List<object>();
        int offset = 0;
        
        while (decoder.HasNext(data, offset))
        {
            object value = dataType switch
            {
                TsDataType.Boolean => decoder.ReadBoolean(data, ref offset),
                TsDataType.Int32 => decoder.ReadInt(data, ref offset),
                TsDataType.Int64 or TsDataType.Timestamp => decoder.ReadLong(data, ref offset),
                TsDataType.Float => decoder.ReadFloat(data, ref offset),
                TsDataType.Double => decoder.ReadDouble(data, ref offset),
                TsDataType.Text or TsDataType.String => decoder.ReadString(data, ref offset),
                _ => throw new NotSupportedException($"Data type {dataType} not supported")
            };
            
            values.Add(value);
        }
        
        return values;
    }
    
    private static long ReadInt64BigEndian(byte[] buffer, int offset)
    {
        return ((long)buffer[offset] << 56)
             | ((long)buffer[offset + 1] << 48)
             | ((long)buffer[offset + 2] << 40)
             | ((long)buffer[offset + 3] << 32)
             | ((long)buffer[offset + 4] << 24)
             | ((long)buffer[offset + 5] << 16)
             | ((long)buffer[offset + 6] << 8)
             | buffer[offset + 7];
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;
        
        _reader?.Dispose();
        _fileStream?.Dispose();
        
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Represents query results from a TSFile.
/// </summary>
public class QueryResult
{
    public string DeviceName { get; }
    public TableSchema Schema { get; }
    public List<long> Timestamps { get; }
    public Dictionary<string, List<object>> MeasurementData { get; }
    
    internal QueryResult(string deviceName, TableSchema schema)
    {
        DeviceName = deviceName;
        Schema = schema;
        Timestamps = new List<long>();
        MeasurementData = new Dictionary<string, List<object>>();
    }
    
    internal void AddTimestamps(long[] timestamps, List<int>? indices)
    {
        if (indices == null)
        {
            Timestamps.AddRange(timestamps);
        }
        else
        {
            foreach (var index in indices)
            {
                Timestamps.Add(timestamps[index]);
            }
        }
    }
    
    internal void AddMeasurementData(string measurement, List<object> values)
    {
        if (!MeasurementData.ContainsKey(measurement))
        {
            MeasurementData[measurement] = new List<object>();
        }
        
        MeasurementData[measurement].AddRange(values);
    }
    
    public Tablet ToTablet()
    {
        var tablet = new Tablet(DeviceName, Schema.Measurements, Timestamps.Count);
        
        for (int i = 0; i < Timestamps.Count; i++)
        {
            var values = new object[Schema.Measurements.Count];
            for (int j = 0; j < Schema.Measurements.Count; j++)
            {
                var measurementName = Schema.Measurements[j].MeasurementName;
                values[j] = MeasurementData[measurementName][i];
            }
            
            tablet.AddRow(Timestamps[i], values);
        }
        
        return tablet;
    }
}
