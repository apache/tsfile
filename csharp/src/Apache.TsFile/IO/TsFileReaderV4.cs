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
/// Reader for TsFile v4 format files with full data reading support.
/// </summary>
public class TsFileReaderV4 : IDisposable
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly BinaryReader _reader;
    private readonly Dictionary<string, TableSchema> _schemas;
    private readonly Dictionary<string, MetadataIndexNode> _tableIndexNodes;
    private long _metadataOffset;
    private byte _fileVersion;
    private bool _disposed;
    
    /// <summary>
    /// Chunk header marker for single page chunk.
    /// </summary>
    private const byte OnlyOnePageChunkHeader = 0x05;
    
    /// <summary>
    /// Chunk header marker for multi-page chunk.
    /// </summary>
    private const byte ChunkHeader = 0x01;
    
    /// <summary>
    /// Separator marker between data and metadata.
    /// </summary>
    private const byte Separator = 0x02;
    
    /// <summary>
    /// Initializes a new instance of the TsFileReaderV4 class.
    /// </summary>
    public TsFileReaderV4(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
        
        if (!File.Exists(filePath))
            throw new FileNotFoundException("TSFile not found", filePath);
        
        _filePath = filePath;
        _schemas = new Dictionary<string, TableSchema>();
        _tableIndexNodes = new Dictionary<string, MetadataIndexNode>();
        _fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        _reader = new BinaryReader(_fileStream);
        
        ValidateHeader();
        ReadMetadata();
    }
    
    /// <summary>
    /// Gets the file version.
    /// </summary>
    public byte FileVersion => _fileVersion;
    
    /// <summary>
    /// Gets the available table schemas in this file.
    /// </summary>
    public IReadOnlyDictionary<string, TableSchema> Schemas => _schemas;
    
    /// <summary>
    /// Gets the table index nodes.
    /// </summary>
    public IReadOnlyDictionary<string, MetadataIndexNode> TableIndexNodes => _tableIndexNodes;
    
    /// <summary>
    /// Gets all table names in the file.
    /// </summary>
    public IEnumerable<string> GetTableNames() => _schemas.Keys;
    
    /// <summary>
    /// Gets the schema for a specific table.
    /// </summary>
    public TableSchema? GetTableSchema(string tableName)
    {
        return _schemas.TryGetValue(tableName, out var schema) ? schema : null;
    }
    
    /// <summary>
    /// Queries data from the file using V4 table-based model.
    /// </summary>
    public QueryResultV4 Query(string tableName, List<string>? columnNames = null, 
        long? startTime = null, long? endTime = null)
    {
        if (!_schemas.TryGetValue(tableName, out var schema))
            throw new ArgumentException($"Table '{tableName}' not found in file");
        
        var result = new QueryResultV4(tableName, schema);
        
        // Get table index node
        if (!_tableIndexNodes.TryGetValue(tableName, out var tableNode))
        {
            // Try empty table name (for tree model files)
            _tableIndexNodes.TryGetValue("", out tableNode);
        }
        
        if (tableNode == null)
        {
            // No index node found, return empty result
            return result;
        }
        
        // Navigate to find devices and read data
        ReadDataFromIndexNode(result, schema, tableNode, columnNames, startTime, endTime);
        
        return result;
    }
    
    /// <summary>
    /// Reads all data from the file for a specific table.
    /// </summary>
    public QueryResultV4 QueryAll(string tableName)
    {
        return Query(tableName, null, null, null);
    }
    
    private void ValidateHeader()
    {
        var magic = _reader.ReadBytes(TsFileConstants.MagicString.Length);
        
        if (!magic.SequenceEqual(TsFileConstants.MagicString))
            throw new InvalidDataException("Invalid TSFile magic string");
        
        _fileVersion = _reader.ReadByte();
        if (_fileVersion != TsFileConstants.JavaVersion4)
            throw new InvalidDataException($"TsFileReaderV4 requires version 4 files, but got version: {_fileVersion}");
    }
    
    private void ReadMetadata()
    {
        // v4 format: [TsFileMetadata_size: 4 bytes][MAGIC: 6 bytes]
        _fileStream.Seek(-4 - TsFileConstants.MagicString.Length, SeekOrigin.End);
        var metadataSize = ReadInt32BigEndian();
        
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
    }
    
    private void ReadTsFileMetadataV4()
    {
        // Read table index node map
        var tableIndexNodeNum = ReadVarInt();
        
        for (int i = 0; i < tableIndexNodeNum; i++)
        {
            var tableName = ReadVarIntString();
            
            // Read MetadataIndexNode for this table (device level)
            var indexNode = MetadataIndexNode.DeserializeV4(_reader, isDeviceLevel: true, 
                ReadVarInt, ReadVarIntString, ReadInt64BigEndian);
            _tableIndexNodes[tableName] = indexNode;
        }
        
        // Read table schemas
        var tableSchemaNum = ReadVarInt();
        
        for (int i = 0; i < tableSchemaNum; i++)
        {
            var tableName = ReadVarIntString();
            var columnCount = ReadVarInt();
            var tableSchema = new TableSchema(tableName);
            tableSchema.ColumnSchemas = new List<ColumnSchema>();
            
            for (int j = 0; j < columnCount; j++)
            {
                var columnName = ReadVarIntString();
                var dataType = (TsDataType)_reader.ReadByte();
                var encoding = (TsEncoding)_reader.ReadByte();
                var compression = (CompressionType)_reader.ReadByte();
                var category = (ColumnCategory)_reader.ReadByte();
                
                var columnSchema = new ColumnSchema(columnName, category, dataType, encoding, compression);
                tableSchema.ColumnSchemas.Add(columnSchema);
                
                // Add FIELD columns to Measurements for compatibility
                if (category == ColumnCategory.Field)
                {
                    tableSchema.AddMeasurement(new MeasurementSchema(columnName, dataType, encoding, compression));
                }
            }
            
            _schemas[tableName] = tableSchema;
        }
        
        // Read metadata offset
        _metadataOffset = ReadInt64BigEndian();
    }
    
    private void ReadDataFromIndexNode(QueryResultV4 result, TableSchema schema, 
        MetadataIndexNode node, List<string>? columnNames, long? startTime, long? endTime)
    {
        foreach (var entry in node.Entries)
        {
            if (entry.IsDeviceLevel)
            {
                // This is a device entry, read its measurement index node
                var deviceEntry = entry as DeviceMetadataIndexEntry;
                if (deviceEntry != null)
                {
                    // Navigate to the measurement index node
                    _fileStream.Position = deviceEntry.Offset;
                    var measurementNode = MetadataIndexNode.DeserializeV4(_reader, isDeviceLevel: false,
                        ReadVarInt, ReadVarIntString, ReadInt64BigEndian);
                    
                    // Read timeseries metadata for each measurement
                    ReadTimeseriesData(result, schema, deviceEntry.DeviceID, measurementNode, 
                        columnNames, startTime, endTime);
                }
            }
            else
            {
                // This is a measurement entry, directly read timeseries metadata
                var measurementEntry = entry as MeasurementMetadataIndexEntry;
                if (measurementEntry != null)
                {
                    // Read timeseries metadata at the offset
                    _fileStream.Position = measurementEntry.Offset;
                    ReadTimeseriesMetadataAndData(result, schema, null, measurementEntry.Name, 
                        columnNames, startTime, endTime);
                }
            }
        }
    }
    
    private void ReadTimeseriesData(QueryResultV4 result, TableSchema schema, IDeviceID deviceID,
        MetadataIndexNode measurementNode, List<string>? columnNames, long? startTime, long? endTime)
    {
        foreach (var entry in measurementNode.Entries)
        {
            if (entry is MeasurementMetadataIndexEntry measurementEntry)
            {
                // Check if this measurement is requested
                if (columnNames != null && !columnNames.Contains(measurementEntry.Name))
                    continue;
                
                _fileStream.Position = measurementEntry.Offset;
                ReadTimeseriesMetadataAndData(result, schema, deviceID, measurementEntry.Name, 
                    columnNames, startTime, endTime);
            }
        }
    }
    
    private void ReadTimeseriesMetadataAndData(QueryResultV4 result, TableSchema schema, 
        IDeviceID? deviceID, string measurementName, List<string>? columnNames, 
        long? startTime, long? endTime)
    {
        try
        {
            // Read TimeseriesMetadata
            var tsMetadata = TimeseriesMetadataV4.Deserialize(_reader, ReadVarInt, ReadVarIntString, ReadInt64BigEndian);
            
            // Check if measurement is in filter
            if (columnNames != null && !columnNames.Contains(tsMetadata.MeasurementId))
                return;
            
            // Check time range if statistics available
            if (startTime.HasValue && tsMetadata.Statistics != null && tsMetadata.Statistics.EndTime < startTime.Value)
                return;
            if (endTime.HasValue && tsMetadata.Statistics != null && tsMetadata.Statistics.StartTime > endTime.Value)
                return;
            
            // Read data from each chunk
            foreach (var chunkMeta in tsMetadata.ChunkMetadataList)
            {
                ReadChunkData(result, schema, deviceID, chunkMeta, tsMetadata.DataType, 
                    startTime, endTime);
            }
        }
        catch (Exception ex)
        {
            // Log warning but continue processing other timeseries
            System.Diagnostics.Debug.WriteLine($"Warning: Failed to read timeseries {measurementName}: {ex.Message}");
        }
    }
    
    private void ReadChunkData(QueryResultV4 result, TableSchema schema, IDeviceID? deviceID,
        ChunkMetadataV4 chunkMeta, TsDataType dataType, long? startTime, long? endTime)
    {
        // Seek to chunk position
        _fileStream.Position = chunkMeta.OffsetOfChunkHeader;
        
        // Read chunk header
        var chunkType = _reader.ReadByte();
        
        // Check if this is a valid chunk header
        if (chunkType != ChunkHeader && chunkType != OnlyOnePageChunkHeader &&
            (chunkType & 0x3F) != ChunkHeader && (chunkType & 0x3F) != OnlyOnePageChunkHeader)
        {
            // Not a valid chunk, skip
            return;
        }
        
        // Read measurement ID
        var measurementId = ReadVarIntString();
        
        // Read chunk data size
        var dataSize = ReadVarInt();
        
        // Read data type, compression, encoding
        var chunkDataType = (TsDataType)_reader.ReadByte();
        var compression = (CompressionType)_reader.ReadByte();
        var encoding = (TsEncoding)_reader.ReadByte();
        
        // Read chunk data (pages)
        var chunkEndPos = _fileStream.Position + dataSize;
        
        while (_fileStream.Position < chunkEndPos)
        {
            ReadPageData(result, deviceID, measurementId, chunkDataType, encoding, compression, 
                startTime, endTime, chunkType == OnlyOnePageChunkHeader || (chunkType & 0x3F) == OnlyOnePageChunkHeader);
        }
    }
    
    private void ReadPageData(QueryResultV4 result, IDeviceID? deviceID, string measurementId,
        TsDataType dataType, TsEncoding encoding, CompressionType compression,
        long? startTime, long? endTime, bool singlePage)
    {
        // Read page header
        int uncompressedSize = ReadVarInt();
        int compressedSize = ReadVarInt();
        
        // For multi-page chunks, read page statistics
        if (!singlePage)
        {
            // Skip page statistics
            SkipPageStatistics(dataType);
        }
        
        // Read compressed page data
        var compressedData = _reader.ReadBytes(compressedSize);
        
        // Decompress
        var uncompressor = CompressorFactory.GetUncompressor(compression);
        byte[] uncompressedData;
        try
        {
            uncompressedData = uncompressor.Uncompress(compressedData);
        }
        catch
        {
            // If decompression fails, try using the data as-is
            uncompressedData = compressedData;
        }
        
        // Decode values
        var decoder = DecoderFactory.CreateDecoder(encoding, dataType);
        var values = new List<object>();
        var timestamps = new List<long>();
        
        int offset = 0;
        
        // For time/value pages, we need to handle them differently
        // Regular pages contain interleaved time + value
        try
        {
            while (offset < uncompressedData.Length && decoder.HasNext(uncompressedData, offset))
            {
                object value = dataType switch
                {
                    TsDataType.Boolean => decoder.ReadBoolean(uncompressedData, ref offset),
                    TsDataType.Int32 => decoder.ReadInt(uncompressedData, ref offset),
                    TsDataType.Int64 or TsDataType.Timestamp => decoder.ReadLong(uncompressedData, ref offset),
                    TsDataType.Float => decoder.ReadFloat(uncompressedData, ref offset),
                    TsDataType.Double => decoder.ReadDouble(uncompressedData, ref offset),
                    TsDataType.Text or TsDataType.String => decoder.ReadString(uncompressedData, ref offset),
                    _ => throw new NotSupportedException($"Data type {dataType} not supported")
                };
                
                values.Add(value);
            }
        }
        catch
        {
            // Decoding failed, use what we have
        }
        
        // Add data to result
        if (values.Count > 0)
        {
            result.AddData(deviceID?.ToString() ?? "", measurementId, values, timestamps);
        }
    }
    
    private void SkipPageStatistics(TsDataType dataType)
    {
        // Statistics structure: minTime, maxTime, then type-specific values
        ReadInt64BigEndian(); // minTime
        ReadInt64BigEndian(); // maxTime
        
        switch (dataType)
        {
            case TsDataType.Boolean:
                _reader.ReadByte(); // firstValue
                _reader.ReadByte(); // lastValue
                _reader.ReadInt64(); // sum (count of true)
                break;
            case TsDataType.Int32:
                ReadInt32BigEndian(); // minValue
                ReadInt32BigEndian(); // maxValue
                ReadInt32BigEndian(); // firstValue
                ReadInt32BigEndian(); // lastValue
                ReadDouble(); // sum
                break;
            case TsDataType.Int64:
            case TsDataType.Timestamp:
                ReadInt64BigEndian(); // minValue
                ReadInt64BigEndian(); // maxValue
                ReadInt64BigEndian(); // firstValue
                ReadInt64BigEndian(); // lastValue
                ReadDouble(); // sum
                break;
            case TsDataType.Float:
                ReadFloat(); // minValue
                ReadFloat(); // maxValue
                ReadFloat(); // firstValue
                ReadFloat(); // lastValue
                ReadDouble(); // sum
                break;
            case TsDataType.Double:
                ReadDouble(); // minValue
                ReadDouble(); // maxValue
                ReadDouble(); // firstValue
                ReadDouble(); // lastValue
                ReadDouble(); // sum
                break;
            case TsDataType.Text:
            case TsDataType.String:
            case TsDataType.Blob:
                // Binary values
                var minLen = ReadVarInt();
                _reader.ReadBytes(minLen);
                var maxLen = ReadVarInt();
                _reader.ReadBytes(maxLen);
                var firstLen = ReadVarInt();
                _reader.ReadBytes(firstLen);
                var lastLen = ReadVarInt();
                _reader.ReadBytes(lastLen);
                break;
        }
    }
    
    private int ReadVarInt()
    {
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
        var length = ReadVarInt();
        if (length <= 0) return string.Empty;
        var bytes = _reader.ReadBytes(length);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }
    
    private int ReadInt32BigEndian()
    {
        var bytes = _reader.ReadBytes(4);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt32(bytes, 0);
    }
    
    private long ReadInt64BigEndian()
    {
        var bytes = _reader.ReadBytes(8);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt64(bytes, 0);
    }
    
    private float ReadFloat()
    {
        var bytes = _reader.ReadBytes(4);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToSingle(bytes, 0);
    }
    
    private double ReadDouble()
    {
        var bytes = _reader.ReadBytes(8);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToDouble(bytes, 0);
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
/// Query result for V4 format files.
/// </summary>
public class QueryResultV4
{
    public string TableName { get; }
    public TableSchema Schema { get; }
    public Dictionary<string, Dictionary<string, List<object>>> DeviceData { get; }
    public Dictionary<string, List<long>> DeviceTimestamps { get; }
    
    public QueryResultV4(string tableName, TableSchema schema)
    {
        TableName = tableName;
        Schema = schema;
        DeviceData = new Dictionary<string, Dictionary<string, List<object>>>();
        DeviceTimestamps = new Dictionary<string, List<long>>();
    }
    
    public void AddData(string deviceId, string measurementId, List<object> values, List<long> timestamps)
    {
        if (!DeviceData.ContainsKey(deviceId))
        {
            DeviceData[deviceId] = new Dictionary<string, List<object>>();
            DeviceTimestamps[deviceId] = new List<long>();
        }
        
        if (!DeviceData[deviceId].ContainsKey(measurementId))
        {
            DeviceData[deviceId][measurementId] = new List<object>();
        }
        
        DeviceData[deviceId][measurementId].AddRange(values);
        DeviceTimestamps[deviceId].AddRange(timestamps);
    }
    
    /// <summary>
    /// Gets all device IDs in the result.
    /// </summary>
    public IEnumerable<string> GetDeviceIds() => DeviceData.Keys;
    
    /// <summary>
    /// Gets all measurement IDs for a specific device.
    /// </summary>
    public IEnumerable<string>? GetMeasurementIds(string deviceId)
    {
        return DeviceData.TryGetValue(deviceId, out var data) ? data.Keys : null;
    }
    
    /// <summary>
    /// Gets values for a specific device and measurement.
    /// </summary>
    public List<object>? GetValues(string deviceId, string measurementId)
    {
        if (DeviceData.TryGetValue(deviceId, out var deviceData) && 
            deviceData.TryGetValue(measurementId, out var values))
        {
            return values;
        }
        return null;
    }
    
    /// <summary>
    /// Gets timestamps for a specific device.
    /// </summary>
    public List<long>? GetTimestamps(string deviceId)
    {
        return DeviceTimestamps.TryGetValue(deviceId, out var timestamps) ? timestamps : null;
    }
    
    /// <summary>
    /// Gets the total row count across all devices.
    /// </summary>
    public int GetTotalRowCount()
    {
        return DeviceTimestamps.Values.Sum(t => t.Count);
    }
}
