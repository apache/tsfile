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
    /// Gets the file version (3 or 4).
    /// </summary>
    public byte FileVersion => _fileVersion;

    /// <summary>
    /// Gets the available table schemas in this file.
    /// </summary>
    public IReadOnlyDictionary<string, TableSchema> Schemas => _schemas!;
    
    /// <summary>
    /// Queries data from the file.
    /// </summary>
    /// <remarks>
    /// For v4 files: This method currently has limited support. V4 files use a table-based
    /// model that requires metadata index tree traversal to locate chunks. Full v4 query
    /// support requires complete implementation of MetadataIndexNode navigation.
    /// Current limitation: May not return data for v4 files.
    /// </remarks>
    public QueryResult Query(string deviceName, string[]? measurements = null, 
        long? startTime = null, long? endTime = null)
    {
        if (!_schemas!.TryGetValue(deviceName, out var schema))
            throw new ArgumentException($"Device {deviceName} not found in file");
        
        var result = new QueryResult(deviceName, schema);
        
        // V4 warning: This implementation is designed for v3 format
        // V4 files may require metadata index navigation to find chunks
        if (_fileVersion == TsFileConstants.JavaVersion4)
        {
            // For v4, we would need to:
            // 1. Navigate the MetadataIndexNode tree using table name
            // 2. Find device entries within the table
            // 3. Locate chunk positions from TimeseriesMetadata
            // 4. Read chunks at those positions
            // Currently returning empty result for v4 files
            return result;
        }
        
        // Read chunks for this device (v3 format)
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
        
        // NOTE: V4 metadata structure is complex. This implementation provides
        // basic schema reading functionality. Full v4 support with data reading
        // may require additional implementation.
        
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
    }
    
    private void ReadTsFileMetadataV4()
    {
        // V4 metadata structure (from Java TsFileMetadata.deserializeFrom):
        // 1. tableIndexNodeMap: VarInt(count) + [VarIntString(tableName) + MetadataIndexNode]...
        // 2. tableSchemaMap: VarInt(count) + [VarIntString(tableName) + TableSchema]...
        // 3. metaOffset: Int64 (big-endian)
        // 4. bloomFilter (optional)
        // 5. properties (optional)

        try
        {
            // 1. Read and skip table index node map
            var tableIndexNodeNum = ReadVarInt();

            for (int i = 0; i < tableIndexNodeNum; i++)
            {
                ReadVarIntString(); // table name
                SkipMetadataIndexNode(); // skip the MetadataIndexNode
            }

            // 2. Read table schemas
            var tableSchemaNum = ReadVarInt();
            _schemas = new Dictionary<string, TableSchema>();

            for (int i = 0; i < tableSchemaNum; i++)
            {
                var tableName = ReadVarIntString();
                var tableSchema = ReadTableSchemaV4(tableName);
                _schemas[tableName] = tableSchema;
            }

            // 3. Read metadata offset
            _metadataOffset = ReadInt64BigEndian(_reader);

            // 4. Skip bloom filter if present
            if (_fileStream.Position < _fileStream.Length - 10)
            {
                var bloomFilterBytesLength = ReadVarInt();
                if (bloomFilterBytesLength > 0 && bloomFilterBytesLength < TsFileConstants.MaxBloomFilterSize)
                {
                    _reader.ReadBytes(bloomFilterBytesLength);
                    ReadVarInt(); // filterSize
                    ReadVarInt(); // hashFunctionSize
                }
            }

            // 5. Skip properties if present
            if (_fileStream.Position < _fileStream.Length - 10)
            {
                var propertiesSize = ReadVarInt();
                for (int i = 0; i < propertiesSize && i < TsFileConstants.MaxPropertiesCount; i++)
                {
                    ReadVarIntString(); // key
                    ReadVarIntString(); // value
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidDataException(
                $"Failed to parse v4 metadata. V4 format support is still experimental. Error: {ex.Message}", ex);
        }
    }

    private void SkipMetadataIndexNode()
    {
        // MetadataIndexNode structure:
        // - VarInt: children count
        // - For each child (DeviceMetadataIndexEntry):
        //   - StringArrayDeviceID: VarInt(segmentCount) + [VarIntString(segment)]...
        //   - Int64 (big-endian): offset
        // - Int64 (big-endian): endOffset
        // - byte: nodeType

        var childrenCount = ReadVarInt();

        for (int j = 0; j < childrenCount; j++)
        {
            // Read StringArrayDeviceID
            SkipStringArrayDeviceID();
            // Skip offset (8 bytes)
            ReadInt64BigEndian(_reader);
        }

        ReadInt64BigEndian(_reader); // endOffset
        _reader.ReadByte(); // nodeType
    }

    private void SkipStringArrayDeviceID()
    {
        // StringArrayDeviceID format:
        // - VarInt: segment count
        // - For each segment: VarIntString
        var segmentCount = ReadVarInt();
        for (int i = 0; i < segmentCount; i++)
        {
            ReadVarIntString();
        }
    }

    private TableSchema ReadTableSchemaV4(string tableName)
    {
        // TableSchema format (from Java TableSchema.deserialize):
        // - VarInt: column count
        // - For each column:
        //   - MeasurementSchema (Int32-prefixed strings)
        //   - Int32 (big-endian): columnCategory ordinal

        var columnCount = ReadVarInt();
        var tableSchema = new TableSchema(tableName);
        tableSchema.ColumnSchemas = new List<ColumnSchema>();

        for (int j = 0; j < columnCount; j++)
        {
            // Read MeasurementSchema (Java format uses Int32 length prefix for strings)
            var columnName = ReadInt32PrefixedString();
            var dataType = (TsDataType)_reader.ReadByte();
            var encoding = (TsEncoding)_reader.ReadByte();
            var compression = (CompressionType)_reader.ReadByte();

            // Skip props map
            var propsCount = ReadInt32BigEndian(_reader);
            for (int k = 0; k < propsCount; k++)
            {
                ReadInt32PrefixedString(); // key
                ReadInt32PrefixedString(); // value
            }

            // Read column category (Int32, big-endian)
            var categoryOrdinal = ReadInt32BigEndian(_reader);
            var category = (ColumnCategory)categoryOrdinal;

            var columnSchema = new ColumnSchema(columnName, category, dataType, encoding, compression);
            tableSchema.ColumnSchemas.Add(columnSchema);

            // Add FIELD columns to Measurements for compatibility
            if (category == ColumnCategory.Field)
            {
                tableSchema.AddMeasurement(new MeasurementSchema(columnName, dataType, encoding, compression));
            }
        }

        return tableSchema;
    }

    private string ReadInt32PrefixedString()
    {
        // Java's ReadWriteIOUtils.write(String) uses Int32 (big-endian) length prefix
        var length = ReadInt32BigEndian(_reader);
        if (length < 0)
            return string.Empty;
        var bytes = _reader.ReadBytes(length);
        return System.Text.Encoding.UTF8.GetString(bytes);
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
    
    private long ReadInt64BigEndian(BinaryReader reader)
    {
        var bytes = reader.ReadBytes(8);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt64(bytes, 0);
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
