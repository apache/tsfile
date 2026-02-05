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
/// Writer for creating TSFile format files.
/// Supports both V3 and V4 formats, with V4 as the default.
/// </summary>
public class TsFileWriter : IDisposable
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly BinaryWriter _writer;
    private readonly Dictionary<string, TableSchema> _schemas;
    private readonly Dictionary<string, MemoryStream> _deviceChunkBuffers;
    private readonly List<ChunkGroupInfo> _chunkGroups;
    private bool _disposed;
    private bool _headerWritten;
    private long _dataStartPosition;

    /// <summary>
    /// Gets the file version being written (3 or 4).
    /// </summary>
    public byte FileVersion { get; }

    /// <summary>
    /// Initializes a new instance of the TsFileWriter class with default V4 format.
    /// </summary>
    public TsFileWriter(string filePath) : this(filePath, TsFileConstants.DefaultFileVersion)
    {
    }

    /// <summary>
    /// Initializes a new instance of the TsFileWriter class with specified version.
    /// </summary>
    public TsFileWriter(string filePath, byte fileVersion)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
        if (fileVersion != 3 && fileVersion != 4)
            throw new ArgumentException("File version must be 3 or 4", nameof(fileVersion));

        _filePath = filePath;
        FileVersion = fileVersion;
        _schemas = new Dictionary<string, TableSchema>();
        _deviceChunkBuffers = new Dictionary<string, MemoryStream>();
        _chunkGroups = new List<ChunkGroupInfo>();

        _fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write);
        _writer = new BinaryWriter(_fileStream);

        WriteHeader();
    }
    
    /// <summary>
    /// Registers a table schema for writing.
    /// </summary>
    public void RegisterTableSchema(TableSchema schema)
    {
        if (schema == null)
            throw new ArgumentNullException(nameof(schema));

        if (_schemas.ContainsKey(schema.TableName))
            throw new ArgumentException($"Schema for table {schema.TableName} already registered");

        _schemas[schema.TableName] = schema;
        _deviceChunkBuffers[schema.TableName] = new MemoryStream();
    }

    /// <summary>
    /// Registers a table schema for writing (alias for RegisterTableSchema).
    /// </summary>
    public void RegisterTable(TableSchema schema) => RegisterTableSchema(schema);
    
    /// <summary>
    /// Registers a device with measurement schemas.
    /// </summary>
    public void RegisterDevice(string deviceName, List<MeasurementSchema> measurements)
    {
        var schema = new TableSchema(deviceName);
        foreach (var measurement in measurements)
        {
            schema.AddMeasurement(measurement);
        }
        RegisterTableSchema(schema);
    }

    /// <summary>
    /// Registers a timeseries using tree model path (e.g., "root.db1.d1.s1").
    /// Automatically converts to table model for V4 format.
    /// </summary>
    public void RegisterTimeseries(string devicePath, List<MeasurementSchema> measurements)
    {
        var schema = TableSchema.CreateFromTreeModel(devicePath, measurements);
        RegisterTableSchema(schema);
    }
    
    /// <summary>
    /// Writes a tablet of data to the file.
    /// </summary>
    public void Write(Tablet tablet)
    {
        if (tablet == null)
            throw new ArgumentNullException(nameof(tablet));

        if (!_schemas.TryGetValue(tablet.DeviceName, out var schema))
            throw new ArgumentException($"Schema for device {tablet.DeviceName} not registered");

        if (tablet.RowCount == 0)
            return;

        // Write data to device chunk buffer
        var buffer = _deviceChunkBuffers[tablet.DeviceName];
        WriteChunkData(buffer, tablet, schema);

        // If buffer is large enough, flush to file
        if (buffer.Length >= TsFileConstants.DefaultChunkSize)
        {
            FlushDeviceBuffer(tablet.DeviceName);
        }
    }

    /// <summary>
    /// Writes a tablet of data to the file (alias for Write).
    /// </summary>
    public void WriteTable(Tablet tablet) => Write(tablet);
    
    /// <summary>
    /// Writes a single row of data.
    /// </summary>
    public void WriteRow(string deviceName, long timestamp, params object[] values)
    {
        if (!_schemas.TryGetValue(deviceName, out var schema))
            throw new ArgumentException($"Schema for device {deviceName} not registered");
        
        var tablet = new Tablet(deviceName, schema.Measurements, 1);
        tablet.AddRow(timestamp, values);
        Write(tablet);
    }
    
    /// <summary>
    /// Flushes all buffered data and closes the file.
    /// </summary>
    public void Close()
    {
        if (_disposed)
            return;

        // Flush all device buffers
        foreach (var deviceName in _schemas.Keys.ToList())
        {
            FlushDeviceBuffer(deviceName);
        }

        // Write separator and metadata based on version
        if (FileVersion == 4)
        {
            WriteV4Metadata();
        }
        else
        {
            WriteV3Metadata();
        }

        // Write footer
        WriteFooter();

        Dispose();
    }
    
    private void WriteHeader()
    {
        if (_headerWritten)
            return;

        // Write magic string
        _writer.Write(TsFileConstants.MagicString);

        // Write version (V3 or V4)
        _writer.Write(FileVersion);

        _dataStartPosition = _fileStream.Position;
        _headerWritten = true;
    }
    
    private void WriteChunkData(MemoryStream buffer, Tablet tablet, TableSchema schema)
    {
        using var chunkWriter = new BinaryWriter(buffer, System.Text.Encoding.UTF8, true);
        
        // Write chunk header marker
        chunkWriter.Write(TsFileConstants.ChunkHeaderMarker);
        
        // Write device name
        chunkWriter.Write(tablet.DeviceName);
        
        // Write each measurement column
        for (int i = 0; i < schema.Measurements.Count; i++)
        {
            var measurement = schema.Measurements[i];
            var encoder = EncoderFactory.CreateEncoder(measurement.Encoding, measurement.DataType);
            var compressor = CompressorFactory.GetCompressor(measurement.Compression);
            
            // Encode values
            using var valueStream = new MemoryStream();
            EncodeColumn(encoder, tablet, i, valueStream);
            
            // Compress encoded data
            var encodedData = valueStream.ToArray();
            var compressedData = compressor.Compress(encodedData);
            
            // Write measurement header
            chunkWriter.Write(measurement.MeasurementName);
            chunkWriter.Write((byte)measurement.DataType);
            chunkWriter.Write((byte)measurement.Encoding);
            chunkWriter.Write((byte)measurement.Compression);
            
            // Write data size and data
            chunkWriter.Write(compressedData.Length);
            chunkWriter.Write(encodedData.Length); // Original size
            chunkWriter.Write(compressedData);
        }
        
        // Write timestamps
        using var timestampStream = new MemoryStream();
        for (int i = 0; i < tablet.RowCount; i++)
        {
            WriteInt64BigEndian(timestampStream, tablet.Timestamps[i]);
        }
        
        var timestampData = timestampStream.ToArray();
        chunkWriter.Write(timestampData.Length);
        chunkWriter.Write(timestampData);
        chunkWriter.Write(tablet.RowCount);
    }
    
    private void EncodeColumn(IEncoder encoder, Tablet tablet, int columnIndex, MemoryStream stream)
    {
        var schema = tablet.Schemas[columnIndex];
        var values = tablet.Values[columnIndex];
        
        for (int row = 0; row < tablet.RowCount; row++)
        {
            switch (schema.DataType)
            {
                case TsDataType.Boolean:
                    encoder.Encode(((bool[])values!)[row], stream);
                    break;
                case TsDataType.Int32:
                    encoder.Encode(((int[])values!)[row], stream);
                    break;
                case TsDataType.Int64:
                case TsDataType.Timestamp:
                    encoder.Encode(((long[])values!)[row], stream);
                    break;
                case TsDataType.Float:
                    encoder.Encode(((float[])values!)[row], stream);
                    break;
                case TsDataType.Double:
                    encoder.Encode(((double[])values!)[row], stream);
                    break;
                case TsDataType.Text:
                case TsDataType.String:
                    encoder.Encode(((string[])values!)[row], stream);
                    break;
            }
        }
        
        encoder.Flush(stream);
    }
    
    private void FlushDeviceBuffer(string deviceName)
    {
        if (!_deviceChunkBuffers.TryGetValue(deviceName, out var buffer))
            return;
        
        if (buffer.Length == 0)
            return;
        
        // Write buffer to file
        buffer.Position = 0;
        buffer.CopyTo(_fileStream);
        
        // Clear buffer
        buffer.SetLength(0);
        buffer.Position = 0;
    }
    
    private void WriteMetadata()
    {
        // Write metadata section
        var metadataStart = _fileStream.Position;

        // Write schema count
        _writer.Write(_schemas.Count);

        // Write each schema
        foreach (var schema in _schemas.Values)
        {
            schema.Serialize(_writer);
        }

        // Write metadata offset at the end
        var metadataEnd = _fileStream.Position;
        _writer.Write(metadataStart);
    }

    private void WriteV3Metadata()
    {
        WriteMetadata();
    }

    private void WriteV4Metadata()
    {
        // Write separator between data and metadata
        _writer.Write((byte)0x02);
        var metadataStartPos = _fileStream.Position;

        // Build and write table index nodes
        var tableIndexNodes = BuildTableIndexNodes();
        WriteVarInt(tableIndexNodes.Count);

        foreach (var (tableName, indexNode) in tableIndexNodes)
        {
            WriteVarIntString(tableName);
            indexNode.Serialize(_writer);
        }

        // Write table schema map
        WriteVarInt(_schemas.Count);
        foreach (var schema in _schemas.Values)
        {
            WriteVarIntString(schema.TableName);
            var columns = schema.ColumnSchemas ?? new List<ColumnSchema>();
            WriteVarInt(columns.Count);
            foreach (var col in columns)
            {
                // Write column in Java-compatible format
                WriteInt32PrefixedString(col.Name);
                _writer.Write((byte)col.DataType);
                _writer.Write((byte)col.Encoding);
                _writer.Write((byte)col.Compression);
                WriteInt32BigEndian(0); // props map count (empty)
                WriteInt32BigEndian((int)col.Category);
            }
        }

        // Write metadata offset (big-endian)
        WriteLongBigEndian(metadataStartPos);

        // Write bloom filter (empty) and properties (empty)
        WriteVarInt(0);
        WriteVarInt(0);

        // Calculate and write metadata size (big-endian, 4 bytes)
        var metadataSize = (int)(_fileStream.Position - metadataStartPos);
        WriteInt32BigEndian(metadataSize);
    }
    
    private void WriteFooter()
    {
        // Write magic string again
        _writer.Write(TsFileConstants.MagicString);
    }
    
    private static void WriteInt64BigEndian(Stream stream, long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, 8);
    }

    private Dictionary<string, MetadataIndexNode> BuildTableIndexNodes()
    {
        var result = new Dictionary<string, MetadataIndexNode>();
        var tableChunkGroups = _chunkGroups.GroupBy(cg => cg.DeviceId.GetTableName());

        foreach (var tableGroup in tableChunkGroups)
        {
            var tableName = tableGroup.Key;
            var deviceNode = new MetadataIndexNode(MetadataIndexNodeType.LeafDevice);

            foreach (var chunkGroup in tableGroup)
            {
                var entry = new DeviceMetadataIndexEntry(chunkGroup.DeviceId, chunkGroup.StartOffset);
                deviceNode.AddEntry(entry);
            }

            deviceNode.SetEndOffset(_fileStream.Position);
            result[tableName] = deviceNode;
        }

        return result;
    }

    private void WriteVarInt(int value)
    {
        while ((value & ~0x7F) != 0)
        {
            _writer.Write((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        _writer.Write((byte)value);
    }

    private void WriteVarIntString(string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteVarInt(bytes.Length);
        _writer.Write(bytes);
    }

    private void WriteLongBigEndian(long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        _writer.Write(bytes);
    }

    private void WriteInt32BigEndian(int value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        _writer.Write(bytes);
    }

    private void WriteInt32PrefixedString(string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteInt32BigEndian(bytes.Length);
        _writer.Write(bytes);
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;

        _writer?.Dispose();
        _fileStream?.Dispose();

        foreach (var buffer in _deviceChunkBuffers.Values)
        {
            buffer?.Dispose();
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    // V4 format helper classes
    private class ChunkGroupInfo
    {
        public IDeviceID DeviceId { get; set; } = null!;
        public long StartOffset { get; set; }
        public long EndOffset { get; set; }
        public List<ChunkInfo> Chunks { get; set; } = new();
    }

    private class ChunkInfo
    {
        public string MeasurementId { get; set; } = string.Empty;
        public TsDataType DataType { get; set; }
        public TsEncoding Encoding { get; set; }
        public CompressionType Compression { get; set; }
        public long Offset { get; set; }
        public bool IsTimeChunk { get; set; }
        public long Count { get; set; }
        public long StartTime { get; set; }
        public long EndTime { get; set; }
    }
}
