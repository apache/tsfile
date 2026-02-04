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
/// Writer for creating TsFile v4 format files.
/// </summary>
public class TsFileWriterV4 : IDisposable
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly BinaryWriter _writer;
    private readonly TableSchema _tableSchema;
    private readonly List<ChunkGroupInfo> _chunkGroups;
    private bool _disposed;
    private bool _headerWritten;
    private long _dataStartPosition;
    
    /// <summary>
    /// Chunk header marker for single page chunk.
    /// </summary>
    private const byte OnlyOnePageChunkHeader = 0x05;
    
    /// <summary>
    /// Chunk header marker for multi-page chunk.
    /// </summary>
    private const byte ChunkHeaderMarker = 0x01;
    
    /// <summary>
    /// Separator marker between data and metadata.
    /// </summary>
    private const byte Separator = 0x02;
    
    /// <summary>
    /// Chunk group footer marker.
    /// </summary>
    private const byte ChunkGroupFooterMarker = 0x00;
    
    /// <summary>
    /// Initializes a new instance of the TsFileWriterV4 class.
    /// </summary>
    public TsFileWriterV4(string filePath, TableSchema tableSchema)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
        
        _filePath = filePath;
        _tableSchema = tableSchema ?? throw new ArgumentNullException(nameof(tableSchema));
        _chunkGroups = new List<ChunkGroupInfo>();
        
        _fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write);
        _writer = new BinaryWriter(_fileStream);
        
        WriteHeader();
    }
    
    /// <summary>
    /// Writes a tablet of data to the file.
    /// </summary>
    public void Write(TabletV4 tablet)
    {
        if (tablet == null)
            throw new ArgumentNullException(nameof(tablet));
        
        if (tablet.RowCount == 0)
            return;
        
        // Group rows by device (tag values combination)
        var deviceGroups = GroupByDevice(tablet);
        
        foreach (var (deviceId, rows) in deviceGroups)
        {
            WriteChunkGroup(deviceId, tablet, rows);
        }
    }
    
    /// <summary>
    /// Closes the writer and flushes all data to disk.
    /// </summary>
    public void Close()
    {
        if (_disposed)
            return;
        
        // Write separator between data and metadata
        _writer.Write(Separator);
        var metadataOffset = _fileStream.Position;
        
        // Write TsFileMetadata
        WriteTsFileMetadata(metadataOffset);
        
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
        
        // Write version (4 for v4 format)
        _writer.Write(TsFileConstants.JavaVersion4);
        
        _dataStartPosition = _fileStream.Position;
        _headerWritten = true;
    }
    
    private Dictionary<IDeviceID, List<int>> GroupByDevice(TabletV4 tablet)
    {
        var groups = new Dictionary<IDeviceID, List<int>>(new DeviceIDComparer());
        
        for (int row = 0; row < tablet.RowCount; row++)
        {
            var tagValues = new List<string>();
            tagValues.Add(_tableSchema.TableName); // First segment is table name
            
            // Collect tag values for this row
            for (int col = 0; col < tablet.ColumnCount; col++)
            {
                var columnName = tablet.ColumnNames[col];
                var columnSchema = _tableSchema.ColumnSchemas?.FirstOrDefault(c => c.Name == columnName);
                
                if (columnSchema?.Category == ColumnCategory.Tag)
                {
                    var value = tablet.GetValue(row, col);
                    tagValues.Add(value?.ToString() ?? "");
                }
            }
            
            var deviceId = new StringArrayDeviceID(tagValues.ToArray());
            
            if (!groups.ContainsKey(deviceId))
                groups[deviceId] = new List<int>();
            
            groups[deviceId].Add(row);
        }
        
        return groups;
    }
    
    private void WriteChunkGroup(IDeviceID deviceId, TabletV4 tablet, List<int> rowIndices)
    {
        var chunkGroupStartOffset = _fileStream.Position;
        var chunkInfoList = new List<ChunkInfo>();
        
        // Get field columns from schema
        var fieldColumns = _tableSchema.ColumnSchemas?
            .Where(c => c.Category == ColumnCategory.Field)
            .ToList() ?? new List<ColumnSchema>();
        
        // Write time chunk first
        var timeChunkInfo = WriteTimeChunk(tablet, rowIndices);
        chunkInfoList.Add(timeChunkInfo);
        
        // Write value chunks for each field column
        foreach (var fieldColumn in fieldColumns)
        {
            var columnIndex = ((List<string>)tablet.ColumnNames).IndexOf(fieldColumn.Name);
            if (columnIndex >= 0)
            {
                var chunkInfo = WriteValueChunk(tablet, rowIndices, columnIndex, fieldColumn);
                chunkInfoList.Add(chunkInfo);
            }
        }
        
        // Write chunk group footer
        _writer.Write(ChunkGroupFooterMarker);
        
        // Record chunk group info for metadata
        _chunkGroups.Add(new ChunkGroupInfo
        {
            DeviceId = deviceId,
            StartOffset = chunkGroupStartOffset,
            EndOffset = _fileStream.Position,
            Chunks = chunkInfoList
        });
    }
    
    private ChunkInfo WriteTimeChunk(TabletV4 tablet, List<int> rowIndices)
    {
        var chunkStartOffset = _fileStream.Position;
        
        // Collect timestamps for this device
        var timestamps = rowIndices.Select(i => tablet.Timestamps[i]).ToArray();
        
        // Encode timestamps using PLAIN encoding
        using var pageDataStream = new MemoryStream();
        var encoder = EncoderFactory.CreateEncoder(TsEncoding.Plain, TsDataType.Int64);
        
        foreach (var ts in timestamps)
        {
            encoder.Encode(ts, pageDataStream);
        }
        encoder.Flush(pageDataStream);
        
        var pageData = pageDataStream.ToArray();
        
        // Compress page data
        var compressor = CompressorFactory.GetCompressor(CompressionType.Uncompressed);
        var compressedData = compressor.Compress(pageData);
        
        // Write chunk header (with time chunk mask)
        _writer.Write((byte)(OnlyOnePageChunkHeader | 0x80)); // Time chunk mask
        
        // Write measurement ID (empty for time chunk)
        WriteVarIntString("");
        
        // Calculate chunk data size (page header + page data)
        var pageHeaderSize = VarIntSize(pageData.Length) + VarIntSize(compressedData.Length);
        var chunkDataSize = pageHeaderSize + compressedData.Length;
        
        // Write chunk data size
        WriteVarInt(chunkDataSize);
        
        // Write data type
        _writer.Write((byte)TsDataType.Int64);
        
        // Write compression type
        _writer.Write((byte)CompressionType.Uncompressed);
        
        // Write encoding type
        _writer.Write((byte)TsEncoding.Plain);
        
        // Write page header
        WriteVarInt(pageData.Length);     // uncompressed size
        WriteVarInt(compressedData.Length); // compressed size
        
        // Write page data
        _writer.Write(compressedData);
        
        return new ChunkInfo
        {
            MeasurementId = "",
            DataType = TsDataType.Int64,
            Encoding = TsEncoding.Plain,
            Compression = CompressionType.Uncompressed,
            Offset = chunkStartOffset,
            IsTimeChunk = true,
            Statistics = new ChunkStatistics
            {
                Count = timestamps.Length,
                StartTime = timestamps.Min(),
                EndTime = timestamps.Max(),
                MinValue = timestamps.Min(),
                MaxValue = timestamps.Max()
            }
        };
    }
    
    private ChunkInfo WriteValueChunk(TabletV4 tablet, List<int> rowIndices, int columnIndex, ColumnSchema columnSchema)
    {
        var chunkStartOffset = _fileStream.Position;
        
        // Collect values for this column
        var values = new List<object?>();
        foreach (var rowIndex in rowIndices)
        {
            values.Add(tablet.GetValue(rowIndex, columnIndex));
        }
        
        // Encode values
        using var pageDataStream = new MemoryStream();
        var encoder = EncoderFactory.CreateEncoder(columnSchema.Encoding, columnSchema.DataType);
        
        foreach (var value in values)
        {
            if (value == null)
            {
                // Handle null values based on data type
                EncodeDefaultValue(encoder, columnSchema.DataType, pageDataStream);
            }
            else
            {
                EncodeValue(encoder, columnSchema.DataType, value, pageDataStream);
            }
        }
        encoder.Flush(pageDataStream);
        
        var pageData = pageDataStream.ToArray();
        
        // Compress page data
        var compressor = CompressorFactory.GetCompressor(columnSchema.Compression);
        var compressedData = compressor.Compress(pageData);
        
        // Write chunk header (with value chunk mask)
        _writer.Write((byte)(OnlyOnePageChunkHeader | 0x40)); // Value chunk mask
        
        // Write measurement ID
        WriteVarIntString(columnSchema.Name);
        
        // Calculate chunk data size
        var pageHeaderSize = VarIntSize(pageData.Length) + VarIntSize(compressedData.Length);
        var chunkDataSize = pageHeaderSize + compressedData.Length;
        
        // Write chunk data size
        WriteVarInt(chunkDataSize);
        
        // Write data type
        _writer.Write((byte)columnSchema.DataType);
        
        // Write compression type
        _writer.Write((byte)columnSchema.Compression);
        
        // Write encoding type
        _writer.Write((byte)columnSchema.Encoding);
        
        // Write page header
        WriteVarInt(pageData.Length);     // uncompressed size
        WriteVarInt(compressedData.Length); // compressed size
        
        // Write page data
        _writer.Write(compressedData);
        
        return new ChunkInfo
        {
            MeasurementId = columnSchema.Name,
            DataType = columnSchema.DataType,
            Encoding = columnSchema.Encoding,
            Compression = columnSchema.Compression,
            Offset = chunkStartOffset,
            IsTimeChunk = false,
            Statistics = new ChunkStatistics
            {
                Count = values.Count
            }
        };
    }
    
    private void EncodeValue(IEncoder encoder, TsDataType dataType, object value, MemoryStream stream)
    {
        switch (dataType)
        {
            case TsDataType.Boolean:
                encoder.Encode(Convert.ToBoolean(value), stream);
                break;
            case TsDataType.Int32:
                encoder.Encode(Convert.ToInt32(value), stream);
                break;
            case TsDataType.Int64:
            case TsDataType.Timestamp:
                encoder.Encode(Convert.ToInt64(value), stream);
                break;
            case TsDataType.Float:
                encoder.Encode(Convert.ToSingle(value), stream);
                break;
            case TsDataType.Double:
                encoder.Encode(Convert.ToDouble(value), stream);
                break;
            case TsDataType.Text:
            case TsDataType.String:
                encoder.Encode(value?.ToString() ?? "", stream);
                break;
        }
    }
    
    private void EncodeDefaultValue(IEncoder encoder, TsDataType dataType, MemoryStream stream)
    {
        switch (dataType)
        {
            case TsDataType.Boolean:
                encoder.Encode(false, stream);
                break;
            case TsDataType.Int32:
                encoder.Encode(0, stream);
                break;
            case TsDataType.Int64:
            case TsDataType.Timestamp:
                encoder.Encode(0L, stream);
                break;
            case TsDataType.Float:
                encoder.Encode(0f, stream);
                break;
            case TsDataType.Double:
                encoder.Encode(0d, stream);
                break;
            case TsDataType.Text:
            case TsDataType.String:
                encoder.Encode("", stream);
                break;
        }
    }
    
    private void WriteTsFileMetadata(long metadataOffset)
    {
        using var metadataBuffer = new MemoryStream();
        using var metadataWriter = new BinaryWriter(metadataBuffer, System.Text.Encoding.UTF8, true);
        
        // Build and write table index node map
        var tableIndexNodes = BuildTableIndexNodes();
        WriteVarInt(metadataWriter, tableIndexNodes.Count);
        
        foreach (var (tableName, indexNode) in tableIndexNodes)
        {
            WriteVarIntString(metadataWriter, tableName);
            indexNode.Serialize(metadataWriter);
        }
        
        // Write table schema map
        WriteVarInt(metadataWriter, 1); // Single table
        WriteVarIntString(metadataWriter, _tableSchema.TableName);
        
        // Write columns
        var allColumns = _tableSchema.ColumnSchemas ?? new List<ColumnSchema>();
        WriteVarInt(metadataWriter, allColumns.Count);
        
        foreach (var column in allColumns)
        {
            WriteVarIntString(metadataWriter, column.Name);
            metadataWriter.Write((byte)column.DataType);
            metadataWriter.Write((byte)column.Encoding);
            metadataWriter.Write((byte)column.Compression);
            metadataWriter.Write((byte)column.Category);
        }
        
        // Write metadata offset (separator position)
        WriteLongBigEndian(metadataWriter, metadataOffset);
        
        // Write bloom filter (empty)
        WriteVarInt(metadataWriter, 0);
        
        // Write properties (empty)
        WriteVarInt(metadataWriter, 0);
        
        // Get metadata bytes
        var metadataBytes = metadataBuffer.ToArray();
        
        // Write metadata to file
        _writer.Write(metadataBytes);
        
        // Write metadata size (big-endian int32)
        WriteInt32BigEndian(_writer, metadataBytes.Length);
    }
    
    private Dictionary<string, MetadataIndexNode> BuildTableIndexNodes()
    {
        var result = new Dictionary<string, MetadataIndexNode>();
        
        // Group chunk groups by table name
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
            
            deviceNode.EndOffset = _fileStream.Position;
            result[tableName] = deviceNode;
        }
        
        return result;
    }
    
    private void WriteFooter()
    {
        // Write magic string
        _writer.Write(TsFileConstants.MagicString);
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
    
    private void WriteLongBigEndian(BinaryWriter writer, long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        writer.Write(bytes);
    }
    
    private static void WriteInt32BigEndian(BinaryWriter writer, int value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        writer.Write(bytes);
    }
    
    private static void WriteVarInt(BinaryWriter writer, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            writer.Write((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        writer.Write((byte)value);
    }
    
    private static void WriteVarIntString(BinaryWriter writer, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        WriteVarInt(writer, bytes.Length);
        writer.Write(bytes);
    }
    
    private static int VarIntSize(int value)
    {
        int size = 1;
        while ((value & ~0x7F) != 0)
        {
            value = (int)((uint)value >> 7);
            size++;
        }
        return size;
    }
    
    public void Dispose()
    {
        if (_disposed)
            return;
        
        _writer?.Dispose();
        _fileStream?.Dispose();
        
        _disposed = true;
        GC.SuppressFinalize(this);
    }
    
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
        public ChunkStatistics Statistics { get; set; } = new();
    }
    
    private class ChunkStatistics
    {
        public long Count { get; set; }
        public long StartTime { get; set; }
        public long EndTime { get; set; }
        public object? MinValue { get; set; }
        public object? MaxValue { get; set; }
    }
    
    private class DeviceIDComparer : IEqualityComparer<IDeviceID>
    {
        public bool Equals(IDeviceID? x, IDeviceID? y)
        {
            if (x == null && y == null) return true;
            if (x == null || y == null) return false;
            return x.ToString() == y.ToString();
        }
        
        public int GetHashCode(IDeviceID obj) => obj?.ToString()?.GetHashCode() ?? 0;
    }
}

/// <summary>
/// Tablet for V4 format with tag and field support.
/// </summary>
public class TabletV4
{
    private readonly List<string> _columnNames;
    private readonly List<TsDataType> _dataTypes;
    private readonly List<long> _timestamps;
    private readonly List<object?[]> _rows;
    
    /// <summary>
    /// Initializes a new instance of TabletV4.
    /// </summary>
    public TabletV4(List<string> columnNames, List<TsDataType> dataTypes)
    {
        _columnNames = columnNames ?? throw new ArgumentNullException(nameof(columnNames));
        _dataTypes = dataTypes ?? throw new ArgumentNullException(nameof(dataTypes));
        
        if (_columnNames.Count != _dataTypes.Count)
            throw new ArgumentException("Column names and data types must have the same count");
        
        _timestamps = new List<long>();
        _rows = new List<object?[]>();
    }
    
    /// <summary>
    /// Gets the column names.
    /// </summary>
    public IReadOnlyList<string> ColumnNames => _columnNames;
    
    /// <summary>
    /// Gets the data types.
    /// </summary>
    public IReadOnlyList<TsDataType> DataTypes => _dataTypes;
    
    /// <summary>
    /// Gets the timestamps.
    /// </summary>
    public IReadOnlyList<long> Timestamps => _timestamps;
    
    /// <summary>
    /// Gets the row count.
    /// </summary>
    public int RowCount => _timestamps.Count;
    
    /// <summary>
    /// Gets the column count.
    /// </summary>
    public int ColumnCount => _columnNames.Count;
    
    /// <summary>
    /// Adds a timestamp at the specified row index.
    /// </summary>
    public void AddTimestamp(int rowIndex, long timestamp)
    {
        while (_timestamps.Count <= rowIndex)
        {
            _timestamps.Add(0);
            _rows.Add(new object?[_columnNames.Count]);
        }
        _timestamps[rowIndex] = timestamp;
    }
    
    /// <summary>
    /// Adds a value at the specified row and column.
    /// </summary>
    public void AddValue(int rowIndex, string columnName, object? value)
    {
        var colIndex = _columnNames.IndexOf(columnName);
        if (colIndex < 0)
            throw new ArgumentException($"Column '{columnName}' not found");
        
        AddValue(rowIndex, colIndex, value);
    }
    
    /// <summary>
    /// Adds a value at the specified row and column index.
    /// </summary>
    public void AddValue(int rowIndex, int columnIndex, object? value)
    {
        if (columnIndex < 0 || columnIndex >= _columnNames.Count)
            throw new ArgumentOutOfRangeException(nameof(columnIndex));
        
        while (_rows.Count <= rowIndex)
        {
            _timestamps.Add(0);
            _rows.Add(new object?[_columnNames.Count]);
        }
        
        _rows[rowIndex][columnIndex] = value;
    }
    
    /// <summary>
    /// Gets the value at the specified row and column.
    /// </summary>
    public object? GetValue(int rowIndex, int columnIndex)
    {
        if (rowIndex < 0 || rowIndex >= _rows.Count)
            return null;
        if (columnIndex < 0 || columnIndex >= _columnNames.Count)
            return null;
        
        return _rows[rowIndex][columnIndex];
    }
    
    /// <summary>
    /// Resets the tablet for reuse.
    /// </summary>
    public void Reset()
    {
        _timestamps.Clear();
        _rows.Clear();
    }
}
