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
/// </summary>
public class TsFileWriter : IDisposable
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private readonly BinaryWriter _writer;
    private readonly Dictionary<string, TableSchema> _schemas;
    private readonly Dictionary<string, MemoryStream> _deviceChunkBuffers;
    private bool _disposed;
    private bool _headerWritten;
    private long _dataStartPosition;
    
    /// <summary>
    /// Initializes a new instance of the TsFileWriter class.
    /// </summary>
    public TsFileWriter(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or empty", nameof(filePath));
        
        _filePath = filePath;
        _schemas = new Dictionary<string, TableSchema>();
        _deviceChunkBuffers = new Dictionary<string, MemoryStream>();
        
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
        
        // Write metadata
        WriteMetadata();
        
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
        
        // Write version
        _writer.Write(TsFileConstants.Version);
        
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
}
