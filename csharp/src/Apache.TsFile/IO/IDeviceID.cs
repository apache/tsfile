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

namespace Apache.TsFile.IO;

/// <summary>
/// Device ID interface for TsFile v4 format.
/// </summary>
public interface IDeviceID : IComparable<IDeviceID>
{
    /// <summary>
    /// Gets the table name associated with the device.
    /// </summary>
    string GetTableName();
    
    /// <summary>
    /// Gets the number of segments in this device ID.
    /// </summary>
    int SegmentCount { get; }
    
    /// <summary>
    /// Gets the segment at the specified index.
    /// </summary>
    string? GetSegment(int index);
    
    /// <summary>
    /// Gets all segments as an array.
    /// </summary>
    string[] GetSegments();
    
    /// <summary>
    /// Checks if this device ID is empty.
    /// </summary>
    bool IsEmpty { get; }
    
    /// <summary>
    /// Checks if this device ID is table model (not path model).
    /// </summary>
    bool IsTableModel { get; }
    
    /// <summary>
    /// Serializes this device ID to a binary writer.
    /// </summary>
    int Serialize(BinaryWriter writer);
    
    /// <summary>
    /// Gets the serialized size of this device ID.
    /// </summary>
    int SerializedSize { get; }
}

/// <summary>
/// String array based device ID implementation for TsFile v4.
/// </summary>
public class StringArrayDeviceID : IDeviceID
{
    private const string PathSeparator = ".";
    private const string PathRoot = "root";
    private const int DefaultSegmentNumForTableName = 3;
    
    private readonly string[] _segments;
    private int _serializedSize = -1;
    
    public StringArrayDeviceID(params string[] segments)
    {
        _segments = Formalize(segments);
    }
    
    public StringArrayDeviceID(string deviceIdString)
    {
        _segments = SplitDeviceIdString(deviceIdString);
    }
    
    private static string[] Formalize(string[] segments)
    {
        // Remove trailing nulls
        int i = segments.Length - 1;
        for (; i >= 0; i--)
        {
            if (segments[i] != null)
                break;
        }
        
        if (i < 0)
            throw new ArgumentException("All segments are null");
        
        if (i != segments.Length - 1)
            return segments[..(i + 1)];
        
        return segments;
    }
    
    private static string[] SplitDeviceIdString(string deviceIdString)
    {
        var splits = deviceIdString.Split('.');
        return SplitDeviceIdString(splits);
    }
    
    private static string[] SplitDeviceIdString(string[] splits)
    {
        int segmentCnt = splits.Length;
        
        if (segmentCnt == 1)
        {
            return new[] { splits[0] };
        }
        
        if (segmentCnt < DefaultSegmentNumForTableName + 1)
        {
            var tableName = string.Join(PathSeparator, splits[..(segmentCnt - 1)]);
            return new[] { tableName, splits[segmentCnt - 1] };
        }
        
        var tableNameFull = string.Join(PathSeparator, splits[..DefaultSegmentNumForTableName]);
        var idSegments = splits[DefaultSegmentNumForTableName..];
        var finalSegments = new string[idSegments.Length + 1];
        finalSegments[0] = tableNameFull;
        Array.Copy(idSegments, 0, finalSegments, 1, idSegments.Length);
        return finalSegments;
    }
    
    public string GetTableName() => _segments[0];
    
    public int SegmentCount => _segments.Length;
    
    public string? GetSegment(int index)
    {
        if (index >= _segments.Length)
            return null;
        return _segments[index];
    }
    
    public string[] GetSegments() => _segments;
    
    public bool IsEmpty => _segments == null || _segments.Length == 0;
    
    public bool IsTableModel => !_segments[0].StartsWith(PathRoot + PathSeparator);
    
    public int Serialize(BinaryWriter writer)
    {
        int cnt = 0;
        cnt += WriteVarInt(writer, _segments.Length);
        foreach (var segment in _segments)
        {
            cnt += WriteVarIntString(writer, segment);
        }
        return cnt;
    }
    
    public int SerializedSize
    {
        get
        {
            if (_serializedSize != -1)
                return _serializedSize;
            
            int cnt = VarIntSize(_segments.Length);
            foreach (var segment in _segments)
            {
                var bytes = segment != null ? System.Text.Encoding.UTF8.GetBytes(segment) : Array.Empty<byte>();
                cnt += VarIntSize(bytes.Length);
                cnt += bytes.Length;
            }
            _serializedSize = cnt;
            return _serializedSize;
        }
    }
    
    public static StringArrayDeviceID Deserialize(BinaryReader reader, Func<int> readVarInt, Func<string> readVarIntString)
    {
        var cnt = readVarInt();
        if (cnt == 0)
            return new StringArrayDeviceID(string.Empty);
        
        var segments = new string[cnt];
        for (int i = 0; i < cnt; i++)
        {
            segments[i] = readVarIntString();
        }
        return new StringArrayDeviceID(segments);
    }
    
    public int CompareTo(IDeviceID? other)
    {
        if (other == null) return 1;
        if (ReferenceEquals(this, other)) return 0;
        
        int thisSegmentNum = SegmentCount;
        int otherSegmentNum = other.SegmentCount;
        
        for (int i = 0; i < thisSegmentNum; i++)
        {
            if (i >= otherSegmentNum)
                return 1; // Other is prefix of this
            
            var comp = string.Compare(GetSegment(i), other.GetSegment(i), StringComparison.Ordinal);
            if (comp != 0)
                return comp;
        }
        
        if (thisSegmentNum < otherSegmentNum)
            return -1; // This is prefix of other
        
        return 0;
    }
    
    public override string ToString() => string.Join(PathSeparator, _segments);
    
    public override bool Equals(object? obj)
    {
        if (obj is StringArrayDeviceID other)
            return _segments.SequenceEqual(other._segments);
        return false;
    }
    
    public override int GetHashCode()
    {
        int hash = 17;
        foreach (var segment in _segments)
        {
            hash = hash * 31 + (segment?.GetHashCode() ?? 0);
        }
        return hash;
    }
    
    private static int WriteVarInt(BinaryWriter writer, int value)
    {
        int count = 0;
        while ((value & ~0x7F) != 0)
        {
            writer.Write((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
            count++;
        }
        writer.Write((byte)value);
        return count + 1;
    }
    
    private static int WriteVarIntString(BinaryWriter writer, string? value)
    {
        if (value == null)
        {
            return WriteVarInt(writer, -1);
        }
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        int cnt = WriteVarInt(writer, bytes.Length);
        writer.Write(bytes);
        return cnt + bytes.Length;
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
}
