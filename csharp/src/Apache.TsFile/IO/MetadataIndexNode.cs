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

using Apache.TsFile.Enums;

namespace Apache.TsFile.IO;

/// <summary>
/// Metadata index node for TsFile v4 hierarchical index tree.
/// </summary>
public class MetadataIndexNode
{
    /// <summary>
    /// Gets the node type.
    /// </summary>
    public MetadataIndexNodeType NodeType { get; internal set; }
    
    /// <summary>
    /// Gets the end offset of this node's data in the file.
    /// </summary>
    public long EndOffset { get; internal set; }
    
    /// <summary>
    /// Gets the children entries of this node.
    /// </summary>
    public List<IMetadataIndexEntry> Entries { get; }
    
    /// <summary>
    /// Gets the children of this node (name -> offset mapping) for backward compatibility.
    /// </summary>
    public Dictionary<string, long> Children { get; }
    
    public MetadataIndexNode()
    {
        Entries = new List<IMetadataIndexEntry>();
        Children = new Dictionary<string, long>();
    }
    
    public MetadataIndexNode(MetadataIndexNodeType nodeType) : this()
    {
        NodeType = nodeType;
    }
    
    public MetadataIndexNode(List<IMetadataIndexEntry> entries, long endOffset, MetadataIndexNodeType nodeType)
    {
        Entries = entries ?? new List<IMetadataIndexEntry>();
        EndOffset = endOffset;
        NodeType = nodeType;
        Children = new Dictionary<string, long>();
        
        // Populate Children dictionary for backward compatibility
        foreach (var entry in Entries)
        {
            var key = entry.GetCompareKey()?.ToString() ?? string.Empty;
            Children[key] = entry.Offset;
        }
    }
    
    /// <summary>
    /// Checks if this is a device-level node.
    /// </summary>
    public bool IsDeviceLevel => NodeType == MetadataIndexNodeType.InternalDevice || 
                                  NodeType == MetadataIndexNodeType.LeafDevice;
    
    /// <summary>
    /// Checks if this is a leaf node.
    /// </summary>
    public bool IsLeaf => NodeType == MetadataIndexNodeType.LeafDevice || 
                          NodeType == MetadataIndexNodeType.LeafMeasurement;
    
    /// <summary>
    /// Adds an entry to this node.
    /// </summary>
    public void AddEntry(IMetadataIndexEntry entry)
    {
        Entries.Add(entry);
        var key = entry.GetCompareKey()?.ToString() ?? string.Empty;
        Children[key] = entry.Offset;
    }
    
    /// <summary>
    /// Sets the end offset of this node's data range.
    /// </summary>
    public void SetEndOffset(long offset)
    {
        EndOffset = offset;
    }
    
    /// <summary>
    /// Gets child entry and its end offset for a given key.
    /// </summary>
    public (IMetadataIndexEntry? Entry, long ChildEndOffset) GetChildIndexEntry(IComparable key, bool exactSearch)
    {
        int index = BinarySearchInChildren(key, exactSearch);
        if (index == -1)
            return (null, -1);
        
        long childEndOffset;
        if (index != Entries.Count - 1)
            childEndOffset = Entries[index + 1].Offset;
        else
            childEndOffset = EndOffset;
        
        return (Entries[index], childEndOffset);
    }
    
    private int BinarySearchInChildren(IComparable key, bool exactSearch)
    {
        int low = 0;
        int high = Entries.Count - 1;
        
        while (low <= high)
        {
            int mid = (low + high) >> 1;
            var midVal = Entries[mid];
            int cmp = midVal.GetCompareKey().CompareTo(key);
            
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        
        // key not found
        if (exactSearch)
            return -1;
        return low == 0 ? low : low - 1;
    }
    
    /// <summary>
    /// Deserializes a metadata index node from a binary stream (V4 format).
    /// </summary>
    public static MetadataIndexNode DeserializeV4(BinaryReader reader, bool isDeviceLevel, 
        Func<int> readVarInt, Func<string> readVarIntString, Func<long> readLong)
    {
        var entries = new List<IMetadataIndexEntry>();
        int size = readVarInt();
        
        for (int i = 0; i < size; i++)
        {
            if (isDeviceLevel)
            {
                var entry = DeviceMetadataIndexEntry.Deserialize(reader, readVarInt, readVarIntString, readLong);
                entries.Add(entry);
            }
            else
            {
                var entry = MeasurementMetadataIndexEntry.Deserialize(reader, readVarIntString, readLong);
                entries.Add(entry);
            }
        }
        
        var endOffset = readLong();
        var nodeType = (MetadataIndexNodeType)reader.ReadByte();
        
        return new MetadataIndexNode(entries, endOffset, nodeType);
    }
    
    /// <summary>
    /// Deserializes a metadata index node from a binary stream (simplified legacy format).
    /// </summary>
    public static MetadataIndexNode Deserialize(BinaryReader reader, Func<int> readVarInt, Func<string> readVarIntString)
    {
        var node = new MetadataIndexNode();
        
        // Read children size
        var childrenSize = readVarInt();
        
        // Read each child entry (name -> offset)
        for (int i = 0; i < childrenSize; i++)
        {
            var name = readVarIntString();
            var offset = ReadInt64BigEndian(reader);
            node.Children[name] = offset;
            
            // Also add as measurement entry for consistency
            var entry = new MeasurementMetadataIndexEntry(name, offset);
            node.Entries.Add(entry);
        }
        
        // Read end offset
        node.EndOffset = ReadInt64BigEndian(reader);
        
        // Read node type
        node.NodeType = (MetadataIndexNodeType)reader.ReadByte();
        
        return node;
    }
    
    /// <summary>
    /// Serializes this node to a binary writer.
    /// </summary>
    public int Serialize(BinaryWriter writer)
    {
        int byteLen = 0;
        byteLen += WriteVarInt(writer, Entries.Count);
        foreach (var entry in Entries)
        {
            byteLen += entry.Serialize(writer);
        }
        byteLen += WriteLongBigEndian(writer, EndOffset);
        writer.Write((byte)NodeType);
        byteLen += 1;
        return byteLen;
    }
    
    private static long ReadInt64BigEndian(BinaryReader reader)
    {
        var bytes = reader.ReadBytes(8);
        if (bytes.Length < 8)
            throw new InvalidDataException($"Expected 8 bytes for Int64, but only {bytes.Length} bytes available");
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        return BitConverter.ToInt64(bytes, 0);
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
    
    private static int WriteLongBigEndian(BinaryWriter writer, long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        writer.Write(bytes);
        return 8;
    }
    
    public override string ToString()
    {
        return $"MetadataIndexNode{{NodeType={NodeType}, EndOffset={EndOffset}, Entries={Entries.Count}}}";
    }
}
