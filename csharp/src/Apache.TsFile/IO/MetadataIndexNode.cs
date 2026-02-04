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
    public MetadataIndexNodeType NodeType { get; private set; }
    
    /// <summary>
    /// Gets the end offset of this node's data in the file.
    /// </summary>
    public long EndOffset { get; private set; }
    
    /// <summary>
    /// Gets the children of this node (name -> offset mapping).
    /// </summary>
    public Dictionary<string, long> Children { get; }
    
    public MetadataIndexNode()
    {
        Children = new Dictionary<string, long>();
    }
    
    /// <summary>
    /// Deserializes a metadata index node from a binary stream.
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
        }
        
        // Read end offset
        node.EndOffset = ReadInt64BigEndian(reader);
        
        // Read node type
        node.NodeType = (MetadataIndexNodeType)reader.ReadByte();
        
        return node;
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
    
    public override string ToString()
    {
        return $"MetadataIndexNode{{NodeType={NodeType}, EndOffset={EndOffset}, Children={Children.Count}}}";
    }
}
