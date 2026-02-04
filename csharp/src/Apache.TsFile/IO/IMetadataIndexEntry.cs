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
/// Interface for metadata index entries in TsFile v4 format.
/// </summary>
public interface IMetadataIndexEntry
{
    /// <summary>
    /// Gets the offset of the entry.
    /// </summary>
    long Offset { get; }
    
    /// <summary>
    /// Gets the key for comparison purposes.
    /// </summary>
    IComparable GetCompareKey();
    
    /// <summary>
    /// Checks if this is a device level entry.
    /// </summary>
    bool IsDeviceLevel { get; }
    
    /// <summary>
    /// Gets the serialized size of this entry.
    /// </summary>
    int SerializedSize { get; }
    
    /// <summary>
    /// Serializes this entry to a binary writer.
    /// </summary>
    int Serialize(BinaryWriter writer);
}

/// <summary>
/// Device-level metadata index entry for TsFile v4.
/// </summary>
public class DeviceMetadataIndexEntry : IMetadataIndexEntry
{
    public IDeviceID DeviceID { get; }
    public long Offset { get; private set; }
    
    public DeviceMetadataIndexEntry(IDeviceID deviceID, long offset)
    {
        DeviceID = deviceID ?? throw new ArgumentNullException(nameof(deviceID));
        Offset = offset;
    }
    
    public IComparable GetCompareKey() => DeviceID.ToString() ?? "";
    
    public bool IsDeviceLevel => true;
    
    public int SerializedSize => DeviceID.SerializedSize + sizeof(long);
    
    public int Serialize(BinaryWriter writer)
    {
        int byteLen = DeviceID.Serialize(writer);
        WriteLongBigEndian(writer, Offset);
        return byteLen + sizeof(long);
    }
    
    public static DeviceMetadataIndexEntry Deserialize(BinaryReader reader, Func<int> readVarInt, Func<string> readVarIntString, Func<long> readLong)
    {
        var deviceID = StringArrayDeviceID.Deserialize(reader, readVarInt, readVarIntString);
        var offset = readLong();
        return new DeviceMetadataIndexEntry(deviceID, offset);
    }
    
    public override string ToString() => $"<{DeviceID},{Offset}>";
    
    private static void WriteLongBigEndian(BinaryWriter writer, long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        writer.Write(bytes);
    }
}

/// <summary>
/// Measurement-level metadata index entry for TsFile v4.
/// </summary>
public class MeasurementMetadataIndexEntry : IMetadataIndexEntry
{
    public string Name { get; }
    public long Offset { get; private set; }
    
    public MeasurementMetadataIndexEntry(string name, long offset)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Offset = offset;
    }
    
    public IComparable GetCompareKey() => Name;
    
    public bool IsDeviceLevel => false;
    
    public int SerializedSize
    {
        get
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(Name);
            return VarIntSize(bytes.Length) + bytes.Length + sizeof(long);
        }
    }
    
    public int Serialize(BinaryWriter writer)
    {
        int byteLen = WriteVarIntString(writer, Name);
        WriteLongBigEndian(writer, Offset);
        return byteLen + sizeof(long);
    }
    
    public static MeasurementMetadataIndexEntry Deserialize(BinaryReader reader, Func<string> readVarIntString, Func<long> readLong)
    {
        var name = readVarIntString();
        var offset = readLong();
        return new MeasurementMetadataIndexEntry(name, offset);
    }
    
    public override string ToString() => $"<{Name},{Offset}>";
    
    private static void WriteLongBigEndian(BinaryWriter writer, long value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        writer.Write(bytes);
    }
    
    private static int WriteVarIntString(BinaryWriter writer, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        int cnt = WriteVarInt(writer, bytes.Length);
        writer.Write(bytes);
        return cnt + bytes.Length;
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
