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

namespace Apache.TsFile.Encoding.Encoder;

/// <summary>
/// RLE (Run-Length Encoding) encoder using hybrid RLE + bit-packing approach.
/// Encodes runs of repeated values efficiently while maintaining good compression for varied data.
/// </summary>
public class RleEncoder : IEncoder
{
    private const int RleMinRepeatedNum = 8;      // Minimum repeats to trigger RLE
    private const int RleMaxRepeatedNum = 0x7FFF;  // Max repeats per RLE run (32,767)
    private const int RleMaxBitPackedNum = 63;     // Max groups in one bit-packed run
    private const int BitPackedGroupSize = 8;      // Values per bit-packed group
    
    private readonly List<int> _intValues = new();
    private readonly List<long> _longValues = new();
    private readonly List<bool> _boolValues = new();
    private readonly TsDataType _dataType;
    
    public RleEncoder(TsDataType dataType)
    {
        _dataType = dataType;
    }
    
    public void Encode(bool value, MemoryStream stream)
    {
        _boolValues.Add(value);
    }
    
    public void Encode(int value, MemoryStream stream)
    {
        _intValues.Add(value);
    }
    
    public void Encode(long value, MemoryStream stream)
    {
        _longValues.Add(value);
    }
    
    public void Encode(float value, MemoryStream stream)
    {
        // RLE not typically used for floats, fall back to int representation
        _intValues.Add(BitConverter.SingleToInt32Bits(value));
    }
    
    public void Encode(double value, MemoryStream stream)
    {
        // RLE not typically used for doubles, fall back to long representation
        _longValues.Add(BitConverter.DoubleToInt64Bits(value));
    }
    
    public void Encode(string value, MemoryStream stream)
    {
        throw new NotSupportedException("RLE encoding does not support string values");
    }
    
    public void Encode(byte[] value, MemoryStream stream)
    {
        throw new NotSupportedException("RLE encoding does not support byte array values");
    }
    
    public void Flush(MemoryStream stream)
    {
        if (_boolValues.Count > 0)
        {
            EncodeIntegers(stream, _boolValues.Select(b => b ? 1 : 0).ToList());
            _boolValues.Clear();
        }
        else if (_intValues.Count > 0)
        {
            EncodeIntegers(stream, _intValues);
            _intValues.Clear();
        }
        else if (_longValues.Count > 0)
        {
            EncodeLongs(stream, _longValues);
            _longValues.Clear();
        }
    }
    
    public int GetOneItemMaxSize()
    {
        return _dataType switch
        {
            TsDataType.Boolean or TsDataType.Int32 or TsDataType.Float => 45,
            TsDataType.Int64 or TsDataType.Double or TsDataType.Timestamp => 77,
            _ => 8
        };
    }
    
    public long GetMaxByteSize()
    {
        return (_boolValues.Count + _intValues.Count) * 45L + _longValues.Count * 77L;
    }
    
    private void EncodeIntegers(MemoryStream stream, List<int> values)
    {
        if (values.Count == 0) return;
        
        // Calculate bit width required
        int bitWidth = CalculateBitWidth(values);
        
        using var buffer = new MemoryStream();
        buffer.WriteByte((byte)bitWidth);
        
        EncodeIntRuns(buffer, values, bitWidth);
        
        // Write length and data
        WriteInt(stream, (int)buffer.Length);
        buffer.Position = 0;
        buffer.CopyTo(stream);
    }
    
    private void EncodeLongs(MemoryStream stream, List<long> values)
    {
        if (values.Count == 0) return;
        
        int bitWidth = CalculateBitWidth(values);
        
        using var buffer = new MemoryStream();
        buffer.WriteByte((byte)bitWidth);
        
        EncodeLongRuns(buffer, values, bitWidth);
        
        WriteInt(stream, (int)buffer.Length);
        buffer.Position = 0;
        buffer.CopyTo(stream);
    }
    
    private void EncodeIntRuns(MemoryStream stream, List<int> values, int bitWidth)
    {
        int i = 0;
        while (i < values.Count)
        {
            int currentValue = values[i];
            int repeatCount = 1;
            
            // Count consecutive repeated values
            while (i + repeatCount < values.Count && values[i + repeatCount] == currentValue)
            {
                repeatCount++;
            }
            
            if (repeatCount >= RleMinRepeatedNum)
            {
                // Emit RLE run(s)
                int totalRepeat = repeatCount;
                while (totalRepeat > 0)
                {
                    int runLength = Math.Min(totalRepeat, RleMaxRepeatedNum);
                    WriteVarInt(stream, runLength << 1); // LSB = 0 for RLE
                    WritePaddedInt(stream, currentValue, bitWidth);
                    totalRepeat -= runLength;
                }
                i += repeatCount;
            }
            else
            {
                // Collect values for bit-packing
                var bitPackedValues = new List<int>();
                while (i < values.Count && bitPackedValues.Count < RleMaxBitPackedNum * BitPackedGroupSize)
                {
                    currentValue = values[i];
                    repeatCount = 1;
                    while (i + repeatCount < values.Count && values[i + repeatCount] == currentValue)
                    {
                        repeatCount++;
                    }
                    
                    if (repeatCount >= RleMinRepeatedNum)
                        break; // Stop before RLE run
                    
                    bitPackedValues.Add(currentValue);
                    i++;
                }
                
                if (bitPackedValues.Count > 0)
                {
                    EmitBitPackedRun(stream, bitPackedValues, bitWidth);
                }
            }
        }
    }
    
    private void EncodeLongRuns(MemoryStream stream, List<long> values, int bitWidth)
    {
        int i = 0;
        while (i < values.Count)
        {
            long currentValue = values[i];
            int repeatCount = 1;
            
            while (i + repeatCount < values.Count && values[i + repeatCount] == currentValue)
            {
                repeatCount++;
            }
            
            if (repeatCount >= RleMinRepeatedNum)
            {
                int totalRepeat = repeatCount;
                while (totalRepeat > 0)
                {
                    int runLength = Math.Min(totalRepeat, RleMaxRepeatedNum);
                    WriteVarInt(stream, runLength << 1);
                    WritePaddedLong(stream, currentValue, bitWidth);
                    totalRepeat -= runLength;
                }
                i += repeatCount;
            }
            else
            {
                var bitPackedValues = new List<long>();
                while (i < values.Count && bitPackedValues.Count < RleMaxBitPackedNum * BitPackedGroupSize)
                {
                    currentValue = values[i];
                    repeatCount = 1;
                    while (i + repeatCount < values.Count && values[i + repeatCount] == currentValue)
                    {
                        repeatCount++;
                    }
                    
                    if (repeatCount >= RleMinRepeatedNum)
                        break;
                    
                    bitPackedValues.Add(currentValue);
                    i++;
                }
                
                if (bitPackedValues.Count > 0)
                {
                    EmitBitPackedRunLong(stream, bitPackedValues, bitWidth);
                }
            }
        }
    }
    
    private void EmitBitPackedRun(MemoryStream stream, List<int> values, int bitWidth)
    {
        int groupCount = (values.Count + BitPackedGroupSize - 1) / BitPackedGroupSize;
        int lastNum = values.Count % BitPackedGroupSize;
        if (lastNum == 0) lastNum = BitPackedGroupSize;
        
        WriteVarInt(stream, (groupCount << 1) | 1); // LSB = 1 for bit-packed
        stream.WriteByte((byte)lastNum);
        
        // Pack values in groups of 8
        for (int g = 0; g < groupCount; g++)
        {
            int start = g * BitPackedGroupSize;
            int count = Math.Min(BitPackedGroupSize, values.Count - start);
            PackInts(stream, values.Skip(start).Take(count).ToList(), bitWidth);
        }
    }
    
    private void EmitBitPackedRunLong(MemoryStream stream, List<long> values, int bitWidth)
    {
        int groupCount = (values.Count + BitPackedGroupSize - 1) / BitPackedGroupSize;
        int lastNum = values.Count % BitPackedGroupSize;
        if (lastNum == 0) lastNum = BitPackedGroupSize;
        
        WriteVarInt(stream, (groupCount << 1) | 1);
        stream.WriteByte((byte)lastNum);
        
        for (int g = 0; g < groupCount; g++)
        {
            int start = g * BitPackedGroupSize;
            int count = Math.Min(BitPackedGroupSize, values.Count - start);
            PackLongs(stream, values.Skip(start).Take(count).ToList(), bitWidth);
        }
    }
    
    private void PackInts(MemoryStream stream, List<int> values, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var packed = new byte[byteWidth * BitPackedGroupSize];
        
        for (int i = 0; i < values.Count; i++)
        {
            var valueBytes = BitConverter.GetBytes(values[i]);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(valueBytes);
            
            int offset = i * byteWidth;
            Array.Copy(valueBytes, 4 - byteWidth, packed, offset, Math.Min(byteWidth, valueBytes.Length - (4 - byteWidth)));
        }
        
        stream.Write(packed, 0, byteWidth * values.Count);
    }
    
    private void PackLongs(MemoryStream stream, List<long> values, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var packed = new byte[byteWidth * BitPackedGroupSize];
        
        for (int i = 0; i < values.Count; i++)
        {
            var valueBytes = BitConverter.GetBytes(values[i]);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(valueBytes);
            
            int offset = i * byteWidth;
            Array.Copy(valueBytes, 8 - byteWidth, packed, offset, Math.Min(byteWidth, valueBytes.Length - (8 - byteWidth)));
        }
        
        stream.Write(packed, 0, byteWidth * values.Count);
    }
    
    private int CalculateBitWidth(List<int> values)
    {
        if (values.Count == 0) return 1;
        
        // For negative numbers, we need all 32 bits
        bool hasNegative = values.Any(v => v < 0);
        if (hasNegative)
        {
            return 32; // Need full width for signed integers
        }
        
        int maxValue = values.Max();
        
        // Calculate bits needed for positive values
        int bits = 1;
        while ((1 << bits) <= maxValue && bits < 32)
        {
            bits++;
        }
        return Math.Max(1, bits);
    }
    
    private int CalculateBitWidth(List<long> values)
    {
        if (values.Count == 0) return 1;
        
        // For negative numbers, we need all 64 bits
        bool hasNegative = values.Any(v => v < 0);
        if (hasNegative)
        {
            return 64; // Need full width for signed integers
        }
        
        long maxValue = values.Max();
        
        int bits = 1;
        while ((1L << bits) <= maxValue && bits < 64)
        {
            bits++;
        }
        return Math.Max(1, bits);
    }
    
    private static void WriteInt(Stream stream, int value)
    {
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 0, 4);
    }
    
    private static void WriteVarInt(Stream stream, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)value);
    }
    
    private static void WritePaddedInt(Stream stream, int value, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 4 - byteWidth, byteWidth);
    }
    
    private static void WritePaddedLong(Stream stream, long value, int bitWidth)
    {
        int byteWidth = (bitWidth + 7) / 8;
        var bytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(bytes);
        stream.Write(bytes, 8 - byteWidth, byteWidth);
    }
}
