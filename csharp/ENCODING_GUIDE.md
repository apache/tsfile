# Encoding Implementation Guide

This document provides detailed guidance for implementing the remaining encoding algorithms in the C# TSFile library.

## Status Overview

### ✅ Implemented
- **Plain Encoding** - All data types
- **RLE (Run-Length Encoding)** - Boolean, Int32, Int64 ✅ NEW

### 📝 To Be Implemented
1. Gorilla Encoding (HIGH priority)
2. ZigZag Encoding (MEDIUM priority)
3. Dictionary Encoding (MEDIUM priority)
4. TS_2DIFF Encoding (MEDIUM priority)
5. CHIMP, SPRINTZ, RLBE (LOW priority)

---

## Implementation Guide for Remaining Encodings

### 1. Gorilla Encoding (Next Priority)

**Purpose**: Lossless compression for time-series floating-point data using XOR delta encoding.

**Algorithm Overview**:
```
1. Store first value as-is (32 or 64 bits)
2. For subsequent values:
   a. XOR with previous value
   b. If XOR == 0: write single '0' bit
   c. If XOR != 0: write '1' bit, then:
      - Check if leading/trailing zeros match previous block
      - If yes: write '0' bit + meaningful bits
      - If no: write '1' bit + [5 bits leading][6 bits length][meaningful bits]
```

**Key Implementation Details**:
- Bit-level encoding (not byte-aligned)
- Needs BitWriter/BitReader helper classes
- Track previous value and previous block info (leading zeros, trailing zeros)
- Special handling for Int32/Int64 (treat as bit patterns)

**Java Reference**: `GorillaEncoderV2.java`, `GorillaDecoderV2.java`

**C# Implementation Pattern**:
```csharp
public class GorillaEncoder : IEncoder
{
    private long _previousValue;
    private int _previousLeadingZeros;
    private int _previousTrailingZeros;
    private BitWriter _bitWriter = new();
    private bool _first = true;
    
    public void Encode(float value, MemoryStream stream)
    {
        long longValue = BitConverter.SingleToInt32Bits(value);
        if (_first)
        {
            _bitWriter.WriteBits(longValue, 32);
            _previousValue = longValue;
            _first = false;
        }
        else
        {
            EncodeValue(longValue, 32);
        }
    }
    
    private void EncodeValue(long value, int bitWidth)
    {
        long xor = value ^ _previousValue;
        if (xor == 0)
        {
            _bitWriter.WriteBit(0);
        }
        else
        {
            _bitWriter.WriteBit(1);
            int leadingZeros = CountLeadingZeros(xor, bitWidth);
            int trailingZeros = CountTrailingZeros(xor, bitWidth);
            
            if (leadingZeros >= _previousLeadingZeros && 
                trailingZeros >= _previousTrailingZeros)
            {
                _bitWriter.WriteBit(0);
                int meaningfulBits = bitWidth - _previousLeadingZeros - _previousTrailingZeros;
                _bitWriter.WriteBits(xor >> _previousTrailingZeros, meaningfulBits);
            }
            else
            {
                _bitWriter.WriteBit(1);
                _bitWriter.WriteBits(leadingZeros, 5);
                int meaningfulBits = bitWidth - leadingZeros - trailingZeros;
                _bitWriter.WriteBits(meaningfulBits, 6);
                _bitWriter.WriteBits(xor >> trailingZeros, meaningfulBits);
                
                _previousLeadingZeros = leadingZeros;
                _previousTrailingZeros = trailingZeros;
            }
        }
        _previousValue = value;
    }
}
```

**Helper Class Needed**:
```csharp
internal class BitWriter
{
    private readonly List<byte> _buffer = new();
    private int _bitOffset = 0;
    
    public void WriteBit(int bit)
    {
        if (_bitOffset == 0)
            _buffer.Add(0);
        
        if (bit != 0)
            _buffer[^1] |= (byte)(1 << (7 - _bitOffset));
        
        _bitOffset++;
        if (_bitOffset == 8)
            _bitOffset = 0;
    }
    
    public void WriteBits(long value, int numBits)
    {
        for (int i = numBits - 1; i >= 0; i--)
            WriteBit((int)((value >> i) & 1));
    }
    
    public byte[] ToArray() => _buffer.ToArray();
}
```

**Test Cases**:
- Similar consecutive floats (should compress well)
- Random floats (should not compress)
- Edge cases: zeros, NaN, Infinity
- Large datasets (1000+ values)

---

### 2. ZigZag Encoding

**Purpose**: Convert signed integers to unsigned, then use variable-length encoding (7 bits per byte).

**Algorithm**:
```
Encode: zigzag(n) = (n << 1) ^ (n >> 31)    // for int32
        zigzag(n) = (n << 1) ^ (n >> 63)    // for int64
        
Decode: n = (zigzag >>> 1) ^ -(zigzag & 1)
```

**Format**: `[count][varInt1][varInt2]...`

**Key Points**:
- Maps negative numbers to positive: -1 → 1, -2 → 3, 1 → 2, 2 → 4
- Then uses VarInt encoding (7 bits per byte, MSB = continuation bit)
- Very efficient for small absolute values

**Java Reference**: `IntZigzagEncoder.java`

**C# Implementation Pattern**:
```csharp
public class ZigZagEncoder : IEncoder
{
    private readonly List<int> _values = new();
    
    public void Encode(int value, MemoryStream stream)
    {
        _values.Add(value);
    }
    
    public void Flush(MemoryStream stream)
    {
        WriteVarInt(stream, _values.Count);
        foreach (var value in _values)
        {
            uint encoded = EncodeZigZag32(value);
            WriteVarUInt(stream, encoded);
        }
        _values.Clear();
    }
    
    private static uint EncodeZigZag32(int n)
    {
        return (uint)((n << 1) ^ (n >> 31));
    }
    
    private static void WriteVarUInt(Stream stream, uint value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }
}
```

---

### 3. Dictionary Encoding

**Purpose**: Map unique strings to integer indices for low-cardinality data.

**Format**: 
```
[dictionary_size]
[entry1_length][entry1_bytes]
[entry2_length][entry2_bytes]
...
[value_count]
[index1][index2]...  // as VarInts
```

**Key Points**:
- Build dictionary during encoding phase
- Write dictionary first, then indices
- Indices use VarInt encoding
- Best for categorical data (status codes, tags)

**Java Reference**: `DictionaryEncoder.java`

**C# Implementation Pattern**:
```csharp
public class DictionaryEncoder : IEncoder
{
    private readonly Dictionary<string, int> _dictionary = new();
    private readonly List<int> _indices = new();
    private int _nextIndex = 0;
    
    public void Encode(string value, MemoryStream stream)
    {
        if (!_dictionary.TryGetValue(value, out int index))
        {
            index = _nextIndex++;
            _dictionary[value] = index;
        }
        _indices.Add(index);
    }
    
    public void Flush(MemoryStream stream)
    {
        // Write dictionary
        WriteVarInt(stream, _dictionary.Count);
        foreach (var kvp in _dictionary.OrderBy(x => x.Value))
        {
            var bytes = Encoding.UTF8.GetBytes(kvp.Key);
            WriteVarInt(stream, bytes.Length);
            stream.Write(bytes);
        }
        
        // Write indices
        WriteVarInt(stream, _indices.Count);
        foreach (var index in _indices)
        {
            WriteVarInt(stream, index);
        }
    }
}
```

---

### 4. TS_2DIFF (Two-Differential) Encoding

**Purpose**: Store second-order deltas for monotonic sequences (especially timestamps).

**Algorithm**:
```
value[0] → stored as-is
delta[1] = value[1] - value[0]
delta[2] = value[2] - value[1]
...
ddelta[2] = delta[2] - delta[1]
ddelta[3] = delta[3] - delta[2]
...
```

**Format**:
```
[first_value]
[first_delta]
[ddelta1][ddelta2]...  // as VarInts or fixed-width
```

**Key Points**:
- Excellent for regular time intervals (e.g., every 100ms)
- Second deltas become zeros or very small
- Can combine with ZigZag for signed deltas
- Works on Int32, Int64, Float, Double (convert to bits)

**Java Reference**: `DeltaBinaryEncoder.java`

---

## Testing Strategy

For each encoding implementation:

1. **Round-trip tests**: Encode then decode, verify data integrity
2. **Edge cases**:
   - Empty data
   - Single value
   - All same values
   - Alternating values
   - Large datasets (1000+ values)
3. **Data type coverage**: Test all supported types
4. **Compression ratio**: Verify compression vs Plain
5. **Negative numbers**: Ensure correct handling
6. **Special values**: NaN, Infinity for floats/doubles

**Test Template**:
```csharp
[Fact]
public void Encoder_DataType_SuccessfulRoundTrip()
{
    var encoder = new XxxEncoder(dataType);
    var decoder = new XxxDecoder(dataType);
    var stream = new MemoryStream();
    
    var testData = new[] { /* test values */ };
    
    foreach (var value in testData)
        encoder.Encode(value, stream);
    encoder.Flush(stream);
    
    var encoded = stream.ToArray();
    int offset = 0;
    var decoded = new List<T>();
    for (int i = 0; i < testData.Length; i++)
        decoded.Add(decoder.ReadXxx(encoded, ref offset));
    
    Assert.Equal(testData.Length, decoded.Count);
    for (int i = 0; i < testData.Length; i++)
        Assert.Equal(testData[i], decoded[i]);
}
```

---

## Common Patterns

### IEncoder Implementation Checklist
- [ ] Implement all Encode() overloads (even if throwing NotSupportedException)
- [ ] Implement Flush() to write buffered data
- [ ] Implement GetOneItemMaxSize()
- [ ] Implement GetMaxByteSize()
- [ ] Add private helper methods for bit/byte manipulation
- [ ] Follow existing code style (PlainEncoder, RleEncoder as templates)

### IDecoder Implementation Checklist
- [ ] Implement all Read() methods
- [ ] Implement HasNext() to check remaining data
- [ ] Implement Reset() to clear state
- [ ] Handle offset tracking correctly (ref int offset)
- [ ] Match encoder format exactly

### Factory Updates
```csharp
// EncoderFactory.cs
TsEncoding.Xxx => new XxxEncoder(dataType),

// DecoderFactory.cs
TsEncoding.Xxx => new XxxDecoder(dataType),
```

---

## Performance Considerations

1. **Avoid unnecessary allocations**: Use ArrayPool<T> for large buffers
2. **Bit manipulation**: Use inline bit operations, avoid branches
3. **Buffer sizing**: Pre-allocate streams with estimated size
4. **Compression ratio**: Track encoded size vs original size
5. **Benchmark**: Compare against Plain encoding

---

## Resources

- **Java Implementation**: `java/tsfile/src/main/java/org/apache/tsfile/encoding/`
- **Gorilla Paper**: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
- **IoTDB Documentation**: https://iotdb.apache.org/UserGuide/latest/Basic-Concept/Encoding-and-Compression.html

---

## Implementation Order Recommendation

Based on priority and complexity:

1. ✅ **RLE** - Complete
2. **Gorilla** (3-4 days) - HIGH impact for time-series
3. **ZigZag** (1-2 days) - Simple, useful for IDs/counters
4. **Dictionary** (2 days) - Good for categorical data
5. **TS_2DIFF** (2-3 days) - Excellent for timestamps
6. **CHIMP** (3-4 days) - Similar to Gorilla
7. **SPRINTZ** (3-4 days) - Specialized for sensors
8. **RLBE** (2-3 days) - Specialized for specific patterns

**Total Estimate**: 2-3 weeks for Priority 1 & 2 encodings

---

Last Updated: 2026-02-02
