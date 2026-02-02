using System;
using System.Collections.Generic;
using System.IO;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding.Encoder
{
    /// <summary>
    /// TS_2DIFF (Two-Differential) encoder for monotonic sequences.
    /// Stores second-order deltas for efficient compression of regular timestamps and sequences.
    /// Format: [count][first_value][first_delta][second_deltas...]
    /// </summary>
    /// <remarks>
    /// Excellent for regular time intervals (e.g., every 100ms) where second deltas become zeros or very small.
    /// Works on Int32, Int64, Float, Double (converts to bit patterns).
    /// </remarks>
    public class Ts2DiffEncoder : IEncoder
    {
        private readonly TsDataType _dataType;
        private readonly List<long> _values = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="Ts2DiffEncoder"/> class.
        /// </summary>
        /// <param name="dataType">The data type (Int32, Int64, Float, Double)</param>
        public Ts2DiffEncoder(TsDataType dataType)
        {
            if (dataType != TsDataType.Int32 && dataType != TsDataType.Int64 &&
                dataType != TsDataType.Float && dataType != TsDataType.Double)
            {
                throw new ArgumentException($"TS_2DIFF encoding only supports Int32/Int64/Float/Double, got {dataType}");
            }
            _dataType = dataType;
        }

        public void Encode(bool value, MemoryStream stream)
        {
            throw new NotSupportedException("TS_2DIFF encoding does not support boolean");
        }

        public void Encode(int value, MemoryStream stream)
        {
            _values.Add(value);
        }

        public void Encode(long value, MemoryStream stream)
        {
            _values.Add(value);
        }

        public void Encode(float value, MemoryStream stream)
        {
            _values.Add(BitConverter.SingleToInt32Bits(value));
        }

        public void Encode(double value, MemoryStream stream)
        {
            _values.Add(BitConverter.DoubleToInt64Bits(value));
        }

        public void Encode(byte[] value, MemoryStream stream)
        {
            throw new NotSupportedException("TS_2DIFF encoding does not support byte arrays");
        }

        public void Encode(string value, MemoryStream stream)
        {
            throw new NotSupportedException("TS_2DIFF encoding does not support strings");
        }

        public void Flush(MemoryStream stream)
        {
            if (_values.Count == 0)
            {
                WriteVarInt(stream, 0);
                return;
            }

            WriteVarInt(stream, _values.Count);

            if (_values.Count == 1)
            {
                WriteValue(stream, _values[0]);
                _values.Clear();
                return;
            }

            // Write first value
            WriteValue(stream, _values[0]);

            // Calculate first delta
            long previousValue = _values[0];
            long currentValue = _values[1];
            long firstDelta = currentValue - previousValue;
            WriteZigZag(stream, firstDelta);

            if (_values.Count == 2)
            {
                _values.Clear();
                return;
            }

            // Calculate and write second deltas
            long previousDelta = firstDelta;
            for (int i = 2; i < _values.Count; i++)
            {
                previousValue = currentValue;
                currentValue = _values[i];
                long currentDelta = currentValue - previousValue;
                long secondDelta = currentDelta - previousDelta;
                
                WriteZigZag(stream, secondDelta);
                previousDelta = currentDelta;
            }

            _values.Clear();
        }

        public int GetOneItemMaxSize()
        {
            return 12; // Max size for zigzag-encoded long
        }

        public long GetMaxByteSize()
        {
            // count + first_value + (n-1) deltas
            int countSize = 5;
            int firstValueSize = (_dataType == TsDataType.Int32 || _dataType == TsDataType.Float) ? 4 : 8;
            int deltasSize = _values.Count * 12; // Max zigzag size per delta
            return countSize + firstValueSize + deltasSize;
        }

        private void WriteValue(Stream stream, long value)
        {
            if (_dataType == TsDataType.Int32 || _dataType == TsDataType.Float)
            {
                // Write as 32-bit
                int intValue = (int)value;
                stream.WriteByte((byte)(intValue));
                stream.WriteByte((byte)(intValue >> 8));
                stream.WriteByte((byte)(intValue >> 16));
                stream.WriteByte((byte)(intValue >> 24));
            }
            else
            {
                // Write as 64-bit
                stream.WriteByte((byte)(value));
                stream.WriteByte((byte)(value >> 8));
                stream.WriteByte((byte)(value >> 16));
                stream.WriteByte((byte)(value >> 24));
                stream.WriteByte((byte)(value >> 32));
                stream.WriteByte((byte)(value >> 40));
                stream.WriteByte((byte)(value >> 48));
                stream.WriteByte((byte)(value >> 56));
            }
        }

        private void WriteVarInt(Stream stream, int value)
        {
            uint uvalue = (uint)value;
            while (uvalue >= 0x80)
            {
                stream.WriteByte((byte)(uvalue | 0x80));
                uvalue >>= 7;
            }
            stream.WriteByte((byte)uvalue);
        }

        private void WriteZigZag(Stream stream, long value)
        {
            // ZigZag encoding: (n << 1) ^ (n >> 63)
            ulong encoded = (ulong)((value << 1) ^ (value >> 63));
            
            // VarInt encoding
            while (encoded >= 0x80)
            {
                stream.WriteByte((byte)(encoded | 0x80));
                encoded >>= 7;
            }
            stream.WriteByte((byte)encoded);
        }
    }
}
