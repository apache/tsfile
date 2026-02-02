using System;
using System.Collections.Generic;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding.Decoder
{
    /// <summary>
    /// TS_2DIFF (Two-Differential) decoder for monotonic sequences.
    /// Reconstructs values from second-order deltas.
    /// </summary>
    public class Ts2DiffDecoder : IDecoder
    {
        private readonly TsDataType _dataType;
        private readonly Queue<long> _values = new();
        private int _count = 0;
        private bool _initialized = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="Ts2DiffDecoder"/> class.
        /// </summary>
        /// <param name="dataType">The data type (Int32, Int64, Float, Double)</param>
        public Ts2DiffDecoder(TsDataType dataType)
        {
            if (dataType != TsDataType.Int32 && dataType != TsDataType.Int64 &&
                dataType != TsDataType.Float && dataType != TsDataType.Double)
            {
                throw new ArgumentException($"TS_2DIFF decoding only supports Int32/Int64/Float/Double, got {dataType}");
            }
            _dataType = dataType;
        }

        public bool ReadBoolean(byte[] data, ref int offset)
        {
            throw new NotSupportedException("TS_2DIFF decoding does not support boolean");
        }

        public int ReadInt(byte[] data, ref int offset)
        {
            EnsureInitialized(data, ref offset);
            if (_values.Count == 0)
            {
                throw new InvalidOperationException("No more values to read");
            }
            return (int)_values.Dequeue();
        }

        public long ReadLong(byte[] data, ref int offset)
        {
            EnsureInitialized(data, ref offset);
            if (_values.Count == 0)
            {
                throw new InvalidOperationException("No more values to read");
            }
            return _values.Dequeue();
        }

        public float ReadFloat(byte[] data, ref int offset)
        {
            EnsureInitialized(data, ref offset);
            if (_values.Count == 0)
            {
                throw new InvalidOperationException("No more values to read");
            }
            int bits = (int)_values.Dequeue();
            return BitConverter.Int32BitsToSingle(bits);
        }

        public double ReadDouble(byte[] data, ref int offset)
        {
            EnsureInitialized(data, ref offset);
            if (_values.Count == 0)
            {
                throw new InvalidOperationException("No more values to read");
            }
            long bits = _values.Dequeue();
            return BitConverter.Int64BitsToDouble(bits);
        }

        public byte[] ReadBytes(byte[] data, ref int offset)
        {
            throw new NotSupportedException("TS_2DIFF decoding does not support byte arrays");
        }

        public string ReadString(byte[] data, ref int offset)
        {
            throw new NotSupportedException("TS_2DIFF decoding does not support strings");
        }

        public bool HasNext(byte[] data, int offset)
        {
            return _values.Count > 0 || (!_initialized && offset < data.Length);
        }

        public void Reset()
        {
            _values.Clear();
            _count = 0;
            _initialized = false;
        }

        private void EnsureInitialized(byte[] data, ref int offset)
        {
            if (_initialized)
            {
                return;
            }

            _count = ReadVarInt(data, ref offset);

            if (_count == 0)
            {
                _initialized = true;
                return;
            }

            // Read first value
            long firstValue = ReadValue(data, ref offset);
            _values.Enqueue(firstValue);

            if (_count == 1)
            {
                _initialized = true;
                return;
            }

            // Read first delta
            long firstDelta = ReadZigZag(data, ref offset);
            long secondValue = firstValue + firstDelta;
            _values.Enqueue(secondValue);

            if (_count == 2)
            {
                _initialized = true;
                return;
            }

            // Reconstruct remaining values from second deltas
            long previousValue = secondValue;
            long previousDelta = firstDelta;

            for (int i = 2; i < _count; i++)
            {
                long secondDelta = ReadZigZag(data, ref offset);
                long currentDelta = previousDelta + secondDelta;
                long currentValue = previousValue + currentDelta;
                
                _values.Enqueue(currentValue);
                
                previousValue = currentValue;
                previousDelta = currentDelta;
            }

            _initialized = true;
        }

        private long ReadValue(byte[] data, ref int offset)
        {
            if (_dataType == TsDataType.Int32 || _dataType == TsDataType.Float)
            {
                // Read as 32-bit
                int value = data[offset]
                    | (data[offset + 1] << 8)
                    | (data[offset + 2] << 16)
                    | (data[offset + 3] << 24);
                offset += 4;
                return value;
            }
            else
            {
                // Read as 64-bit
                long value = data[offset]
                    | ((long)data[offset + 1] << 8)
                    | ((long)data[offset + 2] << 16)
                    | ((long)data[offset + 3] << 24)
                    | ((long)data[offset + 4] << 32)
                    | ((long)data[offset + 5] << 40)
                    | ((long)data[offset + 6] << 48)
                    | ((long)data[offset + 7] << 56);
                offset += 8;
                return value;
            }
        }

        private int ReadVarInt(byte[] data, ref int offset)
        {
            uint result = 0;
            int shift = 0;

            while (offset < data.Length)
            {
                byte b = data[offset++];
                result |= (uint)(b & 0x7F) << shift;

                if ((b & 0x80) == 0)
                {
                    return (int)result;
                }

                shift += 7;
            }

            throw new InvalidOperationException("Incomplete VarInt in data");
        }

        private long ReadZigZag(byte[] data, ref int offset)
        {
            // Read VarInt
            ulong encoded = 0;
            int shift = 0;

            while (offset < data.Length)
            {
                byte b = data[offset++];
                encoded |= (ulong)(b & 0x7F) << shift;

                if ((b & 0x80) == 0)
                {
                    break;
                }

                shift += 7;
            }

            // ZigZag decode: (n >>> 1) ^ -(n & 1)
            return (long)(encoded >> 1) ^ -(long)(encoded & 1);
        }
    }
}
