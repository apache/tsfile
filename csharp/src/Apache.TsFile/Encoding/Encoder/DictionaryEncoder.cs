using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding.Encoder
{
    /// <summary>
    /// Dictionary encoder for low-cardinality string data.
    /// Maps unique strings to integer indices for efficient storage.
    /// Format: [dict_size][entry1_len][entry1_data]...[value_count][index1][index2]...
    /// </summary>
    /// <remarks>
    /// Best for categorical data with few unique values (status codes, tags, categories).
    /// Not recommended for high-cardinality data where dictionary size becomes large.
    /// </remarks>
    public class DictionaryEncoder : IEncoder
    {
        private readonly TsDataType _dataType;
        private readonly Dictionary<string, int> _dictionary = new();
        private readonly List<int> _indices = new();
        private int _nextIndex = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryEncoder"/> class.
        /// </summary>
        /// <param name="dataType">The data type (must be Text or String)</param>
        public DictionaryEncoder(TsDataType dataType)
        {
            if (dataType != TsDataType.Text && dataType != TsDataType.String)
            {
                throw new ArgumentException($"Dictionary encoding only supports Text/String, got {dataType}");
            }
            _dataType = dataType;
        }

        public void Encode(bool value, MemoryStream stream)
        {
            throw new NotSupportedException("Dictionary encoding does not support boolean");
        }

        public void Encode(int value, MemoryStream stream)
        {
            throw new NotSupportedException("Dictionary encoding does not support int");
        }

        public void Encode(long value, MemoryStream stream)
        {
            throw new NotSupportedException("Dictionary encoding does not support long");
        }

        public void Encode(float value, MemoryStream stream)
        {
            throw new NotSupportedException("Dictionary encoding does not support float");
        }

        public void Encode(double value, MemoryStream stream)
        {
            throw new NotSupportedException("Dictionary encoding does not support double");
        }

        public void Encode(byte[] value, MemoryStream stream)
        {
            string str = System.Text.Encoding.UTF8.GetString(value);
            EncodeString(str);
        }

        public void Encode(string value, MemoryStream stream)
        {
            EncodeString(value);
        }

        private void EncodeString(string value)
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
            
            // Sort by index to maintain order
            foreach (var kvp in _dictionary.OrderBy(x => x.Value))
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(kvp.Key);
                WriteVarInt(stream, bytes.Length);
                stream.Write(bytes, 0, bytes.Length);
            }
            
            // Write indices
            WriteVarInt(stream, _indices.Count);
            foreach (var index in _indices)
            {
                WriteVarInt(stream, index);
            }
            
            // Clear for next batch
            _dictionary.Clear();
            _indices.Clear();
            _nextIndex = 0;
        }

        public int GetOneItemMaxSize()
        {
            // Worst case: each unique string + index
            // Conservatively estimate 100 bytes per string + overhead
            return 100 + 10; // string data + VarInt overhead
        }

        public long GetMaxByteSize()
        {
            // Dictionary size + entries + indices
            int dictionaryOverhead = 5; // VarInt for dict size
            int stringDataSize = _dictionary.Sum(kvp => System.Text.Encoding.UTF8.GetByteCount(kvp.Key) + 5); // +5 for length VarInt
            int indicesSize = _indices.Count * 5; // VarInt per index
            return dictionaryOverhead + stringDataSize + indicesSize + 5; // +5 for value count
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
    }
}
