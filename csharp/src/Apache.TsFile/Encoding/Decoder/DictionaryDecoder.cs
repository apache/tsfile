using System;
using System.Collections.Generic;
using System.Text;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding.Decoder
{
    /// <summary>
    /// Dictionary decoder for low-cardinality string data.
    /// Reads dictionary and maps indices back to original strings.
    /// </summary>
    public class DictionaryDecoder : IDecoder
    {
        private readonly TsDataType _dataType;
        private readonly List<string> _dictionary = new();
        private readonly Queue<string> _values = new();
        private bool _dictionaryLoaded = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryDecoder"/> class.
        /// </summary>
        /// <param name="dataType">The data type (must be Text or String)</param>
        public DictionaryDecoder(TsDataType dataType)
        {
            if (dataType != TsDataType.Text && dataType != TsDataType.String)
            {
                throw new ArgumentException($"Dictionary decoding only supports Text/String, got {dataType}");
            }
            _dataType = dataType;
        }

        public bool ReadBoolean(byte[] data, ref int offset)
        {
            throw new NotSupportedException("Dictionary decoding does not support boolean");
        }

        public int ReadInt(byte[] data, ref int offset)
        {
            throw new NotSupportedException("Dictionary decoding does not support int");
        }

        public long ReadLong(byte[] data, ref int offset)
        {
            throw new NotSupportedException("Dictionary decoding does not support long");
        }

        public float ReadFloat(byte[] data, ref int offset)
        {
            throw new NotSupportedException("Dictionary decoding does not support float");
        }

        public double ReadDouble(byte[] data, ref int offset)
        {
            throw new NotSupportedException("Dictionary decoding does not support double");
        }

        public byte[] ReadBytes(byte[] data, ref int offset)
        {
            var str = ReadStringInternal(data, ref offset);
            return System.Text.Encoding.UTF8.GetBytes(str);
        }

        public string ReadString(byte[] data, ref int offset)
        {
            return ReadStringInternal(data, ref offset);
        }

        private string ReadStringInternal(byte[] data, ref int offset)
        {
            // Load dictionary if not already loaded
            if (!_dictionaryLoaded)
            {
                LoadDictionary(data, ref offset);
            }

            // If queue is empty, need to load values
            if (_values.Count == 0)
            {
                LoadValues(data, ref offset);
            }

            return _values.Dequeue();
        }

        private void LoadDictionary(byte[] data, ref int offset)
        {
            _dictionary.Clear();
            
            int dictSize = ReadVarInt(data, ref offset);
            for (int i = 0; i < dictSize; i++)
            {
                int length = ReadVarInt(data, ref offset);
                string str = System.Text.Encoding.UTF8.GetString(data, offset, length);
                offset += length;
                _dictionary.Add(str);
            }
            
            _dictionaryLoaded = true;
        }

        private void LoadValues(byte[] data, ref int offset)
        {
            int valueCount = ReadVarInt(data, ref offset);
            for (int i = 0; i < valueCount; i++)
            {
                int index = ReadVarInt(data, ref offset);
                if (index < 0 || index >= _dictionary.Count)
                {
                    throw new InvalidOperationException($"Invalid dictionary index: {index}");
                }
                _values.Enqueue(_dictionary[index]);
            }
        }

        public bool HasNext(byte[] data, int offset)
        {
            if (_values.Count > 0)
            {
                return true;
            }

            if (!_dictionaryLoaded && offset < data.Length)
            {
                return true;
            }

            return offset < data.Length;
        }

        public void Reset()
        {
            _dictionary.Clear();
            _values.Clear();
            _dictionaryLoaded = false;
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
    }
}
