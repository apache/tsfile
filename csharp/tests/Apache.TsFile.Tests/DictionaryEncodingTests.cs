using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using Apache.TsFile.Encoding.Encoder;
using Apache.TsFile.Encoding.Decoder;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Tests
{
    /// <summary>
    /// Tests for Dictionary encoding/decoding functionality.
    /// </summary>
    public class DictionaryEncodingTests
    {
        [Fact]
        public void DictionaryEncoder_StringRepeated_SuccessfulRoundTrip()
        {
            var encoder = new DictionaryEncoder(TsDataType.String);
            var decoder = new DictionaryDecoder(TsDataType.String);
            var stream = new MemoryStream();

            var testData = new[] { "active", "inactive", "active", "active", "pending", "inactive" };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"Dictionary encoded {testData.Length} strings to {encoded.Length} bytes");

            int offset = 0;
            var decoded = new List<string>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadString(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }

            // Should compress well: 3 unique strings, 6 total values
            Assert.True(encoded.Length < testData.Sum(s => s.Length), "Dictionary encoding should compress repeated strings");
        }

        [Fact]
        public void DictionaryEncoder_TextUnique_SuccessfulRoundTrip()
        {
            var encoder = new DictionaryEncoder(TsDataType.Text);
            var decoder = new DictionaryDecoder(TsDataType.Text);
            var stream = new MemoryStream();

            var testData = new[] { "first", "second", "third", "fourth" };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"Dictionary encoded {testData.Length} unique strings to {encoded.Length} bytes");

            int offset = 0;
            var decoded = new List<string>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadString(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }
        }

        [Fact]
        public void DictionaryEncoder_CategoryData_HighCompression()
        {
            var encoder = new DictionaryEncoder(TsDataType.String);
            var decoder = new DictionaryDecoder(TsDataType.String);
            var stream = new MemoryStream();

            // Simulate categorical status codes
            var statuses = new[] { "OK", "ERROR", "WARNING", "INFO" };
            var testData = new List<string>();
            for (int i = 0; i < 100; i++)
            {
                testData.Add(statuses[i % statuses.Length]);
            }

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"Dictionary encoded 100 categorical values to {encoded.Length} bytes");
            Console.WriteLine($"Compression ratio: {(100.0 * testData.Sum(s => s.Length)) / encoded.Length:F2}x");

            int offset = 0;
            var decoded = new List<string>();
            for (int i = 0; i < testData.Count; i++)
            {
                decoded.Add(decoder.ReadString(encoded, ref offset));
            }

            Assert.Equal(testData.Count, decoded.Count);
            for (int i = 0; i < testData.Count; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }

            // With only 4 unique values and 100 total, should compress very well
            Assert.True(encoded.Length < testData.Sum(s => s.Length) / 2, 
                "Dictionary should achieve >2x compression on low-cardinality data");
        }

        [Fact]
        public void DictionaryEncoder_SingleValue_SuccessfulRoundTrip()
        {
            var encoder = new DictionaryEncoder(TsDataType.String);
            var decoder = new DictionaryDecoder(TsDataType.String);
            var stream = new MemoryStream();

            var testData = new[] { "only_value" };

            encoder.Encode(testData[0], stream);
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            int offset = 0;
            var decoded = decoder.ReadString(encoded, ref offset);

            Assert.Equal(testData[0], decoded);
        }

        [Fact]
        public void DictionaryEncoder_EmptyStrings_SuccessfulRoundTrip()
        {
            var encoder = new DictionaryEncoder(TsDataType.String);
            var decoder = new DictionaryDecoder(TsDataType.String);
            var stream = new MemoryStream();

            var testData = new[] { "", "value", "", "" };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            int offset = 0;
            var decoded = new List<string>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadString(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }
        }

        [Fact]
        public void DictionaryEncoder_LongStrings_SuccessfulRoundTrip()
        {
            var encoder = new DictionaryEncoder(TsDataType.String);
            var decoder = new DictionaryDecoder(TsDataType.String);
            var stream = new MemoryStream();

            var testData = new[] 
            { 
                new string('A', 100),
                new string('B', 100),
                new string('A', 100),
                new string('A', 100)
            };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"Dictionary encoded 4 long strings (2 unique) to {encoded.Length} bytes");

            int offset = 0;
            var decoded = new List<string>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadString(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }
        }

        [Fact]
        public void DictionaryEncoder_Factory_Integration()
        {
            var encoder = Apache.TsFile.Encoding.EncoderFactory.CreateEncoder(
                TsEncoding.Dictionary, TsDataType.String);
            var decoder = Apache.TsFile.Encoding.DecoderFactory.CreateDecoder(
                TsEncoding.Dictionary, TsDataType.String);

            Assert.IsType<DictionaryEncoder>(encoder);
            Assert.IsType<DictionaryDecoder>(decoder);

            var stream = new MemoryStream();
            var testData = new[] { "test1", "test2", "test1" };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            int offset = 0;
            var decoded = new List<string>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadString(encoded, ref offset));
            }

            Assert.Equal(testData, decoded);
        }

        [Fact]
        public void DictionaryEncoder_UnsupportedType_ThrowsException()
        {
            Assert.Throws<ArgumentException>(() => new DictionaryEncoder(TsDataType.Int32));
            Assert.Throws<ArgumentException>(() => new DictionaryEncoder(TsDataType.Float));
            Assert.Throws<ArgumentException>(() => new DictionaryDecoder(TsDataType.Double));
        }
    }
}
