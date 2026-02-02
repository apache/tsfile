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
    /// Tests for TS_2DIFF (Two-Differential) encoding/decoding functionality.
    /// </summary>
    public class Ts2DiffEncodingTests
    {
        [Fact]
        public void Ts2DiffEncoder_Int32Regular_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Int32);
            var decoder = new Ts2DiffDecoder(TsDataType.Int32);
            var stream = new MemoryStream();

            // Regular interval: 100, 200, 300, 400, ... (delta=100, second_delta=0)
            var testData = new int[10];
            for (int i = 0; i < testData.Length; i++)
            {
                testData[i] = 100 + i * 100;
            }

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"TS_2DIFF encoded {testData.Length} regular int32 values to {encoded.Length} bytes");

            int offset = 0;
            var decoded = new List<int>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadInt(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }

            // Should compress excellently: regular intervals mean second deltas are all 0
            Assert.True(encoded.Length < testData.Length * 4 / 2,
                "TS_2DIFF should achieve >2x compression on regular intervals");
        }

        [Fact]
        public void Ts2DiffEncoder_Int64Timestamps_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Int64);
            var decoder = new Ts2DiffDecoder(TsDataType.Int64);
            var stream = new MemoryStream();

            // Simulated timestamps at regular 1-second intervals
            long baseTimestamp = 1609459200000L; // 2021-01-01 00:00:00
            var testData = new long[100];
            for (int i = 0; i < testData.Length; i++)
            {
                testData[i] = baseTimestamp + i * 1000L;
            }

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"TS_2DIFF encoded 100 timestamp values to {encoded.Length} bytes");
            Console.WriteLine($"Compression ratio: {(100.0 * 8) / encoded.Length:F2}x");

            int offset = 0;
            var decoded = new List<long>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadLong(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }
        }

        [Fact]
        public void Ts2DiffEncoder_FloatSensor_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Float);
            var decoder = new Ts2DiffDecoder(TsDataType.Float);
            var stream = new MemoryStream();

            // Slowly changing sensor values
            var testData = new float[20];
            for (int i = 0; i < testData.Length; i++)
            {
                testData[i] = 20.0f + i * 0.1f;
            }

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"TS_2DIFF encoded {testData.Length} float values to {encoded.Length} bytes");

            int offset = 0;
            var decoded = new List<float>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadFloat(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i], 5); // Allow small precision difference
            }
        }

        [Fact]
        public void Ts2DiffEncoder_DoubleSequence_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Double);
            var decoder = new Ts2DiffDecoder(TsDataType.Double);
            var stream = new MemoryStream();

            // Monotonic sequence with varying but predictable deltas
            var testData = new double[15];
            testData[0] = 1.0;
            testData[1] = 2.0;
            for (int i = 2; i < testData.Length; i++)
            {
                testData[i] = testData[i - 1] + (testData[i - 1] - testData[i - 2]);
            }

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"TS_2DIFF encoded {testData.Length} double values to {encoded.Length} bytes");

            int offset = 0;
            var decoded = new List<double>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadDouble(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i], 10); // Allow small precision difference
            }
        }

        [Fact]
        public void Ts2DiffEncoder_SingleValue_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Int32);
            var decoder = new Ts2DiffDecoder(TsDataType.Int32);
            var stream = new MemoryStream();

            encoder.Encode(42, stream);
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            int offset = 0;
            var decoded = decoder.ReadInt(encoded, ref offset);

            Assert.Equal(42, decoded);
        }

        [Fact]
        public void Ts2DiffEncoder_TwoValues_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Int64);
            var decoder = new Ts2DiffDecoder(TsDataType.Int64);
            var stream = new MemoryStream();

            var testData = new long[] { 1000L, 2000L };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            int offset = 0;
            var decoded = new List<long>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadLong(encoded, ref offset));
            }

            Assert.Equal(testData, decoded);
        }

        [Fact]
        public void Ts2DiffEncoder_IrregularIntervals_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Int32);
            var decoder = new Ts2DiffDecoder(TsDataType.Int32);
            var stream = new MemoryStream();

            // Irregular intervals (won't compress as well, but should still work)
            var testData = new int[] { 100, 105, 120, 122, 135, 140 };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"TS_2DIFF encoded {testData.Length} irregular values to {encoded.Length} bytes");

            int offset = 0;
            var decoded = new List<int>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadInt(encoded, ref offset));
            }

            Assert.Equal(testData, decoded);
        }

        [Fact]
        public void Ts2DiffEncoder_NegativeDeltas_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Int32);
            var decoder = new Ts2DiffDecoder(TsDataType.Int32);
            var stream = new MemoryStream();

            // Decreasing sequence
            var testData = new int[] { 1000, 900, 800, 700, 600, 500 };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            int offset = 0;
            var decoded = new List<int>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadInt(encoded, ref offset));
            }

            Assert.Equal(testData, decoded);
        }

        [Fact]
        public void Ts2DiffEncoder_LargeDataset_SuccessfulRoundTrip()
        {
            var encoder = new Ts2DiffEncoder(TsDataType.Int64);
            var decoder = new Ts2DiffDecoder(TsDataType.Int64);
            var stream = new MemoryStream();

            // Large dataset with regular pattern
            var testData = new long[1000];
            for (int i = 0; i < testData.Length; i++)
            {
                testData[i] = i * 1000L;
            }

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            Console.WriteLine($"TS_2DIFF encoded 1000 int64 values to {encoded.Length} bytes");
            Console.WriteLine($"Compression ratio: {(1000.0 * 8) / encoded.Length:F2}x");

            int offset = 0;
            var decoded = new List<long>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadLong(encoded, ref offset));
            }

            Assert.Equal(testData.Length, decoded.Count);
            for (int i = 0; i < testData.Length; i++)
            {
                Assert.Equal(testData[i], decoded[i]);
            }

            // Should achieve excellent compression on regular data
            Assert.True(encoded.Length < testData.Length * 8 / 4,
                "TS_2DIFF should achieve >4x compression on 1000 regular int64 values");
        }

        [Fact]
        public void Ts2DiffEncoder_Factory_Integration()
        {
            var encoder = Apache.TsFile.Encoding.EncoderFactory.CreateEncoder(
                TsEncoding.Ts2Diff, TsDataType.Int32);
            var decoder = Apache.TsFile.Encoding.DecoderFactory.CreateDecoder(
                TsEncoding.Ts2Diff, TsDataType.Int32);

            Assert.IsType<Ts2DiffEncoder>(encoder);
            Assert.IsType<Ts2DiffDecoder>(decoder);

            var stream = new MemoryStream();
            var testData = new int[] { 100, 200, 300, 400 };

            foreach (var value in testData)
            {
                encoder.Encode(value, stream);
            }
            encoder.Flush(stream);

            var encoded = stream.ToArray();
            int offset = 0;
            var decoded = new List<int>();
            for (int i = 0; i < testData.Length; i++)
            {
                decoded.Add(decoder.ReadInt(encoded, ref offset));
            }

            Assert.Equal(testData, decoded);
        }

        [Fact]
        public void Ts2DiffEncoder_UnsupportedType_ThrowsException()
        {
            Assert.Throws<ArgumentException>(() => new Ts2DiffEncoder(TsDataType.Boolean));
            Assert.Throws<ArgumentException>(() => new Ts2DiffEncoder(TsDataType.Text));
            Assert.Throws<ArgumentException>(() => new Ts2DiffDecoder(TsDataType.String));
        }
    }
}
