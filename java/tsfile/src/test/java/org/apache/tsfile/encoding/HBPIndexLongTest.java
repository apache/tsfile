package org.apache.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.junit.Assume;
import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class HBPIndexLongTest {

    private static final List<String> IGNORE_FILES = Arrays.asList(
            ".DS_Store", "full_data", "test.csv", "POI-lat.csv", "POI-lon.csv",
            "Basel-wind.csv", "Basel-temp.csv", "Air-sensor.csv");

    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static void long2Bytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 56);
        cur_byte[encode_pos + 1] = (byte) (integer >> 48);
        cur_byte[encode_pos + 2] = (byte) (integer >> 40);
        cur_byte[encode_pos + 3] = (byte) (integer >> 32);
        cur_byte[encode_pos + 4] = (byte) (integer >> 24);
        cur_byte[encode_pos + 5] = (byte) (integer >> 16);
        cur_byte[encode_pos + 6] = (byte) (integer >> 8);
        cur_byte[encode_pos + 7] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static long bytes2Long(byte[] encoded, int start, int num) {
        long value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static int bitWidth(long value) {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    public static int BlockEncoder(long[] data, int block_index, int block_size, int remainder,
            int encode_pos, ArrayList<HBPIndexLong> indexList, byte[] encoded_result) {

        long[] block_data = new long[remainder];
        System.arraycopy(data, block_index * block_size, block_data, 0, remainder);

        long min_value = Long.MAX_VALUE;
        long max_value = Long.MIN_VALUE;
        for (long value : block_data) {
            if (value < min_value) {
                min_value = value;
            }
            if (value > max_value) {
                max_value = value;
            }
        }

        for (int i = 0; i < remainder; i++) {
            block_data[i] -= min_value;
        }

        long2Bytes(min_value, encode_pos, encoded_result);
        encode_pos += 8;

        int bw = bitWidth(max_value - min_value);

        int2Bytes(bw, encode_pos, encoded_result);
        encode_pos += 4;

        HBPIndexLong idx = new HBPIndexLong(bw, block_data);
        indexList.add(idx);

        return encode_pos;

    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, ArrayList<HBPIndexLong> indexList, long[] data) {

        long min_value = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int bw = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        HBPIndexLong idx = indexList.get(block_index);

        for (int i = 0; i < remainder; i++) {
            long value = idx.getCode(i);

            data[block_index * block_size + i] = value + min_value;
        }

        return encode_pos;

    }

    public static int Encoder(long[] data, int block_size, ArrayList<HBPIndexLong> indexList, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, indexList, encoded_result);
        }

        encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos, indexList,
                encoded_result);

        return encode_pos;
    }

    public static long[] Decoder(byte[] encoded_result, ArrayList<HBPIndexLong> indexList) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        long[] data = new long[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, indexList, data);
        }

        int remainder = data_length % block_size;

        encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                encode_pos, indexList, data);

        return data;
    }

    public static int getDecimalPrecision(String str) {
        int decimalIndex = str.indexOf(".");

        if (decimalIndex == -1) {
            return 0;
        }

        return str.substring(decimalIndex + 1).length();
    }

    public static String extractFileName(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }

        File file = new File(path);
        String fileName = file.getName();

        int dotIndex = fileName.lastIndexOf('.');

        if (dotIndex == -1 || dotIndex == 0) {
            return fileName;
        }

        return fileName.substring(0, dotIndex);
    }

    @Test
    public void test0() throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/ElfTestData_camel";
        String outputDirStr = "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/output_Bitweaving";
        int block_size = 512;
        int repeatTime = 50;

        File dir = new File(directory);
        Assume.assumeTrue(
                "Skip test0: dataset directory missing: " + directory,
                dir.exists() && dir.isDirectory()
        );

        File outputDir = new File(outputDirStr);
        if (!outputDir.exists()) outputDir.mkdir();

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
            if (!file.getName().toLowerCase().endsWith(".csv")) continue;

            System.out.println(file.getName());
            String outputPath = outputDirStr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Throughput (MB/s)",
                    "Decoding Throughput (MB/s)",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            List<String> numbers = new ArrayList<>();
            int max_decimal = 0;
            CsvReader loader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (loader.readRecord()) {
                for (String value : loader.getValues()) {
                    String numStr = value.trim();
                    if (numStr.isEmpty()) continue;
                    numbers.add(numStr);
                    int cur = getDecimalPrecision(numStr);
                    if (cur > max_decimal) max_decimal = cur;
                }
            }
            loader.close();
            if (max_decimal > 17) max_decimal = 17;

            ArrayList<Double> data1 = new ArrayList<>();
            for (String numStr : numbers) {
                data1.add(Double.valueOf(numStr));
            }

            long max_mul = (long) Math.pow(10, max_decimal);
            long[] data2_arr = new long[data1.size()];
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (long) (data1.get(i) * max_mul);
            }

            byte[] encoded_result = new byte[data2_arr.length * 8];
            ArrayList<HBPIndexLong> indexList = new ArrayList<>();

            long s = System.nanoTime();
            int length = 0;
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                indexList.clear();
                length = Encoder(data2_arr, block_size, indexList, encoded_result);
            }
            long e = System.nanoTime();
            long encodeTimeNs = (e - s) / repeatTime;

            long compressedBytes = length;
            for (HBPIndexLong idx : indexList) {
                compressedBytes += idx.segments * (idx.k + 1) * Long.BYTES;
            }
            long compressedSizeBits = compressedBytes * 8L;
            double ratio = (double) compressedSizeBits / (double) (data1.size() * 64L);

            long[] data2_arr_decoded = new long[data2_arr.length];
            s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result, indexList);
            }
            e = System.nanoTime();
            long decodeTimeNs = (e - s) / repeatTime;

            double encodeThroughput = (double) (data1.size() * 8000L) / (double) encodeTimeNs;
            double decodeThroughput = (double) (data1.size() * 8000L) / (double) decodeTimeNs;

            String[] record = {
                    file.toString(),
                    "HBP",
                    String.valueOf(encodeThroughput),
                    String.valueOf(decodeThroughput),
                    String.valueOf(data1.size()),
                    String.valueOf(compressedSizeBits),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            writer.close();

            System.out.println("Encoding throughput: " + encodeThroughput + " MB/s, Decoding throughput: " + decodeThroughput + " MB/s, Ratio: " + ratio);
        }
    }

}
