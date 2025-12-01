package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import org.openjdk.jol.info.ClassLayout;
//import org.openjdk.jol.info.GraphLayout;

public class FSprintz512 {
    private static final List<String> IGNORE_FILES = Arrays.asList(".DS_Store", "full_data","test.csv","POI-lat.csv",
            "POI-lon.csv","Air-sensor.csv","Basel-wind.csv","Basel-temp.csv");
//    private static final List<String> IGNORE_FILES = Arrays.asList(".DS_Store", "full_data","test.csv");
    private static final int CHUNK_SIZE = 1024;
    // 轻量级Octad表示

    public static int getBitWith(int num) {
        if (num == 0)
            return 1;
        else
            return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static int getCount(long long1, int mask) {
        return ((int) (long1 & mask));
    }

    public static int getUniqueValue(long long1, int left_shift) {
        return ((int) ((long1) >> left_shift));
    }

    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static void intByte2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    private static void long2intBytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    private static long bytesLong2Integer(byte[] encoded, int decode_pos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decode_pos] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,
                                   byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values.get(valueIdx) << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 4; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }

    }

    public static void unpack8Values(byte[] encoded, int offset, int width, ArrayList<Integer> result_list) {
        int byteIdx = offset;
        long buffer = 0;
        // total bits which have read from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                result_list.add((int) (buffer >>> (totalBits - width)));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int block_num = (numbers.size() - start) / 8;
        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, start + i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        return encode_pos;

    }

    public static ArrayList<Integer> decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int block_num = (block_size - 1) / 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
            unpack8Values(encoded, decode_pos, bit_width, result_list);
            decode_pos += bit_width;
        }
        return result_list;
    }

    private static int zigzagEncode(int n) {
        return (n << 1) ^ (n >> 31);
    }
    public static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        int scale = (int) Math.pow(10, decimalMax);
        int size = numbers.size();
        int[] result = new int[size];

        if (size == 0) {
            return result;
        }

        // 1. Parse all numbers and scale them up
        int[] scaledValues = new int[size];
        for (int i = 0; i < size; i++) {
            String numStr = numbers.get(i);
            // Parse the number (handling both "123.456" and "123" cases)
            String[] parts = numStr.split("\\.");
            int whole = Integer.parseInt(parts[0]);

            // Handle fractional part
            int fraction = 0;
            if (parts.length > 1) {
                String fractionStr = parts[1];
                // Pad with zeros if necessary to ensure proper scaling
                if (fractionStr.length() < decimalMax) {
                    while (fractionStr.length() < decimalMax) {
                        fractionStr += "0";
                    }
                } else if (fractionStr.length() > decimalMax) {
                    // Truncate if too many decimal places (alternative could be rounding)
                    fractionStr = fractionStr.substring(0, decimalMax);
                }
                fraction = Integer.parseInt(fractionStr);
            }

            scaledValues[i] = whole * scale + fraction;
        }

//        // 2. Process first element
//        int first = scaledValues[0];
//        result[0] = first;
//
//        // 3. Process subsequent elements with delta + ZigZag encoding
//        int prev = first;
//        for (int i = 1; i < size; i++) {
//            int current = scaledValues[i];
//            int diff = current - prev;
//            result[i] = (diff << 1) ^ (diff >> 31); // ZigZag encoding
//            prev = current;
//        }

        return scaledValues;
    }

    public static int[] sprintz(int[] numbers) {
        int size = numbers.length;
        int[] result = new int[size];

        int first = numbers[0];
        result[0] = first;

        // 3. Process subsequent elements with delta + ZigZag encoding
        int prev = first;
        for (int i = 1; i < size; i++) {
            int current = numbers[i];
            int diff = current - prev;
            result[i] = (diff << 1) ^ (diff >> 31); // ZigZag encoding
            prev = current;
        }

        return result;
    }

    public static byte[] encodeBitPacking(int[] paddedArray, int[] bitWidths, int pack_size) {
        List<Byte> result = new ArrayList<>();



        // 3. 对paddedArray进行bit-packing
        int totalGroups = bitWidths.length;

        // 计算bit-packed数据的总字节数 - 修正计算方式
//        int totalBitPackedBytes = (cost_bits+7)/8;
//        for (int i = 0; i < totalGroups; i++) {
//            // 每组需要 ceil(8 * bitWidth / 8) = bitWidth 字节
//            totalBitPackedBytes += bitWidths[i];
//        }

        // 确保数组足够大，添加一些额外空间以防万一
        int max_bit_width = 0;

        for (int bitWidth : bitWidths) {
            if (bitWidth > max_bit_width) {
                max_bit_width = bitWidth;
            }
        }
//        System.out.println(max_bit_width);
//        if(max_bit_width ==0) {
//            System.out.println(Arrays.toString(bitWidths));
//        }
//        System.out.println(pack_size);
//        System.out.println(totalGroups);
        int totalBitPackedBytes = (max_bit_width*pack_size*totalGroups+7)/8;
        byte[] bitPackedData = new byte[totalBitPackedBytes + totalGroups+32];
//        bitPackedData[0] = (byte) max_bit_width;
        int encodePos = 0;
        // 对每组数据进行bit-packing
        for (int group = 0; group < totalGroups; group++) {
            int startIndex = group * pack_size;
            ArrayList<Integer> groupData = new ArrayList<>();
            for (int i = 0; i < pack_size; i++) {
                if (startIndex + i < paddedArray.length) {
                    groupData.add(paddedArray[startIndex + i]);
                } else {
                    groupData.add(0); // 用0填充不足的部分
                }
            }

            bitPackedData[encodePos++] = (byte) bitWidths[group];
            encodePos = bitPacking(groupData, 0,bitWidths[group] , encodePos, bitPackedData);
        }


        // 4. 将bit-packed数据写入结果（只写入实际使用的部分）
        for (int i = 0; i < encodePos; i++) {
            result.add(bitPackedData[i]);
        }

        // 转换为byte数组返回
        byte[] finalResult = new byte[result.size()];
        for (int i = 0; i < result.size(); i++) {
            finalResult[i] = result.get(i);
        }

        return finalResult;
    }
    public static int computeMinPackingCost(int[] bitWidths, int fixed_pack, int pack_size) {
        int blocksize= bitWidths.length;
//        int minCost = Integer.MAX_VALUE;

        // Try all possible pack sizes from 1 to CHUNK_SIZE
//        for (int p = 1; p <= blocksize; p++) {
        int totalCost = 0;
        int numPacks = (int) Math.ceil((double) blocksize / fixed_pack);

        // Calculate cost for each pack
        for (int pack = 0; pack < numPacks; pack++) {
            int start = pack * fixed_pack;
            int end = Math.min(start + fixed_pack, blocksize);


            // Find max bitWidth in current pack
            int maxBitWidth = 0;
            for (int i = start; i < end; i++) {
                if (bitWidths[i] > maxBitWidth) {
                    maxBitWidth = bitWidths[i];
                }
            }

            // Add to cost: 8 * p * maxBitWidth
            totalCost += pack_size * (end-start) * maxBitWidth;
        }

        // Add the chunk cost: 5 * CHUNK_SIZE / p
        totalCost += 5 * blocksize / fixed_pack;

//            // Update minimum cost
//            if (totalCost < minCost) {
//                minCost = totalCost;
//            }
//        }
        return totalCost;
    }

    /**
     * ZigZag解码
     */
    private static int zigzagDecode(int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * 解压函数 - 从压缩数据中恢复原始整数数组
     */
//    public static int[] decodeBitPackingFull(byte[] compressedData, int originalLength, int packSize) {
//        List<Integer> decodedValues = new ArrayList<>();
//        int decodePos = 0;
//
//        // 计算分组数量
//        int totalGroups = (originalLength + packSize - 1) / packSize;
//
//        for (int group = 0; group < totalGroups; group++) {
//            // 读取该组的位宽
//            int bitWidth = compressedData[decodePos++] & 0xFF;
//
//            if (bitWidth == 0) {
//                // 如果位宽为0，说明这组都是0
//                for (int i = 0; i < packSize; i++) {
//                    decodedValues.add(0);
//                }
//                continue;
//            }
//
//            // 解压该组数据
//            ArrayList<Integer> groupData = new ArrayList<>();
//            unpack8Values(compressedData, decodePos, bitWidth, groupData);
//            decodePos += bitWidth;
//
//            // 添加到结果列表
//            for (int value : groupData) {
//                decodedValues.add(value);
//            }
//        }
//
//        // 转换为数组并截取原始长度（因为可能有填充）
//        int[] result = new int[originalLength];
//        for (int i = 0; i < originalLength; i++) {
//            result[i] = decodedValues.get(i);
//        }
//
//        return result;
//    }
    public static int[] decodeBitPackingFull(byte[] compressedData, int originalLength, int packSize) {
        // 预分配结果数组，避免ArrayList的动态扩容开销
        int[] result = new int[originalLength];
        int resultIndex = 0;
        int decodePos = 0;

        // 计算分组数量
        int totalGroups = (originalLength + packSize - 1) / packSize;

        // 预分配组数据数组，避免在循环中重复创建
        int[] groupData = new int[packSize];

        for (int group = 0; group < totalGroups && resultIndex < originalLength; group++) {
            // 读取该组的位宽
            int bitWidth = compressedData[decodePos++] & 0xFF;

            if (bitWidth == 0) {
                // 如果位宽为0，说明这组都是0 - 直接填充0
                int fillCount = Math.min(packSize, originalLength - resultIndex);
                // Arrays.fill比循环更快
                if (fillCount > 0) {
                    Arrays.fill(result, resultIndex, resultIndex + fillCount, 0);
                    resultIndex += fillCount;
                }
                continue;
            }

            // 直接解压到预分配的groupData数组，避免ArrayList的开销
            int actualUnpacked = unpack8ValuesToArray(compressedData, decodePos, bitWidth, groupData);
            decodePos += bitWidth;

            // 直接复制到结果数组，避免额外的循环
            int copyCount = Math.min(actualUnpacked, originalLength - resultIndex);
            System.arraycopy(groupData, 0, result, resultIndex, copyCount);
            resultIndex += copyCount;
        }

        return result;
    }
    private static int unpack8ValuesToArray(byte[] encoded, int offset, int width, int[] result) {
        int byteIdx = offset;
        long buffer = 0;
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8 && valueIdx < result.length) {
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            while (totalBits >= width && valueIdx < 8 && valueIdx < result.length) {
                result[valueIdx] = (int) (buffer >>> (totalBits - width));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }

        return valueIdx; // 返回实际解压的数量
    }
    /**
     * Sprintz解码 - 从差分编码恢复原始数据
     */
    public static int[] sprintzDecode(int[] encodedData) {
        int size = encodedData.length;
        int[] result = new int[size];

        if (size == 0) return result;

        // 第一个元素是原始值
        result[0] = encodedData[0];

        // 后续元素需要ZigZag解码和累加
        int prev = result[0];
        for (int i = 1; i < size; i++) {
            int zigzagEncoded = encodedData[i];
            int diff = (zigzagEncoded >>> 1) ^ -(zigzagEncoded & 1); // ZigZag解码
            result[i] = prev + diff;
            prev = result[i];
        }

        return result;
    }
    public static double[] unscaleNumbers(int[] scaledValues, int decimalMax) {
        double scale = Math.pow(10, decimalMax);
        int size = scaledValues.length;
        double[] result = new double[size];

        for (int i = 0; i < size; i++) {
            result[i] = scaledValues[i] / scale;
        }

        return result;
    }
    public static int[] decompress(byte[] compressedData, int originalLength, int decimalMax, int packSize) {
        // 1. 位解压
        int[] bitUnpacked = decodeBitPackingFull(compressedData, originalLength, packSize);

        // 2. Sprintz解码
        int[] sprintzDecoded = sprintzDecode(bitUnpacked);

        // 3. 缩放逆变换
//        double[] unscaled = unscaleNumbers(sprintzDecoded, decimalMax);

        return sprintzDecoded;
    }
    public static int[] decodeBitPacking(byte[] compressedData, int[] bitWidths, int pack_size, int originalLength) {
        int[] result = new int[originalLength];  // 直接使用数组
        int resultIndex = 0;
        int decodePos = 0;

        for (int group = 0; group < bitWidths.length && resultIndex < originalLength; group++) {
            int bitWidth = compressedData[decodePos++] & 0xFF;

            // 预分配固定大小的列表
            ArrayList<Integer> groupData = new ArrayList<>(pack_size);
            unpack8Values(compressedData, decodePos, bitWidth, groupData);

            // 批量拷贝
            int copyLength = Math.min(pack_size, originalLength - resultIndex);
            for (int i = 0; i < copyLength; i++) {
                result[resultIndex++] = groupData.get(i);
            }

            decodePos += bitWidth;
        }

        return result;
    }

    @Test
    public void printDataTest() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
//        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/bitwidth";
        File outputDir = new File(outputDirstr);

//        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
//            if(!file.getName().equals("Stocks-DE.csv")) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "BP",
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0, sigBits;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                            sigBits = (int) ((parts[0].length() + decimal) * (Math.log(10) / Math.log(2)));
                        } else {
                            sigBits = (int) (numStr.length() * (Math.log(10) / Math.log(2)));
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            List<String> bitWidthRecords = new ArrayList<>();
            List<String> sprintzBitWidthRecords = new ArrayList<>();
            for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                    continue;

                int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                        .stream().max(Integer::compare).orElse(0);

                int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
                long startTime = System.nanoTime();

                int remainder = scaledInts.length % 8;
                int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                // 创建新数组，长度补齐为8的倍数
                int[] paddedArray = new int[scaledInts.length + paddingLength];
                System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                int actual_length = paddedArray.length;
                int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                    int maxInGroup = 0;
                    for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                        if (paddedArray[scaledInts_j] > maxInGroup) {
                            maxInGroup = paddedArray[scaledInts_j];
                        }
                    }

                    int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                    bitWidths[scaledInts_i / 8] = bitWidth;
                    String[] record = {
                            String.valueOf(bitWidth),
                    };
                    writer.writeRecord(record);
                }

            }


            writer.close();
//            break;
        }
    }

    @Test
    public void printSprintzDataTest() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
//        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/bitwidth_sprintz";
        File outputDir = new File(outputDirstr);

//        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
//            if(!file.getName().equals("Stocks-DE.csv")) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Sprintz",
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0, sigBits;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                            sigBits = (int) ((parts[0].length() + decimal) * (Math.log(10) / Math.log(2)));
                        } else {
                            sigBits = (int) (numStr.length() * (Math.log(10) / Math.log(2)));
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            List<String> bitWidthRecords = new ArrayList<>();
            List<String> sprintzBitWidthRecords = new ArrayList<>();
            for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                    continue;

                int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                        .stream().max(Integer::compare).orElse(0);

                int[] scalingInt = scaleNumbers(chunkNumbers, decimalMax);
                long startTime = System.nanoTime();
                int[] scaledInts = sprintz(scalingInt);

                int remainder = scaledInts.length % 8;
                int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                // 创建新数组，长度补齐为8的倍数
                int[] paddedArray = new int[scaledInts.length + paddingLength];
                System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                int actual_length = paddedArray.length;
                int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                    int maxInGroup = 0;
                    for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                        if (paddedArray[scaledInts_j] > maxInGroup) {
                            maxInGroup = paddedArray[scaledInts_j];
                        }
                    }

                    int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                    bitWidths[scaledInts_i / 8] = bitWidth;
                    String[] record = {
                            String.valueOf(bitWidth),
                    };
                    writer.writeRecord(record);
                }

            }


            writer.close();
//            break;
        }
    }

    public static void main(String[] args) throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
//        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz";
        File outputDir = new File(outputDirstr);

//        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
//            if(!file.getName().equals("Stocks-DE.csv")) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0, sigBits;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                            sigBits = (int) ((parts[0].length() + decimal) * (Math.log(10) / Math.log(2)));
                        } else {
                            sigBits = (int) (numStr.length() * (Math.log(10) / Math.log(2)));
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = 50;
//            System.out.println(numbers.size());


            // 方法：强化学习
//            long modelStart =  System.nanoTime();
            int modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0;  // 新增解码时间统计
            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    int[] scalingInt = scaleNumbers(chunkNumbers, decimalMax);
                    long startTime = System.nanoTime();
                    int[] scaledInts = sprintz(scalingInt);

                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                    // 创建新数组，长度补齐为8的倍数
                    int[] paddedArray = new int[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        int maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }

                        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);

                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }
//                    System.out.println(Arrays.toString(bitWidths));
                    int fixed_pack = CHUNK_SIZE / 40;
//                    int cur_cost = computeMinPackingCost(bitWidths,fixed_pack, 8);
                    byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, 8);
                    int cur_cost = compressedData.length * 8; // 转换为bit数
//                    System.out.println(cur_cost);
//                    PackingResult result = packOctads(bitWidths, model, null); // 禁用决策跟踪
                    long duration = System.nanoTime() - startTime;

                    long decodeStartTime = System.nanoTime();
                        // 执行解压
                    int[] decodedData = decodeBitPacking(compressedData, bitWidths, 8, scaledInts.length);
                    sprintzDecode(decodedData);
//                    int[] decompressed = decompress(compressedData, chunkNumbers.size(), decimalMax, 8);

                    long decodeDuration = System.nanoTime() - decodeStartTime;
                    modelDecodeTime += decodeDuration;

                    modelTime += (duration);
                    modelCost +=  cur_cost;
//                    if(i==0)
//                        for (int episode = 0; episode < 10; episode++) {
//                            trainEpisode(scaledInts, episode);
//                        }
//                    List<Integer> optimalK = predictOptimalK(scaledInts);
//                    System.out.println("Optimal k sequence: " + optimalK);
                }

            }
            modelCost /=time_of_repeat;
            modelTime = (modelTime)/time_of_repeat;
            modelDecodeTime /= time_of_repeat;
            double model_ratio = (double) modelCost / (double) (numbers.size()*64);
            double modelTime_throughput = (double)(numbers.size()*8000)/ (double) (modelTime);
            double modelDecodeTime_throughput = (double)(numbers.size()*8000)/ (double) (modelDecodeTime);
            String[] record = {
                    file.toString(),
                    "Sprintz",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelDecodeTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();
//            break;
        }
    }
    @Test
    public void TestVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_SPRINTZ_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0, sigBits;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                            sigBits = (int) ((parts[0].length() + decimal) * (Math.log(10) / Math.log(2)));
                        } else {
                            sigBits = (int) (numStr.length() * (Math.log(10) / Math.log(2)));
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = 50;

            for(int pack_size_exp = 3; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                // 方法：强化学习
                int modelCost = 0;
                long modelTime = 0;
                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        int[] scaledInts = sprintz(scaledInt);
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为8的倍数
                        int[] paddedArray = new int[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每8个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                            // 1. 找出当前8个元素中的最大值
                            int maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + pack_size; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / pack_size] = bitWidth;
                        }

                        byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                        int cur_cost = compressedData.length * 8; // 转换为bit数
                        long duration = System.nanoTime() - startTime;
                        modelTime += (duration);
                        modelCost += cur_cost;
                    }

                }
                modelCost /= time_of_repeat;
                modelTime = (modelTime) / time_of_repeat;
                double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);
                String[] record = {
                        file.toString(),
                        "SPRINTZ",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }
    @Test
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8, 1024*8};

        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }

            int time_of_repeat = 50; // 减少重复次数以加快测试速度
//            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);
//            int[] scaledInts_all = scaleNumbers(numbers, decimalMax);

            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

// 分批处理，每1024个元素一批
            int batchSize = 1024;
            List<int[]> batches = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += batchSize) {
                int end = Math.min(numbers.size(), i + batchSize);
                List<String> batch = numbers.subList(i, end);
                int[] scaledBatch = scaleNumbers(batch, decimalMax);
                batches.add(scaledBatch);
            }

            // 计算总长度并拼接所有批次的结果
            int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
            int[] scaledInts_all = new int[totalLength];

            int currentIndex = 0;
            for (int[] batch : batches) {
                System.arraycopy(batch, 0, scaledInts_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);
//                System.out.println(numbers.subList(0,1000));

                for(int pack_size_exp = 3; pack_size_exp < 4; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    int modelCost = 0;
                    long modelTime = 0;


                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {

//                            List<String> chunkNumbers = numbers.subList(i, Math.min(i + chunkSize, numbers.size()));

//                            if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
//                                continue;
//                            int decimalMax = decimalPlaces.subList(i, Math.min(i + chunkSize, numbers.size()))
//                                    .stream().max(Integer::compare).orElse(0);

                            int end = Math.min(i + chunkSize, numbers.size());
                            int[] scaledInt = new int[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);



                            long startTime = System.nanoTime();
                            int[] scaledInts = sprintz(scaledInt);
                            int remainder = scaledInts.length % pack_size;
                            int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                            // 创建新数组，长度补齐为pack_size的倍数
                            int[] paddedArray = new int[scaledInts.length + paddingLength];
                            System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                            int actual_length = paddedArray.length;
                            int[] bitWidths = new int[actual_length / pack_size];
//                            if(i==0){
//                                System.out.println(Arrays.toString(paddedArray));
//                            }


                            for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                int maxInGroup = 0;
                                for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + pack_size; scaledInts_j++) {
                                    if (paddedArray[scaledInts_j] > maxInGroup) {
                                        maxInGroup = paddedArray[scaledInts_j];
                                    }
                                }

                                int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
                                bitWidths[scaledInts_i / pack_size] = bitWidth;
//                                System.out.println(bitWidth);
                            }

                            byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                            int cur_cost = compressedData.length * 8;
                            long duration = System.nanoTime() - startTime;

                            modelTime += (duration);
                            modelCost += cur_cost;
                        }
                    }

                    modelCost /= time_of_repeat;
                    modelTime = (modelTime) / time_of_repeat;
                    double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                    double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);

                    String[] record = {
                            String.valueOf(chunkSize/8),
                            file.toString(),
                            "BP",
                            String.valueOf(modelTime_throughput),
                            String.valueOf(numbers.size()),
                            String.valueOf(modelCost),
                            String.valueOf(pack_size),
                            String.valueOf(model_ratio)
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
//            break;
        }
    }

}