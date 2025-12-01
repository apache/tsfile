package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RLEPackBitWidthTest {
    static final List<String> IGNORE_FILES = Arrays.asList(".DS_Store", "full_data", "test.csv","POI-lat.csv",
            "POI-lon.csv","Basel-wind.csv","Basel-temp.csv","Air-sensor.csv");
    private static final int CHUNK_SIZE = 1024;

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

    // 新增的解压函数
    public static int[] decodeBitPackingWithRLE(byte[] compressedData, int originalLength, int pack_size) {
        List<Integer> result = new ArrayList<>();
        int pos = 0;

        // 1. 解析RLE编码的bitWidths
        // 读取run_count（4字节）
        int runCount = bytes2Integer(compressedData, pos, 4);
        pos += 4;

        // 解析RLE游程
        int[] bitWidths = decodeRLE(compressedData, pos, runCount);
        pos += runCount * 2; // 每个游程占2字节

        // 2. 解压bit-packed数据
        int totalGroups = bitWidths.length;

        for (int group = 0; group < totalGroups; group++) {
            int bitWidth = bitWidths[group];

            // 解压当前分组
            ArrayList<Integer> groupData = new ArrayList<>();
            unpack8Values(compressedData, pos, bitWidth, groupData);

            // 添加解压出的数据
            for (int i = 0; i < pack_size; i++) {
                if (result.size() < originalLength) {
                    result.add(groupData.get(i));
                }
            }

            pos += bitWidth;
        }

        // 转换为数组返回
        int[] decodedArray = new int[result.size()];
        for (int i = 0; i < result.size(); i++) {
            decodedArray[i] = result.get(i);
        }
        return decodedArray;
    }

    // 新增的RLE解码函数
    public static int[] decodeRLE(byte[] data, int startPos, int runCount) {
        List<Integer> bitWidths = new ArrayList<>();

        for (int i = 0; i < runCount; i++) {
            int runLength = data[startPos + i * 2] & 0xFF;
            int value = data[startPos + i * 2 + 1] & 0xFF;

            // 重复添加runLength次value
            for (int j = 0; j < runLength; j++) {
                bitWidths.add(value);
            }
        }

        // 转换为数组
        int[] result = new int[bitWidths.size()];
        for (int i = 0; i < bitWidths.size(); i++) {
            result[i] = bitWidths.get(i);
        }
        return result;
    }

    private static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        // 1. 预先计算缩放因子
        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
        int size = numbers.size();
        int[] result = new int[size];

        if (size == 0) {
            return result;
        }

        // 2. 单次遍历完成所有转换和最小值查找
        BigDecimal min = null;
        BigDecimal[] scaledValues = new BigDecimal[size];

        for (int i = 0; i < size; i++) {
            BigDecimal val = new BigDecimal(numbers.get(i)).multiply(scale);
            scaledValues[i] = val;
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
        }

        // 3. 处理第一个元素
        BigDecimal first = scaledValues[0].subtract(min);
        result[0] = first.toBigInteger().intValue();

        // 4. 处理后续元素（差分+ZigZag）
        for (int i = 1; i < size; i++) {
            BigDecimal current = scaledValues[i].subtract(min);
            result[i]=current.toBigInteger().intValue();
        }

        return result;
    }

    /**
     * 实际的压缩编码函数：将paddedArray按照bitWidths进行bit-packing，并对bitWidths进行RLE编码
     */
    public static byte[] encodeBitPackingWithRLE(int[] paddedArray, int[] bitWidths, int pack_size, int cost_bits) {
        List<Byte> result = new ArrayList<>();

        // 1. 对bitWidths进行RLE编码
        List<Byte> rleEncoded = encodeRLE(bitWidths);

        // 2. 将RLE编码的bitWidths写入结果
        // 写入RLE数据
        result.addAll(rleEncoded);

        // 3. 对paddedArray进行bit-packing
        int totalGroups = bitWidths.length;

        // 计算bit-packed数据的总字节数 - 修正计算方式
        int totalBitPackedBytes = (cost_bits+7)/8;

        // 确保数组足够大，添加一些额外空间以防万一
        byte[] bitPackedData = new byte[totalBitPackedBytes + 32];
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

            encodePos = bitPacking(groupData, 0, bitWidths[group], encodePos, bitPackedData);
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

    /**
     * RLE编码bitWidths数组
     * chunksize = 1024
     * packsize = 8
     * runlength = 128
     * runcount =
     */
    public static List<Byte> encodeRLE(int[] bitWidths) {
        List<Byte> result = new ArrayList<>();

        if (bitWidths.length == 0) {
            return result;
        }
        int length_bitWidths_list = bitWidths.length;
        int run_count = 0;

        int[] run_lengths = new int[length_bitWidths_list];
        int[] run_values = new int[length_bitWidths_list];
        int pre_bit_width = bitWidths[0];
        int pre_run_length = 1;

        for (int i = 1; i < length_bitWidths_list; i++) {
            if (bitWidths[i] == pre_bit_width) {
                pre_run_length++;
            } else {
                run_lengths[run_count] = pre_run_length;
                run_values[run_count++] = pre_bit_width;
                pre_bit_width = bitWidths[i];
                pre_run_length = 1;
            }
        }
        run_lengths[run_count] = pre_run_length;
        run_values[run_count++] = pre_bit_width;

        result.add((byte) (run_count >> 24));
        result.add((byte) (run_count >> 16));
        result.add((byte) (run_count >> 8));
        result.add((byte) run_count);
        for (int i = 0; i < run_count; i++) {
            encodeRLERun(result, run_lengths[i], run_values[i]);
        }

        return result;
    }

    /**
     * 编码单个RLE游程
     */
    private static void encodeRLERun(List<Byte> result, int runLength, int value) {
        result.add((byte) (runLength >> 24));
        result.add((byte) (runLength >> 16));
        result.add((byte) (runLength >> 8));
        result.add((byte) runLength);
        result.add((byte) (runLength >> 24));
        result.add((byte) (value >> 16));
        result.add((byte) (value >> 8));
        result.add((byte) value);
    }

    public static int computeMinPackingCost(int[] bitWidths, int fixed_pack, int pack_size) {
        int blocksize= bitWidths.length;

        int totalCost = 0;
        int numBlocks = (int) Math.ceil((double) blocksize / fixed_pack);
        // RLE compress bit width series: rle_count 8 bits, (run length 8 bits, bit width value: 6 bits) * run_count

        // Calculate cost for each pack
        for (int pack = 0; pack < numBlocks; pack++) {
            int start = pack * fixed_pack;
            int end = Math.min(start + fixed_pack, blocksize);
            int cur_block_size = end - start;

            // Find max bitWidth in current pack
            int maxBitWidth = 0;
            int[] run_lengths = new int[cur_block_size];
            int[] run_values = new int[cur_block_size];
            int run_count = 0;
            int pre_bit_width = bitWidths[start];
            int pre_run_length = 1;
            totalCost += pack_size * bitWidths[start];

            for (int i = start+1; i < end; i++) {
                totalCost += pack_size * bitWidths[i];
                if(pre_bit_width == bitWidths[i]) {
                    pre_run_length++;
                } else {
                    run_lengths[run_count] = pre_run_length;
                    run_values[run_count++] = pre_bit_width;
                    pre_bit_width = bitWidths[i];
                    pre_run_length = 1;
                }
            }
            run_lengths[run_count] = pre_run_length;
            run_values[run_count++] = pre_bit_width;

            totalCost += 64;
            for (int i = 0; i < run_count; i++) {
                totalCost += 32;
            }
        }
        System.out.println(blocksize);

        return totalCost;
    }

    public static void main(String[] args) throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头，增加解压吞吐率列
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);
            System.out.println("Processing " + file.getName() + "...");
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
            int time_of_repeat = 50;

            long modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0; // 新增：解压时间统计

            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
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

                    int[] paddedArray = new int[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8];

                    int cost_bits = 0;
                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        int maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }
                        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
                        bitWidths[scaledInts_i / 8] = bitWidth;
                        cost_bits += (bitWidth*8);
                    }
                    byte[] compressedData = encodeBitPackingWithRLE(paddedArray, bitWidths, 8,cost_bits);
                    long cur_cost = compressedData.length * 8; // 转换为bit数

                    long duration = System.nanoTime() - startTime;
                    modelTime += duration;
                    modelCost += cur_cost;

                    // 新增：测试解压性能
                    long startDecodeTime = System.nanoTime();
                    int[] decodedData = decodeBitPackingWithRLE(compressedData, scaledInts.length, 8);
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    modelDecodeTime += decodeDuration;

//                    // 可选：验证解压数据的正确性（只在第一次重复时验证）
//                    if (j == 0) {
//                        boolean correct = true;
//                        for (int k = 0; k < scaledInts.length; k++) {
//                            if (scaledInts[k] != decodedData[k]) {
//                                correct = false;
//                                System.err.println("Decompression error at index " + k +
//                                        ": expected " + scaledInts[k] + ", got " + decodedData[k]);
//                                break;
//                            }
//                        }
////                        if (correct) {
////                            System.out.println("Decompression verified successfully for chunk " + (i/CHUNK_SIZE));
////                        }
//                    }
                }
            }
            modelCost /= time_of_repeat;
            modelTime = modelTime / time_of_repeat;
            modelDecodeTime = modelDecodeTime / time_of_repeat; // 平均解压时间

            double model_ratio = (double) modelCost / (double) (numbers.size()*64);
            double modelTime_throughput = (double)(numbers.size()*8000L) / (double) (modelTime); // points per second
            double modelDecodeTime_throughput = (double)(numbers.size()*8000L) / (double) (modelDecodeTime); // points per second

            // 更新输出记录，包含解压吞吐率
            String[] record = {
                    file.toString(),
                    "BP+RLE",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelDecodeTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();

//            System.out.println("Encoding throughput: " + modelTime_throughput + " points/s");
//            System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " points/s");
//            System.out.println("Compression ratio: " + model_ratio);
        }
    }

    @Test
    public void TestVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE_vary_pack_size";
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

            for(int pack_size_exp = 3; pack_size_exp < 10; pack_size_exp++){
                int pack_size = (int) Math.pow(2,pack_size_exp);
                System.out.println(pack_size);
                int modelCost = 0;
                long modelTime = 0;
                for(int j=0;j<time_of_repeat;j++){
                    int totalCost = 0;
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为8的倍数
                        int[] paddedArray = new int[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每8个值的位宽结果

                        int cost_bits = 0;
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
                            cost_bits += (bitWidth*pack_size);
                        }

                        int fixed_block = CHUNK_SIZE / pack_size;
                        byte[] compressedData = encodeBitPackingWithRLE(paddedArray, bitWidths, pack_size, cost_bits);
                        int cur_cost = compressedData.length * 8; // 转换为bit数

                        long duration = System.nanoTime() - startTime;
                        modelTime += (duration);
                        modelCost +=  cur_cost;
                    }

                }
                modelCost /=time_of_repeat;
                modelTime = (modelTime)/time_of_repeat;
                double model_ratio = (double) modelCost / (double) (numbers.size()*64);
                double modelTime_throughput = (double)(numbers.size()*8000)/ (double) (modelTime);
                String[] record = {
                        file.toString(),
                        "BP+RLE",
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

    // 新增方法：测试不同chunk size的表现
    @Test
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE_vary_m";
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
//
//                            int decimalMax = decimalPlaces.subList(i, Math.min(i + chunkSize, numbers.size()))
//                                    .stream().max(Integer::compare).orElse(0);
//
//                            int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
                            int end = Math.min(i + chunkSize, numbers.size());
                            int[] scaledInts = new int[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInts, 0, end - i);

                            long startTime = System.nanoTime();
                            int remainder = scaledInts.length % pack_size;
                            int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                            int[] paddedArray = new int[scaledInts.length + paddingLength];
                            System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                            int actual_length = paddedArray.length;
                            int[] bitWidths = new int[actual_length / pack_size];

                            int cost_bits = 0;
                            for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                int maxInGroup = 0;
                                for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + pack_size; scaledInts_j++) {
                                    if (paddedArray[scaledInts_j] > maxInGroup) {
                                        maxInGroup = paddedArray[scaledInts_j];
                                    }
                                }
                                int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
                                bitWidths[scaledInts_i / pack_size] = bitWidth;
                                cost_bits += (bitWidth * pack_size);
                            }

                            byte[] compressedData = encodeBitPackingWithRLE(paddedArray, bitWidths, pack_size, cost_bits);
                            int cur_cost = compressedData.length * 8;
                            long duration = System.nanoTime() - startTime;
                            modelTime += duration;
                            modelCost += cur_cost;
                        }
                    }

                    modelCost /= time_of_repeat;
                    modelTime = modelTime / time_of_repeat;
                    double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                    double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);

                    String[] record = {
                            String.valueOf(chunkSize/8),
                            file.toString(),
                            "BP+RLE",
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
        }
    }
}