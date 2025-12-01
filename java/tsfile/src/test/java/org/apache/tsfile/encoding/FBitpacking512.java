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


public class FBitpacking512 {

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
//        public static int[] decodeBitPacking(byte[] compressedData, int[] bitWidths, int pack_size, int originalLength) {
//            List<Integer> result = new ArrayList<>();
//            int decodePos = 0;
//
//            for (int group = 0; group < bitWidths.length; group++) {
//                int bitWidth = compressedData[decodePos++] & 0xFF;
//
//                // 解压当前分组
//                ArrayList<Integer> groupData = new ArrayList<>();
//                unpack8Values(compressedData, decodePos, bitWidth, groupData);
//
//                // 添加解压出的数据
//                for (int i = 0; i < pack_size; i++) {
//                    if (result.size() < originalLength) {
//                        result.add(groupData.get(i));
//                    }
//                }
//
//                decodePos += bitWidth;
//            }
//
//            // 转换为数组返回
//            int[] decodedArray = new int[result.size()];
//            for (int i = 0; i < result.size(); i++) {
//                decodedArray[i] = result.get(i);
//            }
//            return decodedArray;
//        }

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

        // 新增的完整解压函数（包含头部信息解析）
        public static int[] decodeBitPackingWithHeader(byte[] encodedWithHeader, int pack_size) {
            // 解析头部信息 - 假设前4个字节存储原始数据长度
            int originalLength = bytes2Integer(encodedWithHeader, 0, 4);

            // 解析分组数
            int groupCount = bytes2Integer(encodedWithHeader, 4, 4);

            // 解析位宽数组
            int[] bitWidths = new int[groupCount];
            int headerSize = 8; // 4字节原始长度 + 4字节分组数
            for (int i = 0; i < groupCount; i++) {
                bitWidths[i] = encodedWithHeader[headerSize + i] & 0xFF;
            }

            // 压缩数据起始位置
            int dataStart = headerSize + groupCount;
            byte[] compressedData = new byte[encodedWithHeader.length - dataStart];
            System.arraycopy(encodedWithHeader, dataStart, compressedData, 0, compressedData.length);

            // 调用解压函数
            return decodeBitPacking(compressedData, bitWidths, pack_size, originalLength);
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

        public static void main(String[] args) throws IOException {
            // 示例数据（实际应替换为真实时间序列）
            System.out.println("\nPerformance Testing...");
            String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
            String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP";
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
                int time_of_repeat = 500;


                long modelCost = 0;
                long modelTime = 0;
                long modelDecodeTime = 0; // 新增：解压时间

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

                        // 创建新数组，长度补齐为8的倍数
                        int[] paddedArray = new int[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                            // 1. 找出当前8个元素中的最大值
                            int maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / 8] = bitWidth;
                        }

//                        int fixed_block = CHUNK_SIZE/40;
                        byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, 8);
                        long cur_cost = compressedData.length * 8L; // 转换为bit数
                        long duration = System.nanoTime() - startTime;
                        modelTime += (duration);
                        modelCost +=  cur_cost;

                        // 新增：测试解压性能
                        long startDecodeTime = System.nanoTime();
                        int[] decodedData = decodeBitPacking(compressedData, bitWidths, 8, scaledInts.length);
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        modelDecodeTime += decodeDuration;

//                        // 可选：验证解压数据的正确性（只在第一次重复时验证）
//                        if (j == 0) {
//                            boolean correct = true;
//                            for (int k = 0; k < scaledInts.length; k++) {
//                                if (scaledInts[k] != decodedData[k]) {
//                                    correct = false;
//                                    System.err.println("Decompression error at index " + k +
//                                            ": expected " + scaledInts[k] + ", got " + decodedData[k]);
//                                    break;
//                                }
//                            }
////                            if (correct) {
////                                System.out.println("Decompression verified successfully for chunk " + (i/CHUNK_SIZE));
////                            }
//                        }
                    }

                }
                modelCost = modelCost/time_of_repeat;
                modelTime = (modelTime)/time_of_repeat;
                modelDecodeTime = (modelDecodeTime)/time_of_repeat; // 平均解压时间

                double model_ratio = (double) modelCost / (double) (numbers.size()*64);
                double modelTime_throughput = (double)(numbers.size()*8000L)/ (double) (modelTime);
                double modelDecodeTime_throughput = (double)(numbers.size()*8000L)/ (double) (modelDecodeTime);

                // 更新输出记录，包含解压吞吐率
                String[] record = {
                        file.toString(),
                        "BP",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(modelDecodeTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(model_ratio)
                };
                writer.writeRecord(record);
                writer.close();

                System.out.println("Encoding throughput: " + modelTime_throughput + " MB/s");
                System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " MB/s");
                System.out.println("Compression ratio: " + model_ratio);
            }

        }

        public static byte[] encodeBitPacking(int[] paddedArray, int[] bitWidths, int pack_size) {
            List<Byte> result = new ArrayList<>();

            int totalGroups = bitWidths.length;
            int max_bit_width = 0;

            for (int i = 0; i < totalGroups; i++) {
                if (bitWidths[i] > max_bit_width) {
                    max_bit_width = bitWidths[i];
                }
            }
            int totalBitPackedBytes = (max_bit_width*pack_size*totalGroups+7)/8;
            byte[] bitPackedData = new byte[totalBitPackedBytes +totalGroups+ 32];
            int encodePos = 0;

            for (int group = 0; group < totalGroups; group++) {
                int startIndex = group * pack_size;
                ArrayList<Integer> groupData = new ArrayList<>();
                for (int i = 0; i < pack_size; i++) {
                    if (startIndex + i < paddedArray.length) {
                        groupData.add(paddedArray[startIndex + i]);
                    } else {
                        groupData.add(0);
                    }
                }

                bitPackedData[encodePos++] = (byte) bitWidths[group];
                encodePos = bitPacking(groupData, 0, bitWidths[group], encodePos, bitPackedData);
            }

            for (int i = 0; i < encodePos; i++) {
                result.add(bitPackedData[i]);
            }

            byte[] finalResult = new byte[result.size()];
            for (int i = 0; i < result.size(); i++) {
                finalResult[i] = result.get(i);
            }

            return finalResult;
        }

        public static int computeMinPackingCost(int[] bitWidths, int fixed_pack, int pack_size) {
            int blocksize= bitWidths.length;
            int totalCost = 0;
            int numPacks = (int) Math.ceil((double) blocksize / fixed_pack);

            for (int pack = 0; pack < numPacks; pack++) {
                int start = pack * fixed_pack;
                int end = Math.min(start + fixed_pack, blocksize);

                int maxBitWidth = 0;
                for (int i = start; i < end; i++) {
                    if (bitWidths[i] > maxBitWidth) {
                        maxBitWidth = bitWidths[i];
                    }
                }

                totalCost += pack_size * (end-start) * maxBitWidth;
            }

            totalCost += 5 * blocksize / fixed_pack;
            return totalCost;
        }

    @Test
    public void TestVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP_vary_pack_size";
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

                        int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
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
                        "BP",
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
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP_vary_m";
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
                            int[] scaledInts = new int[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInts, 0, end - i);


                            long startTime = System.nanoTime();
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