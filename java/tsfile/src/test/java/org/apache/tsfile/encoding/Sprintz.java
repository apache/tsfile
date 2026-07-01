package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Sprintz {
    private static final int CHUNK_SIZE = 1024;

    // 1. 修复：逐位（bit-by-bit）的bitPacking方法 - 使用int[]替代ArrayList<Integer>
    public static int bitPacking(int[] numbers, int start, int count, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int currentBytePos = encode_pos;
        int currentBitPos = 0;

        for (int i = 0; i < count; i++) {
            int value = numbers[start + i];

            // 按位写入，从最高位开始
            for (int bit = bit_width - 1; bit >= 0; bit--) {
                int currentBit = (value >>> bit) & 1;  // 使用无符号右移

                // 将位写入当前字节的适当位置
                encoded_result[currentBytePos] |= (currentBit << (7 - currentBitPos));
                currentBitPos++;

                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }
        }

        // 如果最后没有对齐到字节边界，移动到下一个字节
        if (currentBitPos != 0) {
            currentBytePos++;
        }

        return currentBytePos;
    }

    // 2. 修复：逐位（bit-by-bit）的unpackValues方法 - 使用int[]替代ArrayList<Integer>
    public static int[] unpackValues(
            byte[] encoded, int decode_pos, int bit_width, int num_values) {
        int[] result = new int[num_values];

        // 如果位宽为0，直接返回0
        if (bit_width <= 0) {
            Arrays.fill(result, 0);
            return result;
        }

        int currentBytePos = decode_pos;
        int currentBitPos = 0;

        for (int i = 0; i < num_values; i++) {
            int value = 0;
            for (int bit = 0; bit < bit_width; bit++) {
                // 添加边界检查
                if (currentBytePos >= encoded.length) {
                    System.out.println("数组越界: currentBytePos=" + currentBytePos +
                            ", encoded.length=" + encoded.length);
                    // 返回已解码的部分
                    return Arrays.copyOf(result, i);
                }

                // 读取当前位
                int currentBit = (encoded[currentBytePos] >> (7 - currentBitPos)) & 1;
                value = (value << 1) | currentBit;
                currentBitPos++;

                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }
            result[i] = value;
        }

        return result;
    }

    // 3. 修复：encodeBitPacking方法 - 确保正确写入数据 - 使用int[]替代ArrayList<Integer>
    public static byte[] encodeBitPacking(int[] paddedArray, int[] bitWidths, int pack_size) {
        // 计算总字节数
        int totalBytes = 0;
        for (int bitWidth : bitWidths) {
            totalBytes += 1; // 位宽元数据
            int actualBitWidth = Math.max(1, bitWidth); // 确保位宽至少为1
            int bitsInGroup = actualBitWidth * pack_size;
            totalBytes += (bitsInGroup + 7) / 8; // 数据位
        }

        byte[] bitPackedData = new byte[totalBytes];
        Arrays.fill(bitPackedData, (byte) 0);

        int encodePos = 0;

        for (int group = 0; group < bitWidths.length; group++) {
            int startIndex = group * pack_size;

            // 写入位宽（至少为1）
            int actualBitWidth = Math.max(1, bitWidths[group]);
            bitPackedData[encodePos++] = (byte) actualBitWidth;

            // 使用逐位打包方式写入数据
            encodePos = bitPacking(paddedArray, startIndex, pack_size, actualBitWidth, encodePos, bitPackedData);
        }

        return bitPackedData;
    }

    // 4. 修复：decodeBitPacking方法 - 处理pack_size=1的情况 - 使用int[]替代ArrayList<Integer>
    public static int[] decodeBitPacking(byte[] compressedData, int originalLength, int pack_size) {
        if (pack_size <= 0) {
            throw new IllegalArgumentException("pack_size必须大于0");
        }

        int[] result = new int[originalLength];
        int resultIndex = 0;
        int decodePos = 0;

        // 计算分组数量
        int totalGroups = (originalLength + pack_size - 1) / pack_size;

        for (int group = 0; group < totalGroups && decodePos < compressedData.length; group++) {
            // 读取位宽
            int bitWidth = compressedData[decodePos] & 0xFF;
            decodePos++;

            // 计算当前分组需要读取的数据数量
            int valuesToRead = Math.min(pack_size, originalLength - resultIndex);

            if (valuesToRead <= 0) {
                break;
            }

            if (bitWidth == 0) {
                // 如果位宽为0，说明这组都是0
                for (int i = 0; i < valuesToRead; i++) {
                    if (resultIndex < originalLength) {
                        result[resultIndex++] = 0;
                    }
                }
                continue;
            }

            // 计算当前分组占用的字节数
            int bitsInGroup = bitWidth * pack_size;
            int bytesInGroup = (bitsInGroup + 7) / 8;

            // 解压当前分组
            int[] groupData = unpackValues(compressedData, decodePos, bitWidth, pack_size);

            // 只复制实际需要的数据
            for (int i = 0; i < valuesToRead && i < groupData.length; i++) {
                if (resultIndex < originalLength) {
                    result[resultIndex++] = groupData[i];
                }
            }

            // 更新解码位置
            decodePos += bytesInGroup;
        }

        return result;
    }

    // 5. 修复：验证压缩解压正确性
    public static boolean verifyCompression(int[] original, int[] decompressed, String description) {
        if (original.length != decompressed.length) {
            System.out.println(description + " - 长度不一致: " + original.length + " vs " + decompressed.length);
            return false;
        }

        int mismatchCount = 0;
        for (int i = 0; i < original.length && mismatchCount < 10; i++) {
            if (original[i] != decompressed[i]) {
                System.out.println(description + " - 位置 " + i + " 的值不一致: " +
                        original[i] + " vs " + decompressed[i] +
                        " (二进制: " + Integer.toBinaryString(original[i]) +
                        " vs " + Integer.toBinaryString(decompressed[i]) + ")");
                mismatchCount++;
                if (mismatchCount >= 10) {
                    System.out.println(description + " - 超过10个错误，停止报告");
                    break;
                }
            }
        }

        return mismatchCount == 0;
    }

    // 6. 修复：计算位宽 - 处理负值
    public static int calculateBitWidth(int maxValue) {
        if (maxValue == 0) {
            return 1; // 即使是0，也需要1位来表示
        }

        // 处理负值：取绝对值
        int absValue = Math.abs(maxValue);
        return 32 - Integer.numberOfLeadingZeros(absValue);
    }

    // 7. 修复：scaleNumbers
    public static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        if (numbers.isEmpty()) {
            return new int[0];
        }

        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
        int size = numbers.size();
        int[] result = new int[size];

        for (int i = 0; i < size; i++) {
            try {
                String numStr = numbers.get(i).trim();
                if (numStr.isEmpty()) {
                    result[i] = 0;
                    continue;
                }

                BigDecimal value = new BigDecimal(numStr);
                BigDecimal scaledValue = value.multiply(scale);
                result[i] = scaledValue.setScale(0, RoundingMode.HALF_UP).intValue();
            } catch (NumberFormatException e) {
                System.out.println("无法解析数字: '" + numbers.get(i) + "'");
                result[i] = 0;
            } catch (ArithmeticException e) {
                System.out.println("数值溢出: " + numbers.get(i));
                result[i] = 0;
            }
        }

        return result;
    }

    // 8. 修复：sprintz编码解码
    public static int[] sprintz(int[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new int[0];
        }

        int[] result = new int[numbers.length];
        result[0] = numbers[0];

        for (int i = 1; i < numbers.length; i++) {
            int diff = numbers[i] - numbers[i-1];
            // ZigZag编码
            result[i] = (diff << 1) ^ (diff >> 31);
        }

        return result;
    }

    public static int[] sprintzDecode(int[] encodedData) {
        if (encodedData == null || encodedData.length == 0) {
            return new int[0];
        }

        int[] result = new int[encodedData.length];
        result[0] = encodedData[0];

        for (int i = 1; i < encodedData.length; i++) {
            // ZigZag解码
            int zigzag = encodedData[i];
            int diff = (zigzag >>> 1) ^ -(zigzag & 1);
            result[i] = result[i-1] + diff;
        }

        return result;
    }

    // 9. 新的调试测试
    @Test
    public void testDebugFixed() {
        System.out.println("\n修复后的调试测试...");

        // 创建测试数据
        int[] testData = {100, 105, 110, 115, 120, 125, 130, 135};
        System.out.println("原始数据: " + Arrays.toString(testData));

        // Sprintz编码
        int[] encoded = sprintz(testData);
        System.out.println("Sprintz编码后: " + Arrays.toString(encoded));

        // Sprintz解码
        int[] decoded = sprintzDecode(encoded);
        System.out.println("Sprintz解码后: " + Arrays.toString(decoded));

        // 验证
        boolean match = Arrays.equals(testData, decoded);
        System.out.println("Sprintz编解码验证: " + (match ? "通过" : "失败"));

        // 测试打包大小=1
        System.out.println("\n测试打包大小=1");
        int pack_size = 1;
        int[] paddedArray = Arrays.copyOf(encoded, encoded.length);
        int[] bitWidths = new int[paddedArray.length / pack_size];

        System.out.println("paddedArray: " + Arrays.toString(paddedArray));

        for (int i = 0; i < paddedArray.length; i += pack_size) {
            int maxInGroup = Math.abs(paddedArray[i]);
            bitWidths[i / pack_size] = calculateBitWidth(maxInGroup);
        }

        System.out.println("位宽数组: " + Arrays.toString(bitWidths));

        byte[] compressed = encodeBitPacking(paddedArray, bitWidths, pack_size);
        System.out.println("压缩数据长度: " + compressed.length + " 字节");
        System.out.print("压缩数据十六进制: ");
        for (byte b : compressed) {
            System.out.print(String.format("%02X ", b & 0xFF));
        }
        System.out.println();

        // 打印压缩数据的二进制表示
        System.out.print("压缩数据二进制: ");
        for (byte b : compressed) {
            System.out.print(String.format("%8s ", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        System.out.println();

        try {
            int[] decompressed = decodeBitPacking(compressed, encoded.length, pack_size);
            System.out.println("解压后数据: " + Arrays.toString(decompressed));

            int[] finalResult = sprintzDecode(decompressed);
            System.out.println("最终解码: " + Arrays.toString(finalResult));

            boolean finalMatch = Arrays.equals(testData, finalResult);
            System.out.println("完整流程验证: " + (finalMatch ? "通过" : "失败"));
        } catch (Exception e) {
            System.out.println("解压异常: " + e.getMessage());
            e.printStackTrace();
        }

        // 单独测试bitPacking和unpackValues
        System.out.println("\n单独测试bitPacking和unpackValues:");
        byte[] testArray = new byte[10];
        int[] numbers = new int[]{100, 10}; // 二进制: 1100100 (7位), 1010 (4位)

        System.out.println("要编码的数字: " + Arrays.toString(numbers));
        System.out.println("数字100的二进制: " + Integer.toBinaryString(100));
        System.out.println("数字10的二进制: " + Integer.toBinaryString(10));

        int pos = bitPacking(numbers, 0, 2, 7, 0, testArray);
        System.out.println("写入后位置: " + pos);
        System.out.print("写入的字节数组: ");
        for (int i = 0; i < pos; i++) {
            System.out.print(String.format("%02X ", testArray[i] & 0xFF));
        }
        System.out.println();

        System.out.print("写入的字节数组二进制: ");
        for (int i = 0; i < pos; i++) {
            System.out.print(String.format("%8s ", Integer.toBinaryString(testArray[i] & 0xFF)).replace(' ', '0'));
        }
        System.out.println();

        int[] unpacked = unpackValues(testArray, 0, 7, 2);
        System.out.println("解包结果: " + Arrays.toString(unpacked));

        // 测试实际编码流程
        System.out.println("\n测试实际编码流程:");
        int[] testNumbers = {100, 10, 10, 10, 10, 10, 10, 10};
        int[] testBitWidths = {7, 4, 4, 4, 4, 4, 4, 4};
        byte[] testCompressed = encodeBitPacking(testNumbers, testBitWidths, 1);

        System.out.println("压缩数据: " + Arrays.toString(testCompressed));
        System.out.print("压缩数据十六进制: ");
        for (byte b : testCompressed) {
            System.out.print(String.format("%02X ", b & 0xFF));
        }
        System.out.println();

        System.out.print("压缩数据二进制: ");
        for (byte b : testCompressed) {
            System.out.print(String.format("%8s ", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        System.out.println();

        int[] testDecompressed = decodeBitPacking(testCompressed, testNumbers.length, 1);
        System.out.println("解压后数据: " + Arrays.toString(testDecompressed));

        boolean testMatch = Arrays.equals(testNumbers, testDecompressed);
        System.out.println("测试结果: " + (testMatch ? "通过" : "失败"));
    }

    // 8. 主测试方法 - 修复逻辑并使用BigDecimal
    public static void main(String[] args) throws IOException {
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println(file.getName());
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Verification Result"
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
            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal modelTime = BigDecimal.ZERO;
            BigDecimal modelDecodeTime = BigDecimal.ZERO;
            boolean allVerified = true;

            for (int j = 0; j < time_of_repeat; j++) {
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if (chunkNumbers.size() <= 2) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    // 原始数据
                    int[] scalingInt = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();

                    // Sprintz编码
                    int[] scaledInts = sprintz(scalingInt);

                    // 补齐到8的倍数
                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;
                    int[] paddedArray = new int[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);

                    // 计算位宽
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8];
                    for (int k = 0; k < actual_length; k += 8) {
                        int maxInGroup = 0;
                        for (int l = k; l < k + 8; l++) {
                            maxInGroup = Math.max(maxInGroup, paddedArray[l]);
                        }
                        bitWidths[k / 8] = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxInGroup));
                    }

                    // 压缩
                    byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, 8);
                    long duration = System.nanoTime() - startTime;

                    // 解压并验证
                    long decodeStartTime = System.nanoTime();
                    int[] decodedFromBitPacking = decodeBitPacking(compressedData, paddedArray.length, 8);
                    int[] actualDecoded = Arrays.copyOf(decodedFromBitPacking, scaledInts.length);
                    int[] finalDecoded = sprintzDecode(actualDecoded);
                    long decodeDuration = System.nanoTime() - decodeStartTime;

//                    // 验证
//                    boolean verified = verifyCompression(scalingInt, finalDecoded,"main");
//                    allVerified = allVerified && verified;
//
//                    if (!verified && j == 0) {
//                        System.out.println("验证失败在 chunk: " + i);
//                    }

                    // 累加统计
                    modelCost = modelCost.add(BigDecimal.valueOf(compressedData.length * 8L));
                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                }
            }

            // 计算平均值
            modelCost = modelCost.divide(BigDecimal.valueOf(time_of_repeat), 6, RoundingMode.HALF_UP);
            modelTime = modelTime.divide(BigDecimal.valueOf(time_of_repeat), 6, RoundingMode.HALF_UP);
            modelDecodeTime = modelDecodeTime.divide(BigDecimal.valueOf(time_of_repeat), 6, RoundingMode.HALF_UP);

            // 计算压缩比和吞吐量
            BigDecimal totalPoints = BigDecimal.valueOf(numbers.size());
            BigDecimal originalBits = totalPoints.multiply(BigDecimal.valueOf(64)); // 假设原始64位
            BigDecimal compressionRatio = modelCost.divide(originalBits, 6, RoundingMode.HALF_UP);

            // 吞吐量计算（数据点数 * 8000 / 时间(纳秒)）
            BigDecimal modelTimeThroughput = totalPoints
                    .multiply(BigDecimal.valueOf(8000))
                    .divide(modelTime, 6, RoundingMode.HALF_UP);

            BigDecimal modelDecodeTimeThroughput = totalPoints
                    .multiply(BigDecimal.valueOf(8000))
                    .divide(modelDecodeTime, 6, RoundingMode.HALF_UP);

            String[] record = {
                    file.toString(),
                    "Sprintz",
                    modelTimeThroughput.toString(),
                    modelDecodeTimeThroughput.toString(),
                    totalPoints.toString(),
                    modelCost.toString(),
                    compressionRatio.toString(),
                    allVerified ? "PASS" : "FAIL"
            };
            writer.writeRecord(record);
            writer.close();

            System.out.println("文件 " + file.getName() + " 验证结果: " + (allVerified ? "通过" : "失败"));
        }
    }

    @Test
    public void TestVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_SPRINTZ_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println(file.getName());
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio",
                    "Verification Result"
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

            int time_of_repeat = 100;

            for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allVerified = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() <= 2) continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);
                        long encodeStart = System.nanoTime();
                        int[] scaledInts = sprintz(scaledInt);

                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        int[] paddedArray = new int[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size];

                        for (int k = 0; k < actual_length; k += pack_size) {
                            int maxInGroup = 0;
                            for (int l = k; l < k + pack_size; l++) {
                                maxInGroup = Math.max(maxInGroup, paddedArray[l]);
                            }
                            bitWidths[k / pack_size] = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxInGroup));
                        }

                        byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                        long encodeDuration = System.nanoTime() - encodeStart;

                        long decodeStart = System.nanoTime();
                        int[] decodedFromBitPacking = decodeBitPacking(compressedData, paddedArray.length, pack_size);
                        int[] actualDecoded = Arrays.copyOf(decodedFromBitPacking, scaledInts.length);
                        sprintzDecode(actualDecoded);
                        long decodeDuration = System.nanoTime() - decodeStart;

                        modelTime = modelTime.add(BigDecimal.valueOf(encodeDuration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressedData.length * 8L));
                    }
                }

                modelCost = modelCost.divide(BigDecimal.valueOf(time_of_repeat), 6, RoundingMode.HALF_UP);
                modelTime = modelTime.divide(BigDecimal.valueOf(time_of_repeat), 6, RoundingMode.HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(BigDecimal.valueOf(time_of_repeat), 6, RoundingMode.HALF_UP);

                BigDecimal totalPoints = BigDecimal.valueOf(numbers.size());
                BigDecimal originalBits = totalPoints.multiply(BigDecimal.valueOf(64));
                BigDecimal compressionRatio = modelCost.divide(originalBits, 6, RoundingMode.HALF_UP);
                BigDecimal modelTimeThroughput = totalPoints
                        .multiply(BigDecimal.valueOf(8000))
                        .divide(modelTime, 6, RoundingMode.HALF_UP);
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    decodeThroughput =
                            totalPoints.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 6, RoundingMode.HALF_UP);
                }

                String[] record = {
                        file.toString(),
                        "SPRINTZ",
                        modelTimeThroughput.toString(),
                        decodeThroughput.toString(),
                        totalPoints.toString(),
                        modelCost.toString(),
                        String.valueOf(pack_size),
                        compressionRatio.toString(),
                        allVerified ? "PASS" : "FAIL"
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
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8, 1024*8};

        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
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
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;


                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {

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

                            for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                int maxInGroup = 0;
                                for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + pack_size; scaledInts_j++) {
                                    if (paddedArray[scaledInts_j] > maxInGroup) {
                                        maxInGroup = paddedArray[scaledInts_j];
                                    }
                                }

                                int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
                                bitWidths[scaledInts_i / pack_size] = bitWidth;
                            }

                            byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                            BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8);
                            long duration = System.nanoTime() - startTime;

                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "BP",
                            modelTime_throughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            modelCost.toPlainString(),
                            String.valueOf(pack_size),
                            model_ratio.toPlainString()
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }
}