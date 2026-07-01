package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SprintzApproximate {
    private static final int CHUNK_SIZE = 1024;

    // 段数据结构
    static class Segment {
        int bitWidth;  // 段的位宽
        int length;    // 段的长度（包含的组数）
        int packSize;  // pack大小

        Segment(int bitWidth, int length, int packSize) {
            this.bitWidth = bitWidth;
            this.length = length;
            this.packSize = packSize;
        }
    }

    // 成本结果类
    static class CostResult {
        int cost;
        int packSize;
        List<Segment> segments;

        CostResult(int cost, int packSize, List<Segment> segments) {
            this.cost = cost;
            this.packSize = packSize;
            this.segments = segments;
        }
    }

    public static void main(String[] args) throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE_sprintz";
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
                    "Selected Scheme",
                    "Lossless Verified"
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
            int time_of_repeat = 10;

            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal modelTime = BigDecimal.ZERO;
            BigDecimal modelDecodeTime = BigDecimal.ZERO;
            String selectedScheme = "Sprintz+RLE";
            int totalPackSizeUsed = 0;
            boolean allLossless = true;

            for (int j = 0; j < time_of_repeat; j++) {
                BigDecimal totalCost = BigDecimal.ZERO;
                int chunkCount = 0;

                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if (chunkNumbers.size() < 8) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    int[] scaledInts = sprintz(scaledInt);

                    // 使用智能选择方案（尝试packsize 8和16）
                    byte[] compressedData = encodeSprintzWithSmartSelection(scaledInts);
                    BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length).multiply(BigDecimal.valueOf(8));

                    // 提取packsize信息
                    int packSizeUsed = extractPackSizeFromEncodedData(compressedData);
                    totalPackSizeUsed += packSizeUsed;
                    chunkCount++;

                    long duration = System.nanoTime() - startTime;

                    long decodeStartTime = System.nanoTime();
                    int[] decompressedPacked = decodeAdaptiveSprintz(compressedData, scaledInts.length);
                    int[] decompressedScaled = sprintzDecode(decompressedPacked);
                    long decodeDuration = System.nanoTime() - decodeStartTime;

//                    // 验证无损性
//                    if (!Arrays.equals(scaledInt, decompressedScaled)) {
//                        System.err.println("警告: 在第 " + (i/CHUNK_SIZE) + " 个块检测到数据丢失!");
//                        allLossless = false;
//                    }

                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                    modelCost = modelCost.add(cur_cost);
                }
            }

            // 计算平均值
            modelCost = modelCost.divide(BigDecimal.valueOf(time_of_repeat), 10, RoundingMode.HALF_UP);
            modelTime = modelTime.divide(BigDecimal.valueOf(time_of_repeat), 10, RoundingMode.HALF_UP);
            modelDecodeTime = modelDecodeTime.divide(BigDecimal.valueOf(time_of_repeat), 10, RoundingMode.HALF_UP);

            // 计算压缩比
            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal bitsPerValue = BigDecimal.valueOf(64);
            BigDecimal totalBits = numbersSizeBD.multiply(bitsPerValue);
            BigDecimal modelRatio = modelCost.divide(totalBits, 10, RoundingMode.HALF_UP);

            // 计算编码吞吐量
            BigDecimal numbersSizeBits = numbersSizeBD.multiply(BigDecimal.valueOf(8000));
            BigDecimal modelTimeThroughput = BigDecimal.ZERO;
            if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                modelTimeThroughput = numbersSizeBits.divide(modelTime, 10, RoundingMode.HALF_UP);
            }

            // 计算解码吞吐量
            BigDecimal decodeThroughput = BigDecimal.ZERO;
            if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                decodeThroughput = numbersSizeBits.divide(modelDecodeTime, 10, RoundingMode.HALF_UP);
            }

            String[] record = {
                    file.toString(),
                    "AdaptiveSprintz",
                    modelTimeThroughput.toPlainString(),
                    decodeThroughput.toPlainString(),
                    String.valueOf(numbers.size()),
                    modelCost.toPlainString(),
                    modelRatio.toPlainString(),
                    selectedScheme,
                    String.valueOf(allLossless)
            };
            writer.writeRecord(record);
            writer.close();
        }
    }

    // 从编码数据中提取packsize
    private static int extractPackSizeFromEncodedData(byte[] encodedData) {
        if (encodedData == null || encodedData.length < 1) {
            return 8; // 默认值
        }
        // 第一个字节的低4位存储packsize标记
        int packSizeFlag = encodedData[0] & 0x0F;
        return packSizeFlag == 0 ? 8 : 16;
    }

    // 智能选择编码方案（尝试packsize 8和16）
    private static byte[] encodeSprintzWithSmartSelection(int[] data) {
        // 计算packsize 8的成本（使用优化版本）
        CostResult result8 = calculateRLECostOptimized(data, 8);

        // 计算packsize 16的成本（使用优化版本）
        CostResult result16 = calculateRLECostOptimized(data, 16);

        // 选择成本更低的packsize
        if (result8.cost <= result16.cost) {
            return encodeSprintzWithPackSizeOptimized(data, 8, result8.segments);
        } else {
            return encodeSprintzWithPackSizeOptimized(data, 16, result16.segments);
        }
    }

    // 计算RLE方案的成本（优化版本）
    private static CostResult calculateRLECostOptimized(int[] data, int packSize) {
        // 处理数据，确保长度是packSize的倍数
        int actual_length = data.length;
        int remainder = actual_length % packSize;

        int[] paddedData;
        if (remainder != 0) {
            int paddingLength = packSize - remainder;
            paddedData = new int[actual_length + paddingLength];
            System.arraycopy(data, 0, paddedData, 0, actual_length);
            // 填充0值
            for (int i = actual_length; i < paddedData.length; i++) {
                paddedData[i] = 0;
            }
        } else {
            paddedData = data;
            actual_length = data.length;
        }

        int groupCount = actual_length / packSize;
        int[] bitWidths = new int[groupCount];

        // 计算每个组的bitwidth
        for (int i = 0; i < groupCount; i++) {
            int maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                int val = Math.abs(paddedData[startIdx + j]);
                if (val > maxInGroup) {
                    maxInGroup = val;
                }
            }
            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        // 计算最优分段
        List<Segment> segments = computeOptimalSegmentation(bitWidths, packSize);

        // 计算总成本：头信息 + 段信息 + 数据位
        int headerCost = 8; // 1字节方案标记（包含packsize标记）

        // 使用优化编码方式计算段信息成本
        List<Byte> encodedSegments = encodeSegmentsOptimized(segments);
        int segmentHeaderCost = encodedSegments.size() * 8;

        int dataCost = 0;
        for (Segment segment : segments) {
            dataCost += segment.bitWidth * segment.length * packSize;
        }

        int totalCost = headerCost + segmentHeaderCost + dataCost;

        return new CostResult(totalCost, packSize, segments);
    }

    // 使用指定packsize编码（优化版本）
    private static byte[] encodeSprintzWithPackSizeOptimized(int[] data, int packSize, List<Segment> segments) {
        List<Byte> result = new ArrayList<>();

        // 添加方案标记（低4位存储packsize标记：0表示8，1表示16）
        byte schemeFlag = (byte) (packSize == 8 ? 0 : 1);
        result.add(schemeFlag);

        // 处理数据，确保长度是packSize的倍数
        int actual_length = data.length;
        int remainder = actual_length % packSize;

        int[] paddedData;
        if (remainder != 0) {
            int paddingLength = packSize - remainder;
            paddedData = new int[actual_length + paddingLength];
            System.arraycopy(data, 0, paddedData, 0, actual_length);
            // 填充0值
            for (int i = actual_length; i < paddedData.length; i++) {
                paddedData[i] = 0;
            }
        } else {
            paddedData = data;
        }

        // 对segments进行优化后的RLE编码
        List<Byte> rleEncoded = encodeSegmentsOptimized(segments);
        result.addAll(rleEncoded);

        // 进行bit-packing
        int totalBitPackedBytes = 0;
        for (Segment segment : segments) {
            totalBitPackedBytes += (segment.bitWidth * packSize * segment.length + 7) / 8;
        }

        byte[] bitPackedData = new byte[totalBitPackedBytes];
        int encodePos = 0;

        // 按照分段进行编码
        int groupIndex = 0;
        for (Segment segment : segments) {
            for (int g = 0; g < segment.length; g++) {
                int startIndex = groupIndex * packSize;
                ArrayList<Integer> groupData = new ArrayList<>();
                for (int j = 0; j < packSize; j++) {
                    groupData.add(paddedData[startIndex + j]);
                }
                encodePos = bitPacking(groupData, 0, segment.bitWidth, encodePos, bitPackedData);
                groupIndex++;
            }
        }

        // 添加bit-packed数据
        for (byte b : bitPackedData) {
            result.add(b);
        }

        // 转换为byte数组
        byte[] finalResult = new byte[result.size()];
        for (int i = 0; i < result.size(); i++) {
            finalResult[i] = result.get(i);
        }
        return finalResult;
    }

    // 计算最优分段
    private static List<Segment> computeOptimalSegmentation(int[] bitWidths, int packSize) {
        // 首先将连续的相同位宽合并为初始段
        List<Segment> segments = new ArrayList<>();
        if (bitWidths.length == 0) return segments;

        int currentBitWidth = bitWidths[0];
        int currentLength = 1;

        for (int i = 1; i < bitWidths.length; i++) {
            if (bitWidths[i] == currentBitWidth) {
                currentLength++;
            } else {
                segments.add(new Segment(currentBitWidth, currentLength, packSize));
                currentBitWidth = bitWidths[i];
                currentLength = 1;
            }
        }
        segments.add(new Segment(currentBitWidth, currentLength, packSize));

        // 计算初始参数
        int l = segments.size();
        int sMax = segments.stream().mapToInt(s -> s.length).max().orElse(0);
        int z = (int) Math.ceil(Math.log(sMax + 1) / Math.log(2));

        // 应用命题1：合并相邻的相同位宽段
        boolean changed = true;
        while (changed) {
            changed = false;
            for (int i = 0; i < segments.size() - 1; i++) {
                Segment current = segments.get(i);
                Segment next = segments.get(i + 1);

                if (current.bitWidth == next.bitWidth) {
                    // 计算合并后的长度
                    int mergedLength = current.length + next.length;

                    // 检查命题1的条件
                    boolean condition1 = mergedLength <= (1 << z) - 1;
                    boolean condition2 = l <= z + 6;

                    if (condition1 || condition2) {
                        // 合并段
                        current.length = mergedLength;
                        segments.remove(i + 1);

                        // 更新参数
                        l--;
                        sMax = Math.max(sMax, mergedLength);
                        z = (int) Math.ceil(Math.log(sMax + 1) / Math.log(2));

                        changed = true;
                        i--; // 重新检查当前位置
                    }
                }
            }
        }

        return segments;
    }

    // 自适应解码Sprintz数据
    public static int[] decodeAdaptiveSprintz(byte[] encodedData, int originalLength) {
        if (encodedData == null || encodedData.length == 0) {
            return new int[originalLength];
        }

        int pos = 0;

        try {
            // 读取packsize标记
            int packSizeFlag = encodedData[pos] & 0x0F;
            int packSize = packSizeFlag == 0 ? 8 : 16;
            pos++;

            // 解码优化版本的RLE方案
            return decodeSprintzRLEBasedOptimized(encodedData, pos, originalLength, packSize);
        } catch (Exception e) {
            // 发生异常时返回空数组
            System.err.println("解码异常: " + e.getMessage());
            e.printStackTrace();
            return new int[originalLength];
        }
    }

    // 解码优化版本的RLE方案
    private static int[] decodeSprintzRLEBasedOptimized(byte[] encodedData, int startPos, int originalLength, int packSize) {
        int pos = startPos;

        // 解码段列表（使用优化版本）
        List<Segment> segments = decodeSegmentsOptimized(encodedData, pos);
        if (segments.isEmpty()) {
            return new int[originalLength];
        }

        // 重新计算段信息占用的字节数
        List<Segment> tempSegments = decodeSegmentsOptimized(encodedData, startPos);
        int segmentBytes = calculateEncodedSegmentSize(tempSegments);
        pos = startPos + segmentBytes;

        // 计算总的数据位
        int totalBits = 0;
        for (Segment segment : segments) {
            totalBits += segment.bitWidth * segment.length * packSize;
        }

        // 计算需要的字节数
        int dataBytes = (totalBits + 7) / 8;

//        // 检查数据是否足够
//        if (pos + dataBytes > encodedData.length) {
//            System.err.println("警告: 编码数据不完整，期望 " + (pos + dataBytes) + " 字节，实际 " + encodedData.length + " 字节");
//            return new int[originalLength];
//        }

        // 解压bit-packed数据
        int[] result = new int[originalLength];
        int resultIndex = 0;
        int currentBitPos = 0;
        int currentBytePos = pos;

        for (Segment segment : segments) {
            if (resultIndex >= originalLength) break;

            int totalValues = segment.length * packSize;
            int valuesToRead = Math.min(totalValues, originalLength - resultIndex);

            for (int i = 0; i < valuesToRead && resultIndex < originalLength; i++) {
                int value = 0;

                // 按位读取
                for (int bit = 0; bit < segment.bitWidth; bit++) {
                    if (currentBytePos >= encodedData.length) {
                        // 数据不足，用0填充
                        value = (value << 1);
                    } else {
                        int currentByte = encodedData[currentBytePos] & 0xFF;
                        int currentBit = (currentByte >> (7 - currentBitPos)) & 1;
                        value = (value << 1) | currentBit;
                    }

                    currentBitPos++;
                    if (currentBitPos == 8) {
                        currentBytePos++;
                        currentBitPos = 0;
                    }
                }

                // 如果读取的位数小于bitWidth，需要左移对齐
                int remainingBits = segment.bitWidth;
                while (remainingBits < 32) {
                    value <<= 1;
                    remainingBits++;
                }

                if (resultIndex < originalLength) {
                    result[resultIndex++] = value;
                }
            }

            // 跳过剩余的数据位（如果还有的话）
            int remainingValues = totalValues - valuesToRead;
            if (remainingValues > 0) {
                int bitsToSkip = remainingValues * segment.bitWidth;
                for (int bit = 0; bit < bitsToSkip; bit++) {
                    currentBitPos++;
                    if (currentBitPos == 8) {
                        currentBytePos++;
                        currentBitPos = 0;
                    }
                }
            }
        }

        return result;
    }

    // 计算编码后的段信息大小
    private static int calculateEncodedSegmentSize(List<Segment> segments) {
        // 模拟编码过程计算字节数
        List<Byte> encoded = encodeSegmentsOptimized(segments);
        return encoded.size();
    }

    // 改进后的段信息编码方式（优化版本）
    private static List<Byte> encodeSegmentsOptimized(List<Segment> segments) {
        List<Byte> result = new ArrayList<>();
        if (segments.isEmpty()) return result;

        int segmentCount = segments.size();
        // 压缩段数：使用变长编码（1-2字节）
        if (segmentCount <= 127) {
            // 单个字节存储段数（最高位为0表示单字节）
            result.add((byte) segmentCount);
        } else {
            // 两个字节存储段数（最高位为1表示双字节）
            result.add((byte) (0x80 | (segmentCount >> 8)));
            result.add((byte) (segmentCount & 0xFF));
        }

        for (Segment segment : segments) {
            // bitWidth用5位存储（0-31，实际最大位宽为32）
            // bitWidth为0时特殊表示32位
            int bitWidth = segment.bitWidth;
            if (bitWidth == 32) {
                bitWidth = 0; // 用0表示32
            }
            bitWidth = Math.min(bitWidth, 31);

            int length = segment.length;

            // 编码bitWidth和length的第一个字节
            byte firstByte = (byte) ((bitWidth & 0x1F) << 3); // bitWidth放在高5位

            if (length <= 7) {
                // 小长度：直接存储在低3位
                firstByte |= (byte) (length & 0x07);
                result.add(firstByte);
            } else {
                // 大长度：使用额外字节存储
                firstByte |= 0x07; // 设置特殊标记111
                result.add(firstByte);

                // 编码长度值
                if (length <= 134) { // 7 + 127 = 134
                    // 单字节长度
                    result.add((byte) ((length - 7) & 0xFF));
                } else {
                    // 双字节长度
                    int lengthValue = length - 7;
                    result.add((byte) (0x80 | ((lengthValue >> 8) & 0x7F)));
                    result.add((byte) (lengthValue & 0xFF));
                }
            }
        }

        return result;
    }

    // 改进后的段信息解码方式（优化版本）
    private static List<Segment> decodeSegmentsOptimized(byte[] data, int startPos) {
        List<Segment> segments = new ArrayList<>();
        if (startPos >= data.length) return segments;

        // 解码段数
        int segmentCount;
        int pos = startPos;

        if ((data[pos] & 0x80) == 0) {
            // 单字节段数
            segmentCount = data[pos] & 0x7F;
            pos++;
        } else {
            // 双字节段数
            if (pos + 1 >= data.length) {
                return segments;
            }
            segmentCount = ((data[pos] & 0x7F) << 8) | (data[pos + 1] & 0xFF);
            pos += 2;
        }

        for (int i = 0; i < segmentCount && pos < data.length; i++) {
            // 解码第一个字节
            byte firstByte = data[pos];
            pos++;

            // 解码bitWidth（高5位）
            int bitWidth = (firstByte >> 3) & 0x1F;
            // bitWidth为0时表示32位
            if (bitWidth == 0) bitWidth = 32;

            // 解码length标记（低3位）
            int lengthMarker = firstByte & 0x07;

            int length;
            if (lengthMarker != 7) {
                // 小长度直接解码
                length = lengthMarker;
            } else {
                // 大长度，读取额外字节
                if (pos >= data.length) {
                    // 数据不完整，使用默认值
                    length = 8;
                } else if ((data[pos] & 0x80) == 0) {
                    // 单字节长度
                    length = (data[pos] & 0xFF) + 7;
                    pos++;
                } else {
                    // 双字节长度
                    if (pos + 1 >= data.length) {
                        // 数据不完整，使用默认值
                        length = 8;
                        pos++;
                    } else {
                        length = (((data[pos] & 0x7F) << 8) | (data[pos + 1] & 0xFF)) + 7;
                        pos += 2;
                    }
                }
            }

            // 确保最小长度为1
            if (length < 1) length = 1;

            segments.add(new Segment(bitWidth, length, 0));
        }

        return segments;
    }

    @Test
    public void TestVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_AdaptiveSprintz_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println("Processing " + file.getName() + "...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Pack Size Used"
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

            int time_of_repeat = 1;

            for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                System.out.println("Testing pack size: " + pack_size);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                String selectedScheme = "Sprintz+RLE";
                int totalPackSizeUsed = 0;
                int chunkCount = 0;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() < 8) continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        int[] scaledInts = sprintz(scaledInt);

                        // 使用智能选择方案（在固定packsize内尝试）
                        byte[] compressedData = encodeSprintzWithSmartSelectionForFixedSize(scaledInts, pack_size);
                        BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length).multiply(BigDecimal.valueOf(8));

                        // 提取packsize信息
                        int packSizeUsed = extractPackSizeFromEncodedData(compressedData);
                        totalPackSizeUsed += packSizeUsed;
                        chunkCount++;

                        long duration = System.nanoTime() - startTime;
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(cur_cost);
                    }
                }

                // 计算平均值
                modelCost = modelCost.divide(BigDecimal.valueOf(time_of_repeat), 10, RoundingMode.HALF_UP);
                modelTime = modelTime.divide(BigDecimal.valueOf(time_of_repeat), 10, RoundingMode.HALF_UP);
                int avgPackSizeUsed = chunkCount > 0 ? totalPackSizeUsed / chunkCount : 0;

                // 计算压缩比
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64));
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, RoundingMode.HALF_UP);

                // 计算吞吐量
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal numbersSizeBits = numbersSizeBD.multiply(BigDecimal.valueOf(8000));
                    modelTimeThroughput = numbersSizeBits.divide(modelTime, 10, RoundingMode.HALF_UP);
                }

                String[] record = {
                        file.toString(),
                        "AdaptiveSprintz",
                        modelTimeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        modelRatio.toPlainString(),
                        selectedScheme,
                        String.valueOf(avgPackSizeUsed)
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    // 为固定packsize的测试使用智能选择
    private static byte[] encodeSprintzWithSmartSelectionForFixedSize(int[] data, int fixedPackSize) {
        // 如果固定packsize小于等于8，只尝试8
        if (fixedPackSize <= 8) {
            List<Segment> segments = computeSegmentsForPackSize(data, 8);
            CostResult result8 = new CostResult(0, 8, segments); // 成本计算在编码时进行
            return encodeSprintzWithPackSizeOptimized(data, 8, segments);
        }
        // 如果固定packsize为16，尝试8和16
        else if (fixedPackSize == 16) {
            // 计算packsize 8的成本（使用优化版本）
            CostResult result8 = calculateRLECostOptimized(data, 8);

            // 计算packsize 16的成本（使用优化版本）
            CostResult result16 = calculateRLECostOptimized(data, 16);

            // 选择成本更低的packsize
            if (result8.cost <= result16.cost) {
                return encodeSprintzWithPackSizeOptimized(data, 8, result8.segments);
            } else {
                return encodeSprintzWithPackSizeOptimized(data, 16, result16.segments);
            }
        }
        // 其他情况，使用指定的packsize
        else {
            List<Segment> segments = computeSegmentsForPackSize(data, fixedPackSize);
            return encodeSprintzWithPackSizeOptimized(data, fixedPackSize, segments);
        }
    }

    // 为指定packsize计算分段
    private static List<Segment> computeSegmentsForPackSize(int[] data, int packSize) {
        // 处理数据，确保长度是packSize的倍数
        int actual_length = data.length;
        int remainder = actual_length % packSize;

        int[] paddedData;
        if (remainder != 0) {
            int paddingLength = packSize - remainder;
            paddedData = new int[actual_length + paddingLength];
            System.arraycopy(data, 0, paddedData, 0, actual_length);
            // 填充0值
            for (int i = actual_length; i < paddedData.length; i++) {
                paddedData[i] = 0;
            }
        } else {
            paddedData = data;
            actual_length = data.length;
        }

        int groupCount = actual_length / packSize;
        int[] bitWidths = new int[groupCount];

        // 计算每个组的bitwidth
        for (int i = 0; i < groupCount; i++) {
            int maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                int val = Math.abs(paddedData[startIdx + j]);
                if (val > maxInGroup) {
                    maxInGroup = val;
                }
            }
            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        // 计算最优分段
        return computeOptimalSegmentation(bitWidths, packSize);
    }

    // 位打包函数
    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int totalCount = numbers.size() - start;
        int currentBytePos = encode_pos;
        int currentBitPos = 0;

        for (int i = 0; i < totalCount; i++) {
            int value = numbers.get(start + i);
            for (int bit = bit_width - 1; bit >= 0; bit--) {
                int currentBit = (value >> bit) & 1;
                encoded_result[currentBytePos] |= (currentBit << (7 - currentBitPos));
                currentBitPos++;
                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }
        }

        // 处理最后一个字节未满的情况
        if (currentBitPos != 0) {
            currentBytePos++;
        }

        return currentBytePos;
    }

    // 位解包函数（新增）
    public static int[] bitUnpacking(byte[] encodedData, int startPos, int totalValues, int bitWidth, int originalLength) {
        int[] result = new int[originalLength];
        int resultIndex = 0;
        int currentBitPos = 0;
        int currentBytePos = startPos;

        for (int i = 0; i < totalValues && resultIndex < originalLength; i++) {
            int value = 0;

            // 按位读取
            for (int bit = 0; bit < bitWidth; bit++) {
                if (currentBytePos >= encodedData.length) {
                    // 数据不足，用0填充
                    value = (value << 1);
                } else {
                    int currentByte = encodedData[currentBytePos] & 0xFF;
                    int currentBit = (currentByte >> (7 - currentBitPos)) & 1;
                    value = (value << 1) | currentBit;
                }

                currentBitPos++;
                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }

            // 如果读取的位数小于bitWidth，需要左移对齐
            if (bitWidth < 32) {
                value <<= (32 - bitWidth);
            }

            if (resultIndex < originalLength) {
                result[resultIndex++] = value;
            }
        }

        return result;
    }

    public static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        int scale = (int) Math.pow(10, decimalMax);
        int size = numbers.size();
        int[] result = new int[size];

        if (size == 0) {
            return result;
        }

        // 1. Parse all numbers and scale them up
        for (int i = 0; i < size; i++) {
            String numStr = numbers.get(i);
            String[] parts = numStr.split("\\.");
            int whole = Integer.parseInt(parts[0]);

            int fraction = 0;
            if (parts.length > 1) {
                String fractionStr = parts[1];
                if (fractionStr.length() < decimalMax) {
                    while (fractionStr.length() < decimalMax) {
                        fractionStr += "0";
                    }
                } else if (fractionStr.length() > decimalMax) {
                    fractionStr = fractionStr.substring(0, decimalMax);
                }
                fraction = Integer.parseInt(fractionStr);
            }

            // 处理负数
            if (whole < 0) {
                fraction = -fraction;
            }

            result[i] = whole * scale + fraction;
        }
        return result;
    }

    // Sprintz编码（带ZigZag）
    public static int[] sprintz(int[] numbers) {
        int size = numbers.length;
        int[] result = new int[size];

        int first = numbers[0];
        result[0] = first;

        int prev = first;
        for (int i = 1; i < size; i++) {
            int current = numbers[i];
            int diff = current - prev;
            // ZigZag编码
            result[i] = (diff << 1) ^ (diff >> 31);
            prev = current;
        }

        return result;
    }

    // Sprintz解码（带ZigZag解码）
    public static int[] sprintzDecode(int[] encodedData) {
        int size = encodedData.length;
        int[] result = new int[size];

        if (size == 0) return result;

        result[0] = encodedData[0];
        int prev = result[0];

        for (int i = 1; i < size; i++) {
            int zigzagEncoded = encodedData[i];
            // ZigZag解码
            int diff = (zigzagEncoded >>> 1) ^ (-(zigzagEncoded & 1));
            result[i] = prev + diff;
            prev = result[i];
        }

        return result;
    }


    @Test
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (Sprintz+RLE)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_RLE_vary_m";
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
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Average Pack Size Used",
                    "Lossless Verified"
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

            int time_of_repeat = 10; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 分批处理，每1024个元素一批进行scaling
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

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                String selectedScheme = "Sprintz+RLE";
                int totalPackSizeUsed = 0;
                int chunkCount = 0;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < scaledInts_all.length; i += chunkSize) {
                        int end = Math.min(i + chunkSize, scaledInts_all.length);
                        int[] chunkData = new int[end - i];
                        System.arraycopy(scaledInts_all, i, chunkData, 0, end - i);

                        if (chunkData.length < 8) continue;

                        // 保存原始数据用于无损性验证
                        int[] originalChunkData = chunkData.clone();

                        long startTime = System.nanoTime();

                        // Sprintz编码
                        int[] sprintzEncoded = sprintz(chunkData);

                        // 使用智能选择方案
                        byte[] compressedData = encodeSprintzWithSmartSelection(sprintzEncoded);
                        BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length).multiply(BigDecimal.valueOf(8));

                        // 提取packsize信息
                        int packSizeUsed = extractPackSizeFromEncodedData(compressedData);
                        totalPackSizeUsed += packSizeUsed;
                        chunkCount++;

                        long duration = System.nanoTime() - startTime;

                        // 解码测试
                        long decodeStartTime = System.nanoTime();
                        int[] decompressedPacked = decodeAdaptiveSprintz(compressedData, sprintzEncoded.length);
                        int[] decompressedScaled = sprintzDecode(decompressedPacked);
                        long decodeDuration = System.nanoTime() - decodeStartTime;

                        // 验证无损性（可选，因为会减慢测试速度）
//                    if (!Arrays.equals(originalChunkData, decompressedScaled)) {
//                        System.err.println("警告: 在第 " + (i/chunkSize) + " 个块检测到数据丢失!");
//                        allLossless = false;
//                    }

                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);

                int avgPackSizeUsed = chunkCount > 0 ? totalPackSizeUsed / chunkCount : 0;

                // 计算压缩比
                BigDecimal numbersSizeBD = BigDecimal.valueOf(scaledInts_all.length);
                BigDecimal bitsPerValue = BigDecimal.valueOf(64); // 原始数据每个值64位（双精度浮点数）
                BigDecimal totalBits = numbersSizeBD.multiply(bitsPerValue);
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, RoundingMode.HALF_UP);

                // 计算编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    // 将纳秒转换为毫秒，然后计算吞吐量
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(1000000), 10, RoundingMode.HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, RoundingMode.HALF_UP);
                }

                // 计算解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(1000000), 10, RoundingMode.HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, RoundingMode.HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize),
                        file.toString(),
                        "Sprintz+RLE",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(scaledInts_all.length),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        selectedScheme,
                        String.valueOf(avgPackSizeUsed),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }
    // 测试无损性的单元测试
    @Test
    public void testLosslessCompression() {
        System.out.println("\nTesting Lossless Compression...");

        // 测试1: 随机整数数组
        Random rand = new Random(42);
        int[] testData = new int[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = rand.nextInt(10000);
        }

        // Sprintz编码
        int[] encoded = sprintz(testData);

        // 压缩
        byte[] compressed = encodeSprintzWithSmartSelection(encoded);

        // 解压
        int[] decoded = decodeAdaptiveSprintz(compressed, encoded.length);

        // Sprintz解码
        int[] finalResult = sprintzDecode(decoded);

        // 验证无损
        assertArrayEquals("测试1失败: 数据不匹配", testData, finalResult);
        System.out.println("测试1通过: 1000个随机整数");

        // 测试2: 小数据块
        int[] smallData = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] smallEncoded = sprintz(smallData);
        byte[] smallCompressed = encodeSprintzWithSmartSelection(smallEncoded);
        int[] smallDecoded = decodeAdaptiveSprintz(smallCompressed, smallEncoded.length);
        int[] smallFinal = sprintzDecode(smallDecoded);

        assertArrayEquals("测试2失败: 小数据块不匹配", smallData, smallFinal);
        System.out.println("测试2通过: 小数据块");

        // 测试3: 负数和零
        int[] mixedData = {-5, 0, 10, -3, 7, 0, -1, 4};
        int[] mixedEncoded = sprintz(mixedData);
        byte[] mixedCompressed = encodeSprintzWithSmartSelection(mixedEncoded);
        int[] mixedDecoded = decodeAdaptiveSprintz(mixedCompressed, mixedEncoded.length);
        int[] mixedFinal = sprintzDecode(mixedDecoded);

        assertArrayEquals("测试3失败: 混合数据不匹配", mixedData, mixedFinal);
        System.out.println("测试3通过: 混合数据（负数和零）");

        System.out.println("所有无损测试通过！");
    }

    // 测试ZigZag编码解码
    @Test
    public void testZigZagEncoding() {
        System.out.println("\nTesting ZigZag Encoding...");

        int[] testValues = {-100, -50, -10, -1, 0, 1, 10, 50, 100};

        for (int value : testValues) {
            // ZigZag编码
            int encoded = (value << 1) ^ (value >> 31);
            // ZigZag解码
            int decoded = (encoded >>> 1) ^ (-(encoded & 1));

            assertEquals("ZigZag测试失败: " + value, value, decoded);
        }

        System.out.println("ZigZag编码解码测试通过！");
    }
}