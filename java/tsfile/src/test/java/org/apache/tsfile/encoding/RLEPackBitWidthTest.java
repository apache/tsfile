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

    private static final int CHUNK_SIZE = 1024;
    private static final int BASELINE_PACK_SIZE = 8;

    // 新增：压缩方案标记
    static final byte SCHEME_BASELINE = 0;  // 使用baseline bitpacking
    static final byte SCHEME_RLE = 1;       // 使用RLE分段压缩

    // 新增：数据特征分析
    static class DataFeatures {
        double variance;          // 方差
        int bitWidthRange;        // 位宽范围
        double entropy;           // 熵
        double compressionScore;  // 压缩评分

        DataFeatures(double variance, int bitWidthRange, double entropy) {
            this.variance = variance;
            this.bitWidthRange = bitWidthRange;
            this.entropy = entropy;
            this.compressionScore = calculateCompressionScore();
        }

        private double calculateCompressionScore() {
            // 综合多个特征计算压缩潜力评分
            return (1.0 / (1 + Math.sqrt(variance))) * (1.0 / (1 + bitWidthRange/10.0)) * (1.0 / (1 + entropy));
        }
    }

    // 段数据结构
    static class Segment {
        int bitWidth;  // 段的位宽
        int length;    // 段的长度（包含的组数）
        double cost;   // 段的成本
        int packSize;  // pack大小

        Segment(int bitWidth, int length, int packSize) {
            this.bitWidth = bitWidth;
            this.length = length;
            this.packSize = packSize;
            this.cost = calculateCost();
        }

        private double calculateCost() {
            // 段的成本包括段头成本(24bits)和数据存储成本
            return 24 + bitWidth * length * packSize;
        }

        // 检查是否可以与另一个段合并（优化版）
        boolean canMergeWith(Segment other, double threshold) {
            int mergedBitWidth = Math.max(this.bitWidth, other.bitWidth);
            int mergedLength = this.length + other.length;

            // 合并后的成本
            double mergedCost = 24 + mergedBitWidth * mergedLength * this.packSize;
            double originalCost = this.cost + other.cost;

            // 计算压缩收益
            double compressionBenefit = (originalCost - mergedCost) / originalCost;

            // 只有当压缩收益超过阈值时才合并
            return compressionBenefit > threshold;
        }

        // 合并两个段
        Segment merge(Segment other) {
            int mergedBitWidth = Math.max(this.bitWidth, other.bitWidth);
            int mergedLength = this.length + other.length;
            return new Segment(mergedBitWidth, mergedLength, this.packSize);
        }
    }

    // 新增：压缩结果
    static class CompressionResult {
        byte[] data;
        byte scheme;  // 0: baseline, 1: RLE
        int compressedSize;
        double compressionRatio;

        CompressionResult(byte[] data, byte scheme) {
            this.data = data;
            this.scheme = scheme;
            this.compressedSize = data.length * 8;
        }
    }

    // 新增：分段评估结果
    static class SegmentationResult {
        List<Segment> segments;
        double totalCost;
        int numSegments;
        double compressionRatio;

        SegmentationResult(List<Segment> segments, int originalSize) {
            this.segments = segments;
            this.totalCost = segments.stream().mapToDouble(s -> s.cost).sum();
            this.numSegments = segments.size();
            this.compressionRatio = originalSize / this.totalCost;
        }
    }

    public static void main(String[] args) throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_AdaptivePack_Improved";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Segmentation Strategy",
                    "Average Segment Length",
                    "Merging Threshold"
            };
            writer.writeRecord(head);
            System.out.println("Processing " + file.getName() + "...");

            // 读取数据
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

            // 测试不同的合并策略
            double[] thresholds = {0.0, 0.05, 0.1, 0.15, 0.2};
            String[] strategies = {"BASIC", "DP_OPTIMAL", "ADAPTIVE", "TWO_LEVEL", "FAST_GREEDY", "HYBRID"};

            for (String strategy : strategies) {
                for (double threshold : thresholds) {
                    long modelCost = 0;
                    long modelTime = 0;
                    long modelDecodeTime = 0;
                    int baselineCount = 0;
                    int rleCount = 0;
                    double avgSegmentLength = 0;

                    for(int j = 0; j < time_of_repeat; j++){
                        int totalCost = 0;
                        int totalSegments = 0;

                        for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                            List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                            if(chunkNumbers.size() < 8) continue;

                            int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                    .stream().max(Integer::compare).orElse(0);

                            int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                            long startTime = System.nanoTime();

                            // 使用智能选择方案
                            CompressionResult compResult = encodeWithSmartSelection(scaledInts, 8, strategy, threshold);
                            int curCost = compResult.compressedSize;

                            // 统计方案选择
                            if (compResult.scheme == SCHEME_BASELINE) {
                                baselineCount++;
                            } else {
                                rleCount++;

                                // 获取分段信息
                                SegmentationResult segResult = getSegmentationInfo(scaledInts, 8, strategy, threshold);
                                totalSegments += segResult.numSegments;
                            }

                            long duration = System.nanoTime() - startTime;
                            modelTime += duration;
                            modelCost += curCost;

                            // 解码测试
                            long startDecodeTime = System.nanoTime();
                            int[] decodedData = decodeWithSmartSelection(compResult.data, scaledInts.length, 8);
                            long decodeDuration = System.nanoTime() - startDecodeTime;
                            modelDecodeTime += decodeDuration;
                        }

                        avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * 8) : 0;
                    }

                    // 计算统计结果
                    modelCost /= time_of_repeat;
                    modelTime = modelTime / time_of_repeat;
                    modelDecodeTime = modelDecodeTime / time_of_repeat;

                    double model_ratio = (double) modelCost / (double) (numbers.size()*64);
                    double modelTime_throughput = modelTime > 0 ? (double)(numbers.size()*8000L) / (double) (modelTime) : 0;
                    double modelDecodeTime_throughput = modelDecodeTime > 0 ? (double)(numbers.size()*8000L) / (double) (modelDecodeTime) : 0;

                    // 确定选择的方案
                    String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                    // 输出结果
                    String[] record = {
                            file.toString(),
                            "AdaptivePack_Improved",
                            String.valueOf(modelTime_throughput),
                            String.valueOf(modelDecodeTime_throughput),
                            String.valueOf(numbers.size()),
                            String.valueOf(modelCost),
                            String.valueOf(model_ratio),
                            selectedScheme,
                            strategy,
                            String.valueOf(avgSegmentLength),
                            String.valueOf(threshold)
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }

    @Test
    public void TestVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE_vary_pack_size";
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
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Segmentation Strategy",
                    "Average Segment Length",
                    "Merging Threshold"
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

            int time_of_repeat = 10;

            // 使用优化的HYBRID策略和自适应阈值
            String strategy = "HYBRID";
            double threshold = 0.3;

            for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                System.out.println("Testing pack size: " + pack_size);

                long modelCost = 0;
                long modelTime = 0;
                long modelDecodeTime = 0;
                int baselineCount = 0;
                int rleCount = 0;
                double avgSegmentLength = 0;

                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    int totalSegments = 0;

                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() < 8) continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(scaledInts, pack_size, strategy, threshold);
                        int cur_cost = compResult.compressedSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(scaledInts, pack_size, strategy, threshold);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        modelTime += duration;
                        modelCost += cur_cost;

                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        int[] decodedData = decodeWithSmartSelection(compResult.data, scaledInts.length, pack_size);
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        modelDecodeTime += decodeDuration;
                    }

                    avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * pack_size) : 0;
                }

                modelCost /= time_of_repeat;
                modelTime = modelTime / time_of_repeat;
                modelDecodeTime = modelDecodeTime / time_of_repeat;

                double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                double modelTime_throughput = modelTime > 0 ? (double) (numbers.size() * 8000) / (double) (modelTime) : 0;
                double modelDecodeTime_throughput = modelDecodeTime > 0 ? (double) (numbers.size() * 8000) / (double) (modelDecodeTime) : 0;

                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                String[] record = {
                        file.toString(),
                        "AdaptivePack_Improved",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(modelDecodeTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        strategy,
                        String.valueOf(avgSegmentLength),
                        String.valueOf(threshold)
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
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE_vary_chunk_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes
        int[] chunkSizes = {16 * 8, 32 * 8, 64 * 8, 128 * 8, 256 * 8, 512 * 8, 1024 * 8};

        // 使用优化的HYBRID策略和自适应阈值，pack_size=8
        String strategy = "HYBRID";
        double threshold = 0.1;
        int pack_size = 8;

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头
            String[] head = {
                    "Chunk Size (m)",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Segmentation Strategy",
                    "Average Segment Length",
                    "Merging Threshold"
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

            int time_of_repeat = 50;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 预处理所有数据
            int batchSize = 1024;
            List<int[]> batches = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += batchSize) {
                int end = Math.min(numbers.size(), i + batchSize);
                List<String> batch = numbers.subList(i, end);
                int[] scaledBatch = scaleNumbers(batch, decimalMax);
                batches.add(scaledBatch);
            }

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

                long modelCost = 0;
                long modelTime = 0;
                long modelDecodeTime = 0;
                int baselineCount = 0;
                int rleCount = 0;
                double avgSegmentLength = 0;

                for (int j = 0; j < time_of_repeat; j++) {
                    int totalSegments = 0;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        int[] scaledInts = new int[end - i];

                        if (end - i >= 0) {
                            System.arraycopy(scaledInts_all, i, scaledInts, 0, end - i);
                        }

                        long startTime = System.nanoTime();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(scaledInts, pack_size, strategy, threshold);
                        int cur_cost = compResult.compressedSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(scaledInts, pack_size, strategy, threshold);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        modelTime += duration;
                        modelCost += cur_cost;

                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        int[] decodedData = decodeWithSmartSelection(compResult.data, scaledInts.length, pack_size);
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        modelDecodeTime += decodeDuration;
                    }

                    avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * pack_size) : 0;
                }

                modelCost /= time_of_repeat;
                modelTime = modelTime / time_of_repeat;
                modelDecodeTime = modelDecodeTime / time_of_repeat;

                double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                double modelTime_throughput = modelTime > 0 ? (double) (numbers.size() * 8000) / (double) (modelTime) : 0;
                double modelDecodeTime_throughput = modelDecodeTime > 0 ? (double) (numbers.size() * 8000) / (double) (modelDecodeTime) : 0;

                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                String[] record = {
                        String.valueOf(chunkSize / 8),
                        file.toString(),
                        "AdaptivePack_Improved",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(modelDecodeTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        strategy,
                        String.valueOf(avgSegmentLength),
                        String.valueOf(threshold)
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    // 新增：智能选择编码方案
    private static CompressionResult encodeWithSmartSelection(int[] data, int packSize, String strategy, double threshold) {
        // 计算baseline方案的压缩成本
        int baselineCost = calculateBaselineCost(data, packSize);

        // 计算RLE方案的压缩成本
        int rleCost = calculateRLECost(data, packSize, strategy, threshold);

        // 选择成本更小的方案
        if (baselineCost <= rleCost) {
            // 使用baseline方案
            byte[] compressedData = encodeWithBaseline(data, packSize);
            return new CompressionResult(compressedData, SCHEME_BASELINE);
        } else {
            // 使用RLE方案
            byte[] compressedData = encodeWithImprovedRLE(data, packSize, strategy, threshold);
            return new CompressionResult(compressedData, SCHEME_RLE);
        }
    }

    // 新增：计算baseline方案的压缩成本
    private static int calculateBaselineCost(int[] data, int packSize) {
        // 计算最大位宽
        int maxVal = 0;
        for (int value : data) {
            if (value > maxVal) {
                maxVal = value;
            }
        }
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxVal);

        // baseline成本：1字节（方案标记）+ 1字节（位宽）+ 数据位
        int headerCost = 16; // 2字节 = 16bits
        int dataCost = bitWidth * data.length;

        return headerCost + dataCost;
    }

    // 新增：计算RLE方案的压缩成本
    private static int calculateRLECost(int[] data, int packSize, String strategy, double threshold) {
        // 获取分段信息
        SegmentationResult segResult = getSegmentationInfo(data, packSize, strategy, threshold);

        // RLE成本：1字节（方案标记）+ 分段信息 + 数据位
        int headerCost = 8; // 1字节方案标记
        int segmentHeaderCost = 16 + segResult.numSegments * 24; // 4字节段数 + 每段3字节

        return headerCost + segmentHeaderCost + (int)segResult.totalCost;
    }

    // 新增：baseline编码方案
    private static byte[] encodeWithBaseline(int[] data, int packSize) {
        List<Byte> result = new ArrayList<>();

        // 方案标记
        result.add(SCHEME_BASELINE);

        // 计算最大位宽
        int maxVal = 0;
        for (int value : data) {
            if (value > maxVal) {
                maxVal = value;
            }
        }
        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxVal);

        // 写入位宽
        result.add((byte) bitWidth);

        // 进行bit-packing
        int totalBits = bitWidth * data.length;
        int totalBytes = (totalBits + 7) / 8;
        byte[] packedData = new byte[totalBytes];

        int currentBytePos = 0;
        int currentBitPos = 0;

        for (int value : data) {
            for (int bit = bitWidth - 1; bit >= 0; bit--) {
                int currentBit = (value >> bit) & 1;
                packedData[currentBytePos] |= (currentBit << (7 - currentBitPos));
                currentBitPos++;
                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }
        }

        // 添加打包数据
        for (byte b : packedData) {
            result.add(b);
        }

        // 转换为byte数组
        byte[] finalResult = new byte[result.size()];
        for (int i = 0; i < result.size(); i++) {
            finalResult[i] = result.get(i);
        }

        return finalResult;
    }

    // 新增：智能解码方案
    private static int[] decodeWithSmartSelection(byte[] encodedData, int originalLength, int packSize) {
        if (encodedData.length == 0) {
            return new int[originalLength];
        }

        // 读取方案标记
        byte scheme = encodedData[0];

        if (scheme == SCHEME_BASELINE) {
            return decodeBaseline(encodedData, originalLength);
        } else {
            return decodeImprovedRLE(encodedData, originalLength, packSize);
        }
    }

    // 新增：baseline解码方案
    private static int[] decodeBaseline(byte[] encodedData, int originalLength) {
        try {
            if (encodedData.length < 2) {
                return new int[originalLength];
            }

            int pos = 1; // 跳过方案标记
            int bitWidth = encodedData[pos++] & 0xFF;

            int[] result = new int[originalLength];
            int resultIndex = 0;
            int currentBitPos = 0;

            for (int i = 0; i < originalLength && pos < encodedData.length; i++) {
                int value = 0;

                for (int bit = 0; bit < bitWidth; bit++) {
                    if (pos >= encodedData.length) {
                        value = (value << 1);
                    } else {
                        int currentBit = (encodedData[pos] >> (7 - currentBitPos)) & 1;
                        value = (value << 1) | currentBit;
                    }
                    currentBitPos++;

                    if (currentBitPos == 8) {
                        pos++;
                        currentBitPos = 0;
                    }
                }

                if (resultIndex < originalLength) {
                    result[resultIndex++] = value;
                }
            }

            return result;
        } catch (Exception e) {
            System.err.println("baseline解码异常: " + e.getMessage());
            return new int[originalLength];
        }
    }

    // 新增：获取分段信息
    private static SegmentationResult getSegmentationInfo(int[] data, int packSize, String strategy, double threshold) {
        int groupCount = data.length / packSize;
        int[] bitWidths = new int[groupCount];

        for (int i = 0; i < groupCount; i++) {
            int maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                if (data[startIdx + j] > maxInGroup) {
                    maxInGroup = data[startIdx + j];
                }
            }
            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        List<Segment> segments;
        switch (strategy) {
            case "DP_OPTIMAL":
                segments = computeOptimalSegmentationDP(bitWidths, packSize, threshold);
                break;
            case "ADAPTIVE":
                segments = computeAdaptiveSegmentation(bitWidths, packSize, threshold);
                break;
            case "TWO_LEVEL":
                segments = computeTwoLevelSegmentation(bitWidths, packSize, threshold);
                break;
            case "FAST_GREEDY":
                segments = computeFastGreedySegmentation(bitWidths, packSize, threshold);
                break;
            case "HYBRID":
                segments = computeHybridSegmentation(bitWidths, packSize, threshold);
                break;
            default:
                segments = computeOptimalSegmentation(bitWidths, packSize);
        }

        return new SegmentationResult(segments, data.length * 32); // 原始大小为每个值32位
    }

    // 改进的编码方法（现在只用于RLE方案）
    public static byte[] encodeWithImprovedRLE(int[] data, int packSize, String strategy, double threshold) {
        List<Byte> result = new ArrayList<>();

        // 添加方案标记
        result.add(SCHEME_RLE);

        // 处理数据，确保长度是packSize的倍数
        int actual_length = data.length;
        int remainder = actual_length % packSize;
        if (remainder != 0) {
            int paddingLength = packSize - remainder;
            int[] paddedArray = new int[actual_length + paddingLength];
            System.arraycopy(data, 0, paddedArray, 0, actual_length);
            data = paddedArray;
            actual_length = paddedArray.length;
        }

        int groupCount = actual_length / packSize;
        int[] bitWidths = new int[groupCount];

        // 计算每个组的bitwidth
        for (int i = 0; i < groupCount; i++) {
            int maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                if (data[startIdx + j] > maxInGroup) {
                    maxInGroup = data[startIdx + j];
                }
            }
            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        // 根据策略选择分段算法
        List<Segment> segments;
        switch (strategy) {
            case "DP_OPTIMAL":
                segments = computeOptimalSegmentationDP(bitWidths, packSize, threshold);
                break;
            case "ADAPTIVE":
                segments = computeAdaptiveSegmentation(bitWidths, packSize, threshold);
                break;
            case "TWO_LEVEL":
                segments = computeTwoLevelSegmentation(bitWidths, packSize, threshold);
                break;
            case "FAST_GREEDY":
                segments = computeFastGreedySegmentation(bitWidths, packSize, threshold);
                break;
            case "HYBRID":
                segments = computeHybridSegmentation(bitWidths, packSize, threshold);
                break;
            default:
                segments = computeOptimalSegmentation(bitWidths, packSize);
        }

        // 对segments进行编码
        List<Byte> rleEncoded = encodeSegments(segments);
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
                    if (startIndex + j < data.length) {
                        groupData.add(data[startIndex + j]);
                    } else {
                        groupData.add(0);
                    }
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

    // 快速动态规划分段（降低复杂度）
    private static List<Segment> computeOptimalSegmentationDP(int[] bitWidths, int packSize, double threshold) {
        int n = bitWidths.length;
        if (n == 0) return new ArrayList<>();

        // 限制搜索深度以提高性能
        int maxLookAhead = Math.min(20, n);

        // DP数组：dp[i]表示前i个组的最小成本
        double[] dp = new double[n + 1];
        int[] prev = new int[n + 1];
        Arrays.fill(dp, Double.MAX_VALUE);
        dp[0] = 0;

        for (int i = 1; i <= n; i++) {
            int maxWidth = bitWidths[i-1];

            // 限制搜索范围
            int start = Math.max(0, i - maxLookAhead);
            for (int j = start; j < i; j++) {
                // 更新最大位宽
                if (j < i-1) {
                    maxWidth = Math.max(maxWidth, bitWidths[j]);
                }

                int length = i - j;
                double segmentCost = 24 + maxWidth * length * packSize;
                double totalCost = dp[j] + segmentCost;

                if (totalCost < dp[i]) {
                    dp[i] = totalCost;
                    prev[i] = j;
                }
            }
        }

        // 回溯构建分段
        List<Segment> segments = new ArrayList<>();
        int i = n;
        while (i > 0) {
            int j = prev[i];
            int length = i - j;
            int maxWidth = 0;
            for (int k = j; k < i; k++) {
                maxWidth = Math.max(maxWidth, bitWidths[k]);
            }
            segments.add(0, new Segment(maxWidth, length, packSize));
            i = j;
        }

        return segments;
    }

    // 新增：快速贪心分段算法
    private static List<Segment> computeFastGreedySegmentation(int[] bitWidths, int packSize, double threshold) {
        List<Segment> segments = new ArrayList<>();
        if (bitWidths.length == 0) return segments;

        int n = bitWidths.length;
        int start = 0;
        int currentMax = bitWidths[0];
        double currentCost = 24 + currentMax * packSize; // 第一段的成本

        for (int i = 1; i < n; i++) {
            int potentialMax = Math.max(currentMax, bitWidths[i]);
            int length = i - start + 1;

            // 计算两种情况的成本
            double mergedCost = 24 + potentialMax * length * packSize;
            double splitCost = currentCost + 24 + bitWidths[i] * packSize;

            // 如果合并的成本更高，结束当前段
            if (mergedCost > splitCost * (1 - threshold) || length > 32) { // 限制段的最大长度
                segments.add(new Segment(currentMax, i - start, packSize));
                start = i;
                currentMax = bitWidths[i];
                currentCost = 24 + currentMax * packSize;
            } else {
                currentMax = potentialMax;
                currentCost = mergedCost;
            }
        }

        // 添加最后一段
        segments.add(new Segment(currentMax, n - start, packSize));

        // 后处理：尝试合并小段
        return mergeSmallSegments(segments, packSize, threshold);
    }

    // 新增：混合分段策略
    private static List<Segment> computeHybridSegmentation(int[] bitWidths, int packSize, double threshold) {
        // 分析数据特征
        DataFeatures features = analyzeBitWidthFeatures(bitWidths);

        // 根据数据特征选择策略
        if (bitWidths.length <= 32) {
            // 数据量小，使用快速贪心
            return computeFastGreedySegmentation(bitWidths, packSize, threshold);
        } else if (features.variance < 2.0 && features.bitWidthRange < 8) {
            // 数据变化小，使用简单分段
            return computeSimpleSegmentation(bitWidths, packSize);
        } else if (features.entropy < 1.5) {
            // 熵低，使用两级分段
            return computeTwoLevelSegmentation(bitWidths, packSize, threshold);
        } else {
            // 复杂情况，使用自适应分段
            return computeAdaptiveSegmentation(bitWidths, packSize, threshold);
        }
    }

    // 新增：简单分段（用于均匀数据）
    private static List<Segment> computeSimpleSegmentation(int[] bitWidths, int packSize) {
        List<Segment> segments = new ArrayList<>();
        if (bitWidths.length == 0) return segments;

        int currentBitWidth = bitWidths[0];
        int count = 1;

        for (int i = 1; i < bitWidths.length; i++) {
            if (bitWidths[i] == currentBitWidth) {
                count++;
                // 限制最大段长度
                if (count >= 256) {
                    segments.add(new Segment(currentBitWidth, count, packSize));
                    count = 0;
                }
            } else {
                if (count > 0) {
                    segments.add(new Segment(currentBitWidth, count, packSize));
                }
                currentBitWidth = bitWidths[i];
                count = 1;
            }
        }

        if (count > 0) {
            segments.add(new Segment(currentBitWidth, count, packSize));
        }

        return segments;
    }

    // 自适应分段策略（优化版）
    private static List<Segment> computeAdaptiveSegmentation(int[] bitWidths, int packSize, double threshold) {
        // 分析数据特征
        DataFeatures features = analyzeBitWidthFeatures(bitWidths);

        // 根据压缩评分调整阈值
        double adjustedThreshold = threshold;
        if (features.compressionScore > 0.7) {
            adjustedThreshold *= 1.2; // 高压缩潜力，更积极合并
        } else if (features.compressionScore < 0.3) {
            adjustedThreshold *= 0.8; // 低压缩潜力，更保守合并
        }

        // 使用快速贪心算法
        List<Segment> segments = computeFastGreedySegmentation(bitWidths, packSize, adjustedThreshold);

        // 如果段太多，尝试进一步合并
        if (segments.size() > bitWidths.length * 0.3) {
            segments = mergeSmallSegments(segments, packSize, adjustedThreshold * 1.5);
        }

        return segments;
    }

    // 新增：合并小段
    private static List<Segment> mergeSmallSegments(List<Segment> segments, int packSize, double threshold) {
        if (segments.size() <= 1) return segments;

        List<Segment> mergedSegments = new ArrayList<>();
        Segment current = segments.get(0);

        for (int i = 1; i < segments.size(); i++) {
            Segment next = segments.get(i);

            // 检查是否可以合并
            if (current.canMergeWith(next, threshold) ||
                    (current.length < 4 && next.length < 4)) { // 小段强制合并
                current = current.merge(next);
            } else {
                mergedSegments.add(current);
                current = next;
            }
        }

        mergedSegments.add(current);
        return mergedSegments;
    }

    // 两级分段策略（优化版）
    private static List<Segment> computeTwoLevelSegmentation(int[] bitWidths, int packSize, double threshold) {
        // 第一级：使用滑动窗口检测大的变化
        List<Integer> breakPoints = new ArrayList<>();
        int windowSize = Math.min(8, bitWidths.length / 4);

        for (int i = windowSize; i < bitWidths.length; i++) {
            // 计算窗口内的方差
            double windowVariance = computeWindowVariance(bitWidths, i - windowSize, windowSize);
            if (windowVariance > 5.0) { // 变化大，作为分割点
                breakPoints.add(i);
                i += windowSize; // 跳过一段
            }
        }

        // 第二级：在每个块内使用快速贪心
        List<Segment> allSegments = new ArrayList<>();
        int start = 0;

        for (int breakPoint : breakPoints) {
            if (breakPoint > start) {
                int[] subBitWidths = Arrays.copyOfRange(bitWidths, start, breakPoint);
                List<Segment> subSegments = computeFastGreedySegmentation(subBitWidths, packSize, threshold);
                allSegments.addAll(subSegments);
                start = breakPoint;
            }
        }

        // 处理最后一段
        if (start < bitWidths.length) {
            int[] subBitWidths = Arrays.copyOfRange(bitWidths, start, bitWidths.length);
            List<Segment> subSegments = computeFastGreedySegmentation(subBitWidths, packSize, threshold);
            allSegments.addAll(subSegments);
        }

        return allSegments;
    }

    // 新增：计算窗口方差
    private static double computeWindowVariance(int[] data, int start, int length) {
        if (length <= 1) return 0;

        double sum = 0;
        for (int i = 0; i < length && start + i < data.length; i++) {
            sum += data[start + i];
        }
        double mean = sum / length;

        double variance = 0;
        for (int i = 0; i < length && start + i < data.length; i++) {
            variance += Math.pow(data[start + i] - mean, 2);
        }

        return variance / length;
    }

    // 分析位宽特征
    private static DataFeatures analyzeBitWidthFeatures(int[] bitWidths) {
        if (bitWidths.length == 0) {
            return new DataFeatures(0, 0, 0);
        }

        // 计算统计特征
        double sum = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        for (int bitWidth : bitWidths) {
            sum += bitWidth;
            min = Math.min(min, bitWidth);
            max = Math.max(max, bitWidth);
        }

        double mean = sum / bitWidths.length;

        // 计算方差
        double variance = 0;
        for (int bitWidth : bitWidths) {
            variance += Math.pow(bitWidth - mean, 2);
        }
        variance /= bitWidths.length;

        // 计算熵（简化版，只考虑0-8位的情况）
        int[] freq = new int[9]; // 0-8位
        for (int bitWidth : bitWidths) {
            if (bitWidth < 9) {
                freq[bitWidth]++;
            }
        }

        double entropy = 0;
        for (int f : freq) {
            if (f > 0) {
                double p = (double) f / bitWidths.length;
                entropy -= p * Math.log(p) / Math.log(2);
            }
        }

        int range = max - min;

        return new DataFeatures(variance, range, entropy);
    }

    // 原始的最优分段算法（保持不变）
    private static List<Segment> computeOptimalSegmentation(int[] bitWidths, int packSize) {
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

        int l = segments.size();
        int sMax = segments.stream().mapToInt(s -> s.length).max().orElse(0);
        int z = (int) Math.ceil(Math.log(sMax + 1) / Math.log(2));

        boolean changed = true;
        while (changed) {
            changed = false;
            for (int i = 0; i < segments.size() - 1; i++) {
                Segment current = segments.get(i);
                Segment next = segments.get(i + 1);

                if (current.bitWidth == next.bitWidth) {
                    int mergedLength = current.length + next.length;
                    boolean condition1 = mergedLength <= (1 << z) - 1;
                    boolean condition2 = l <= z + 6;

                    if (condition1 || condition2) {
                        current.length = mergedLength;
                        segments.remove(i + 1);
                        l--;
                        sMax = Math.max(sMax, mergedLength);
                        z = (int) Math.ceil(Math.log(sMax + 1) / Math.log(2));
                        changed = true;
                        i--;
                    }
                }
            }
        }

        return segments;
    }

    // 解码改进的RLE方案（现在需要跳过方案标记）
    public static int[] decodeImprovedRLE(byte[] encodedData, int originalLength, int packSize) {
        try {
            // 跳过方案标记（第一个字节）
            int startPos = 1;

            // 解码段列表
            List<Segment> segments = decodeSegments(encodedData, startPos);
            if (segments.isEmpty()) {
                return new int[originalLength];
            }

            int pos = startPos + 2 + segments.size() * 3;
            int[] result = new int[originalLength];
            int resultIndex = 0;
            int currentBitPos = 0;

            for (Segment segment : segments) {
                if (resultIndex >= originalLength) break;

                int totalValues = segment.length * packSize;
                int valuesToRead = Math.min(totalValues, originalLength - resultIndex);

                for (int i = 0; i < valuesToRead && resultIndex < originalLength; i++) {
                    int value = 0;

                    for (int bit = 0; bit < segment.bitWidth; bit++) {
                        if (pos >= encodedData.length) {
                            value = (value << 1);
                        } else {
                            int currentBit = (encodedData[pos] >> (7 - currentBitPos)) & 1;
                            value = (value << 1) | currentBit;
                        }
                        currentBitPos++;

                        if (currentBitPos == 8) {
                            pos++;
                            currentBitPos = 0;
                        }
                    }

                    if (resultIndex < originalLength) {
                        result[resultIndex++] = value;
                    }
                }

                int remainingValues = totalValues - valuesToRead;
                if (remainingValues > 0) {
                    int bitsToSkip = remainingValues * segment.bitWidth;
                    for (int bit = 0; bit < bitsToSkip; bit++) {
                        currentBitPos++;
                        if (currentBitPos == 8) {
                            pos++;
                            currentBitPos = 0;
                        }
                    }
                }
            }

            return result;
        } catch (Exception e) {
            System.err.println("解码异常: " + e.getMessage());
            return new int[originalLength];
        }
    }

    // 以下是原有的辅助方法，保持不变
    private static List<Byte> encodeSegments(List<Segment> segments) {
        List<Byte> result = new ArrayList<>();
        if (segments.isEmpty()) return result;

        int segmentCount = segments.size();
//        result.add((byte) (segmentCount >> 24));
//        result.add((byte) (segmentCount >> 16));
        result.add((byte) (segmentCount >> 8));
        result.add((byte) segmentCount);

        for (Segment segment : segments) {
            result.add((byte) segment.bitWidth);
            result.add((byte) (segment.length >> 8));
            result.add((byte) segment.length);
        }

        return result;
    }

    private static List<Segment> decodeSegments(byte[] data, int startPos) {
        List<Segment> segments = new ArrayList<>();
        if (startPos + 2 > data.length) return segments;

        int segmentCount = ((data[startPos] & 0xFF) << 8) | (data[startPos + 1] & 0xFF);
        if (segmentCount <= 0) return segments;

        int pos = startPos + 2;
        for (int i = 0; i < segmentCount && pos + 2 < data.length; i++) {
            int bitWidth = data[pos++] & 0xFF;
            int length = ((data[pos++] & 0xFF) << 8) | (data[pos++] & 0xFF);
            segments.add(new Segment(bitWidth, length, 0)); // packSize在解码时不重要
        }

        return segments;
    }

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
        return currentBytePos;
    }

    public static ArrayList<Integer> unpackValues(
            byte[] encoded, int decode_pos, int bit_width, int num_values) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int currentBytePos = decode_pos;
        int currentBitPos = 0;

        for (int i = 0; i < num_values; i++) {
            int value = 0;
            for (int bit = 0; bit < bit_width; bit++) {
                int currentBit = (encoded[currentBytePos] >> (7 - currentBitPos)) & 1;
                value = (value << 1) | currentBit;
                currentBitPos++;
                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }
            result_list.add(value);
        }
        return result_list;
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

    private static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
        int size = numbers.size();
        int[] result = new int[size];

        if (size == 0) return result;

        BigDecimal min = null;
        BigDecimal[] scaledValues = new BigDecimal[size];

        for (int i = 0; i < size; i++) {
            BigDecimal val = new BigDecimal(numbers.get(i)).multiply(scale);
            scaledValues[i] = val;
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
        }

        BigDecimal first = scaledValues[0].subtract(min);
        result[0] = first.toBigInteger().intValue();

        for (int i = 1; i < size; i++) {
            BigDecimal current = scaledValues[i].subtract(min);
            result[i] = current.toBigInteger().intValue();
        }

        return result;
    }
}