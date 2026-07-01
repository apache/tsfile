package org.apache.iotdb.tsfile.encoding;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import org.openjdk.jol.info.ClassLayout;
//import org.openjdk.jol.info.GraphLayout;

public class EfficientOctadPackingSprintz {

    private static final int CHUNK_SIZE = 1000;
    // 轻量级Octad表示
    static class Octad {
        final int bitWidth;

        Octad(int bitWidth) {
            this.bitWidth = bitWidth;
        }
    }

    // 紧凑的Pack表示
    static class Pack {
        int size = 0;
        int maxBitWidth = 0;
        int startIndex;

        void addOctad(int index, int bitWidth) {
            if (size == 0) {
                startIndex = index;
                maxBitWidth = bitWidth;
            } else {
                if (bitWidth > maxBitWidth) maxBitWidth = bitWidth;
            }
            size++;
        }

        int dataCost() {
            return 8 * size * maxBitWidth;
        }

        int logSize() {
            return 32 - Integer.numberOfLeadingZeros(size); // 快速计算 ⌈log2(size+1)⌉
        }
    }

    // 打包结果
    static class PackingResult {
        int packCount = 0;
        int dataCostA = 0;
        int bitWidthCostB = 0;
        int packSizeCostC = 0;
        int totalCost = 0;

        void calculateCost(int maxLog) {
            bitWidthCostB = 5 * packCount;
            packSizeCostC = packCount * maxLog;
            totalCost = dataCostA + bitWidthCostB + packSizeCostC;
        }

        @Override
        public String toString() {
            return String.format("Packs: %d, Cost: %d (A=%d, B=%d, C=%d)",
                    packCount, totalCost, dataCostA, bitWidthCostB, packSizeCostC);
        }
    }

    // 高效的强化学习决策模型
    static class RLDecisionModel {
        // 使用数组代替对象，提高性能
        private final float[] weights = new float[5]; // 5个特征权重
        private float explorationRate = 0.3f;
        private final float learningRate = 0.01f;

        public RLDecisionModel() {
            // 初始化随机权重
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < weights.length; i++) {
                weights[i] = random.nextFloat() * 2 - 1; // [-1, 1]
            }
        }

        // 决策函数 - 避免对象创建
        public boolean shouldMerge(
                int currentPackSize,
                int currentPackMaxB,
                int newOctadB,
                int packCount,
                int currentMaxLog
        ) {
            // 探索
            if (ThreadLocalRandom.current().nextFloat() < explorationRate) {
                return ThreadLocalRandom.current().nextBoolean();
            }

            // 利用：计算特征向量点积
            float score = 0;
            score += weights[0] * currentPackSize / 100.0f;       // 归一化
            score += weights[1] * currentPackMaxB / 32.0f;         // 归一化
            score += weights[2] * newOctadB / 32.0f;               // 归一化
            score += weights[3] * packCount / 100.0f;              // 归一化
            score += weights[4] * currentMaxLog / 10.0f;           // 归一化

            // Sigmoid激活函数
            float probability = 1.0f / (1.0f + (float)Math.exp(-score));
            return probability > 0.5f;
        }

        // 高效训练方法 - 使用REINFORCE算法
        public void train(List<DecisionPoint> decisions, float reward) {
            // 减少探索率
            explorationRate *= 0.99f;
            if (explorationRate < 0.05f) explorationRate = 0.05f;

            // 更新权重
            for (DecisionPoint dp : decisions) {
                float gradient = dp.action ? (1 - dp.probability) : -dp.probability;
                gradient *= reward * learningRate;

                // 更新权重
                weights[0] += gradient * dp.currentPackSize / 100.0f;
                weights[1] += gradient * dp.currentPackMaxB / 32.0f;
                weights[2] += gradient * dp.newOctadB / 32.0f;
                weights[3] += gradient * dp.packCount / 100.0f;
                weights[4] += gradient * dp.currentMaxLog / 10.0f;
            }
        }
    }

    // 决策点记录（轻量级）
    static class DecisionPoint {
        final int currentPackSize;
        final int currentPackMaxB;
        final int newOctadB;
        final int packCount;
        final int currentMaxLog;
        final boolean action;
        final float probability;

        public DecisionPoint(int currentPackSize, int currentPackMaxB,
                             int newOctadB, int packCount,
                             int currentMaxLog, boolean action,
                             float probability) {
            this.currentPackSize = currentPackSize;
            this.currentPackMaxB = currentPackMaxB;
            this.newOctadB = newOctadB;
            this.packCount = packCount;
            this.currentMaxLog = currentMaxLog;
            this.action = action;
            this.probability = probability;
        }
    }

    // 高性能打包算法
    public static PackingResult packOctads(int[] bitWidths, RLDecisionModel model,
                                           List<DecisionPoint> decisionTrace) {
        PackingResult result = new PackingResult();
        Pack currentPack = new Pack();
        int globalMaxLog = 0;
        int packCount = 0;

        for (int i = 0; i < bitWidths.length; i++) {
            int b = bitWidths[i];

            if (currentPack.size == 0) {
                // 第一个octad
                currentPack.addOctad(i, b);
            } else if (b == currentPack.maxBitWidth) {
                // 位宽相同必须合并
                currentPack.addOctad(i, b);
            } else {
                // 位宽不同，使用模型决策
                boolean shouldMerge = false;
                float probability = 0.5f;

                // 计算模型概率
                float score = 0;
                score += model.weights[0] * currentPack.size / 100.0f;
                score += model.weights[1] * currentPack.maxBitWidth / 32.0f;
                score += model.weights[2] * b / 32.0f;
                score += model.weights[3] * packCount / 100.0f;
                score += model.weights[4] * globalMaxLog / 10.0f;
                probability = 1.0f / (1.0f + (float)Math.exp(-score));

                // 决策
                if (ThreadLocalRandom.current().nextFloat() < model.explorationRate) {
                    // 探索：随机决策
                    shouldMerge = ThreadLocalRandom.current().nextBoolean();
                } else {
                    // 利用：基于概率
                    shouldMerge = probability > 0.5f;
                }

                // 记录决策点用于训练
                if (decisionTrace != null) {
                    decisionTrace.add(new DecisionPoint(
                            currentPack.size, currentPack.maxBitWidth,
                            b, packCount, globalMaxLog,
                            shouldMerge, probability
                    ));
                }

                if (shouldMerge) {
                    // 合并到当前pack
                    currentPack.addOctad(i, b);
                } else {
                    // 完成当前pack
                    result.dataCostA += currentPack.dataCost();
                    int logSize = currentPack.logSize();
                    if (logSize > globalMaxLog) globalMaxLog = logSize;
                    packCount++;

                    // 开始新pack
                    currentPack = new Pack();
                    currentPack.addOctad(i, b);
                }
            }
        }

        // 完成最后一个pack
        if (currentPack.size > 0) {
            result.dataCostA += currentPack.dataCost();
            int logSize = currentPack.logSize();
            if (logSize > globalMaxLog) globalMaxLog = logSize;
            packCount++;
        }

        result.packCount = packCount;
        result.calculateCost(globalMaxLog);
        return result;
    }

    // 快速生成位宽数据
    public static int[] generateBitWidths(int count) {
        int[] bitWidths = new int[count];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < count; i++) {
            bitWidths[i] = random.nextInt(1, 33); // 1-32位
        }
        return bitWidths;
    }

    public static List<int[]> loadDataFromCSV(String filename) {
        List<int[]> sequences = new ArrayList<>();
        Pattern pattern = Pattern.compile("\"\\[([0-9, ]+)\\]\"");

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                // 跳过标题行
                if (firstLine) {
                    firstLine = false;
                    continue;
                }

                // 分割CSV行
                String[] parts = line.split(",");
                if (parts.length < 2) continue;

                // 提取new_input列
                String inputCol = parts[1].trim();
                Matcher matcher = pattern.matcher(inputCol);

                if (matcher.find()) {
                    String data = matcher.group(1);
                    String[] values = data.split(",");
                    int[] bitWidths = new int[values.length];

                    for (int i = 0; i < values.length; i++) {
                        bitWidths[i] = Integer.parseInt(values[i].trim());
                    }
                    sequences.add(bitWidths);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        }

        return sequences;
    }

    // 修改后的高性能训练循环
    public static RLDecisionModel trainModel(int epochs, String csvFilePath) {
        System.out.println("Training RL model from CSV data...");
        RLDecisionModel model = new RLDecisionModel();

        // 从CSV加载数据
        List<int[]> sequences = loadDataFromCSV(csvFilePath);
        if (sequences.isEmpty()) {
            System.out.println("No data loaded from CSV. Exiting.");
            return model;
        }

        System.out.println("Loaded " + sequences.size() + " sequences from CSV");
        List<DecisionPoint> decisionTrace = new ArrayList<>();

        for (int epoch = 1; epoch <= epochs; epoch++) {
            long startTime = System.nanoTime();
            float totalReward = 0;
            int processedSequences = 0;

            for (int[] bitWidths : sequences) {
                // 清除决策轨迹
                decisionTrace.clear();

                // 打包并获取结果
                PackingResult result = packOctads(bitWidths, model, decisionTrace);

                // 计算奖励（负成本）
                float reward = -result.totalCost / 10000.0f; // 归一化
                totalReward += reward;

                // 使用决策轨迹训练模型
                model.train(decisionTrace, reward);
                processedSequences++;
            }

            long duration = (System.nanoTime() - startTime) / 1_000_000;
            System.out.printf("Epoch %d: Avg Reward = %.2f, Time = %d ms%n",
                    epoch, totalReward / processedSequences, duration);
        }

        return model;
    }
    private static long measureMemory() {
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // 建议垃圾回收（不保证执行）
        return runtime.totalMemory() - runtime.freeMemory();
    }
    public static void main(String[] args) throws IOException {
        // 从CSV文件训练模型
        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
//        long startTime = System.nanoTime();
        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
//        long duration = System.nanoTime() - startTime;
//        System.out.println(duration);
//        int estimatedSize = 12 // 对象头
//                + 4   // float[] weights 引用 (压缩指针)
//                + 4   // float explorationRate
//                + 40; // weights数组自身 (12头 + 4长度 + 5*4数据)
//        System.out.println("Estimated model size: " + estimatedSize + " bytes");
//
//        // 方法2: 运行时内存差测量
//        int instanceCount = 100_000;
//        long memBefore = measureMemory();
//        RLDecisionModel[] models = new RLDecisionModel[instanceCount];
//        for (int i = 0; i < instanceCount; i++) {
//            models[i] = new RLDecisionModel();
//        }
//        long memAfter = measureMemory();
//        long avgSize = Math.round((memAfter - memBefore) / (double) instanceCount);
//        System.out.println("Measured average model size: " + avgSize + " bytes");

        // 性能测试
        performanceTest(trainedModel); //, 10000, 100

//        // 简单演示
//        int[] demoBitWidths = {5, 5, 8, 8, 8, 3, 3, 3, 10, 10};
//        PackingResult demoResult = packOctads(demoBitWidths, trainedModel, null);
//        System.out.println("\nDemo result: " + demoResult);
    }
    // 性能测试
//    public static void performanceTest(RLDecisionModel model, int octadCount, int iterations) {
//        System.out.println("\nPerformance Testing...");
//        long totalTime = 0;
//        int totalCost = 0;
//        int[] bitWidths = generateBitWidths(octadCount);
//
//        for (int i = 0; i < iterations; i++) {
//            long startTime = System.nanoTime();
//            PackingResult result = packOctads(bitWidths, model, null); // 禁用决策跟踪
//            long duration = System.nanoTime() - startTime;
//
//            totalTime += duration;
//            totalCost += result.totalCost;
//
//            if (i == 0) {
//                System.out.println("First run result: " + result);
//            }
//        }
//
//        double avgTime = totalTime / 1_000_000.0 / iterations;
//        double avgCost = totalCost / (double)iterations;
//
//        System.out.printf("Average packing time: %.4f ms%n", avgTime);
//        System.out.printf("Average cost: %.2f%n", avgCost);
//        System.out.printf("Throughput: %.2f octads/ms%n", octadCount / avgTime);
//    }
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

//    private static int[] scaleNumbers(List<String> numbers, int decimalMax) {
//        // 1. 预先计算缩放因子
//        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
//        int size = numbers.size();
//        int[] result = new int[size];
//
//        if (size == 0) {
//            return result;
//        }
//
//        // 2. 单次遍历完成所有转换和最小值查找
//        BigDecimal min = null;
//        BigDecimal[] scaledValues = new BigDecimal[size];
//
//        for (int i = 0; i < size; i++) {
//            BigDecimal val = new BigDecimal(numbers.get(i)).multiply(scale);
//            scaledValues[i] = val;
//            if (min == null || val.compareTo(min) < 0) {
//                min = val;
//            }
//        }
//
//        // 3. 处理第一个元素
//        BigDecimal first = scaledValues[0].subtract(min);
//        result[0] = first.toBigInteger().intValue();
//
//        // 4. 处理后续元素（差分+ZigZag）
//        BigDecimal prev = first;
//        for (int i = 1; i < size; i++) {
//            BigDecimal current = scaledValues[i].subtract(min);
//            int diff = current.subtract(prev).intValue(); // 直接使用intValue()更高效
//            result[i] = (diff << 1) ^ (diff >> 31); // 内联ZigZag编码
//            prev = current;
//        }
//
//        return result;
//    }

    public static void performanceTest(RLDecisionModel model) throws IOException {
        // 示例数据（实际应替换为真实时间序列）

        System.out.println("\nPerformance Testing...");
//        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_rl";
        File outputDir = new File(outputDirstr);

//        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
//            if(!file.getName().equals("Stocks-DE.csv")) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
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
                    int[] scaledInts =sprintz(scalingInt);

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

                        // 3. 存储结果    public static void main(String[] args) throws IOException {
                        //        // 示例数据（实际应替换为真实时间序列）
                        //        System.out.println("\nPerformance Testing...");
                        ////        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
                        //        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
                        //        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPDP";
                        //        File outputDir = new File(outputDirstr);
                        //
                        ////        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
                        //        if (!outputDir.exists()) outputDir.mkdir();
                        //        File dir = new File(directory);
                        //        for (File file : Objects.requireNonNull(dir.listFiles())) {
                        //
                        //            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
                        ////            if(!file.getName().equals("Stocks-DE.csv")) continue;
                        //            System.out.println(file.getName());
                        //            String Output = outputDirstr+"/"+file.getName();
                        //            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);
                        //
                        //            String[] head = {
                        //                    "Input Direction",
                        //                    "Encoding Algorithm",
                        //                    "Encoding Time",
                        //                    "Points",
                        //                    "Compressed Size",
                        //                    "Compression Ratio"
                        //            };
                        //            writer.writeRecord(head); // write header to output file
                        //            System.out.println("Processing " + file.getName() + "...");
                        //            List<String> numbers = new ArrayList<>();
                        //            List<Integer> decimalPlaces = new ArrayList<>();
                        //            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
                        //            while (csvReader.readRecord()) {
                        //                for (String value : csvReader.getValues()) {
                        //                    String numStr = value.trim();
                        //                    if (!numStr.isEmpty()) {
                        //                        numbers.add(numStr);
                        //                        int decimal = 0, sigBits;
                        //                        if (numStr.contains(".")) {
                        //                            String[] parts = numStr.split("\\.");
                        //                            decimal = parts[1].length();
                        //                            sigBits = (int) ((parts[0].length() + decimal) * (Math.log(10) / Math.log(2)));
                        //                        } else {
                        //                            sigBits = (int) (numStr.length() * (Math.log(10) / Math.log(2)));
                        //                        }
                        //                        decimalPlaces.add(decimal);
                        //                    }
                        //                }
                        //            }
                        //            int time_of_repeat = 50;
                        ////            System.out.println(numbers.size());
                        //
                        //
                        //            // 方法：强化学习
                        ////            long modelStart =  System.nanoTime();
                        //            int modelCost = 0;
                        //            long modelTime = 0;
                        //            for(int j=0;j<time_of_repeat;j++){
                        //                int totalCost = 0;
                        //                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                        //
                        //                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        //                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        //                        continue;
                        //
                        //                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                        //                            .stream().max(Integer::compare).orElse(0);
                        //
                        //                    long startTime = System.nanoTime();
                        //                    int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
                        //
                        //                    int remainder = scaledInts.length % 8;
                        //                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;
                        //
                        //                    // 创建新数组，长度补齐为8的倍数
                        //                    int[] paddedArray = new int[scaledInts.length + paddingLength];
                        //                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        //                    int actual_length = paddedArray.length;
                        //                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果
                        //
                        //                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        //                        // 1. 找出当前8个元素中的最大值
                        //                        int maxInGroup = 0;
                        //                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                        //                            if (paddedArray[scaledInts_j] > maxInGroup) {
                        //                                maxInGroup = paddedArray[scaledInts_j];
                        //                            }
                        //                        }
                        //
                        //                        // 2. 计算该最大值的去头零位宽
                        //                        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
                        //
                        //                        // 3. 存储结果
                        //                        bitWidths[scaledInts_i / 8] = bitWidth;
                        //                    }
                        //                    int cur_cost = computeMinPackingCost(bitWidths);
                        //
                        ////                    PackingResult result = packOctads(bitWidths, model, null); // 禁用决策跟踪
                        //                    long duration = System.nanoTime() - startTime;
                        //                    modelTime += (duration);
                        //                    modelCost +=  cur_cost;
                        ////                    if(i==0)
                        ////                        for (int episode = 0; episode < 10; episode++) {
                        ////                            trainEpisode(scaledInts, episode);
                        ////                        }
                        ////                    List<Integer> optimalK = predictOptimalK(scaledInts);
                        ////                    System.out.println("Optimal k sequence: " + optimalK);
                        //                }
                        //
                        //            }
                        //            modelCost /=time_of_repeat;
                        //            modelTime = (modelTime)/time_of_repeat;
                        //            double model_ratio = (double) modelCost / (double) (numbers.size()*64);
                        //            double modelTime_throughput = (double)(numbers.size()*8000)/ (double) (modelTime);
                        //            String[] record = {
                        //                    file.toString(),
                        //                    "BP-DP",
                        //                    String.valueOf(modelTime_throughput),
                        //                    String.valueOf(numbers.size()),
                        //                    String.valueOf(modelCost),
                        //                    String.valueOf(model_ratio)
                        //            };
                        //            writer.writeRecord(record);
                        //            writer.close();
                        ////            break;
                        //        }
                        //
                        //    }
                        //
                        //    public static int computeMinPackingCost(int[] bitWidths) {
                        //        int N = bitWidths.length;
                        //        if (N == 0) {
                        //            return 0;
                        //        }
                        //
                        //        // Precompute the maximum bit width for all intervals [l, r] (0-based)
                        //        int[][] maxB = new int[N][N];
                        //        for (int l = 0; l < N; l++) {
                        //            maxB[l][l] = bitWidths[l];
                        //            for (int r = l + 1; r < N; r++) {
                        //                maxB[l][r] = Math.max(maxB[l][r - 1], bitWidths[r]);
                        //            }
                        //        }
                        //
                        //        // Precompute bit width needed to store pack size s: ceil(log2(s + 1))
                        //        int[] sizeBitWidth = new int[N + 1];
                        //        for (int s = 1; s <= N; s++) {
                        //            sizeBitWidth[s] = 32 - Integer.numberOfLeadingZeros(s);
                        //        }
                        //
                        //        // DP table: dp[i][p][s] = min cost for first i octads, p packs, max size bit width s
                        //        // Initialize with infinity
                        //        int[][][] dp = new int[N + 1][N + 1][11]; // s <= 10 since log2(1000) ~ 10
                        //        for (int i = 0; i <= N; i++) {
                        //            for (int p = 0; p <= N; p++) {
                        //                Arrays.fill(dp[i][p], Integer.MAX_VALUE / 2);
                        //            }
                        //        }
                        //        dp[0][0][0] = 0; // Base case
                        //
                        //        for (int i = 1; i <= N; i++) {
                        //            for (int k = 0; k < i; k++) {
                        //                int len = i - k;
                        //                int currentMaxB = maxB[k][i - 1];
                        //                int currentSizeBitWidth = sizeBitWidth[len];
                        //
                        //                for (int prevP = 0; prevP <= k; prevP++) {
                        //                    for (int prevS = 0; prevS <= 10; prevS++) {
                        //                        if (dp[k][prevP][prevS] == Integer.MAX_VALUE / 2) {
                        //                            continue;
                        //                        }
                        //
                        //                        int newP = prevP + 1;
                        //                        int newS = Math.max(prevS, currentSizeBitWidth);
                        //                        int aPart = 8 * len * currentMaxB;
                        //                        int bPart = 5;
                        //                        int cPartDelta = newP * newS - prevP * prevS;
                        //
                        //                        int totalCost = dp[k][prevP][prevS] + aPart + bPart + cPartDelta;
                        //                        if (totalCost < dp[i][newP][newS]) {
                        //                            dp[i][newP][newS] = totalCost;
                        //                        }
                        //                    }
                        //                }
                        //            }
                        //        }
                        //
                        //        // Find the minimal cost among all possible p and s for dp[N][p][s]
                        //        int minCost = Integer.MAX_VALUE;
                        //        for (int p = 1; p <= N; p++) {
                        //            for (int s = 1; s <= 10; s++) {
                        //                if (dp[N][p][s] < minCost) {
                        //                    minCost = dp[N][p][s];
                        //                }
                        //            }
                        //        }
                        //
                        //        return minCost;
                        //    }
                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }


                    PackingResult result = packOctads(bitWidths, model, null); // 禁用决策跟踪
                    long duration = System.nanoTime() - startTime;
                    modelTime += (duration);
                    modelCost +=  result.totalCost;
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
            double model_ratio = (double) modelCost / (double) (numbers.size()*64);
            double modelTime_throughput = (double)(numbers.size()*8000)/ (double) (modelTime);
            String[] record = {
                    file.toString(),
                    "Sprintz-Reinforce-learning",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();
//            break;
        }

    }
}