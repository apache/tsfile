package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.encoding.RandomForestPredictor;

public class ModelPredict{

    private static RandomForestPredictor modelPredictor;
    private static final int CHUNK_SIZE = 10000;

    public ModelPredict() throws Exception {
        // 初始化模型预测器（建议使用单例模式）
        modelPredictor = new RandomForestPredictor("/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/forest_params.json");
    }

    public static void main(String[] args) throws Exception {

        modelPredictor = new RandomForestPredictor("/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/forest_params.json");

        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        File outputDir = new File("/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output");
        if (!outputDir.exists()) outputDir.mkdir();


        // 初始化性能记录文件
        CsvWriter perfWriter = new CsvWriter(new FileWriter("/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output/performance_comparison.csv"), ',');
        perfWriter.writeRecord(new String[]{"Dataset", "Model_Time", "Model_Cost","Model_Ratio", "Optimized_Time", "Optimized_Cost", "Optimized_Ratio"});

//        // 初始化特征文件（如需）
//        CsvWriter featWriter = new CsvWriter(new FileWriter("/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output/features_for_train.csv"), ',');
//        featWriter.writeRecord(new String[]{"Dataset", "decimal_precision_max", "decimal_precision_avg", "range",
//                "significance_bits_max", "significance_bits_avg", "optimal_block_size", "block_cost"});

        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
//            if(!file.getName().equals("Stocks-DE.csv")) continue;

            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            List<Integer> significanceBits = new ArrayList<>();

            // 读取数据
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
                        significanceBits.add(sigBits);
                    }
                }
            }
            int time_of_repeat = 100;
//            System.out.println(numbers.size());


            // 方法1：模型预测（此处简化为固定值，需替换实际模型逻辑）
            long modelStart =  System.nanoTime();
            int modelCost = 0;
            for(int i=0;i<time_of_repeat;i++)
                modelCost = calculateModelCost(numbers, decimalPlaces, significanceBits);
            long modelTime = ( System.nanoTime() - modelStart)/time_of_repeat;
            double model_ratio = (double) modelCost / (double) (numbers.size()*64);
            double modelTime_throughput = (double)(numbers.size()*8000)/ (double) (modelTime);

            // 方法2：优化搜索
            long optimizedStart =  System.nanoTime();
            int optimizedCost = 0;
            for(int i=0;i<time_of_repeat;i++)
                 optimizedCost = calculateOptimizedCost(numbers, decimalPlaces);
            long optimizedTime = ( System.nanoTime() - optimizedStart)/time_of_repeat;
            double optimized_ratio = (double) optimizedCost / (double) (numbers.size()*64);
            double optimizedTime_throughput = (double)(numbers.size()*8000)/ (double) (optimizedTime);

            // 写入性能记录
            perfWriter.writeRecord(new String[]{
                    file.getName(),
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio),
                    String.valueOf(optimizedTime_throughput),
                    String.valueOf(optimizedCost),
                    String.valueOf(optimized_ratio)
            });
            perfWriter.flush();
//            break;
        }

        perfWriter.close();
//        featWriter.close();
    }

    private static int calculateModelCost(List<String> numbers, List<Integer> decimalPlaces,
                                          List<Integer> significanceBits) {
        int totalCost = 0;
        List<String> chunkNumbers = numbers.subList(0, Math.min(CHUNK_SIZE, numbers.size()));
        List<Integer> chunkDecimal = decimalPlaces.subList(0, Math.min( CHUNK_SIZE, numbers.size()));
        List<Integer> chunkSigBits = significanceBits.subList(0, Math.min( CHUNK_SIZE, numbers.size()));

        int decimalMax = Collections.max(chunkDecimal);
        double decimalAvg = chunkDecimal.stream().mapToInt(v -> v).average().orElse(0);
        int sigBitsMax = Collections.max(chunkSigBits);
        double sigBitsAvg = chunkSigBits.stream().mapToInt(v -> v).average().orElse(0);

        int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
        int chunkMin = Arrays.stream(scaledInts).min().orElse(0);
        int chunkMax = Arrays.stream(scaledInts).max().orElse(0);

        double[] features = {
                decimalMax,                // decimal_precision_max
                decimalAvg,                // decimal_precision_avg
                chunkMin,                  // chunk_min_int
                chunkMax,                  // chunk_max_int
                sigBitsMax,                // significance_bits_max
                sigBitsAvg                 // significance_bits_avg
        };


        // 模型预测
        int predictedBlockSize = modelPredictor.predict(features);
//        ModelPredict optimizer = new ModelPredict();
        for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
            chunkNumbers = numbers.subList(i, Math.min(i+CHUNK_SIZE, numbers.size()));
            decimalMax = Collections.max(chunkDecimal);
            scaledInts = scaleNumbers(chunkNumbers, decimalMax);

            scaledInts = processScaledInts(scaledInts);
            if(scaledInts.length==0) continue;
//            System.out.println(Arrays.toString(scaledInts));
//            int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
            totalCost += calculateCost(scaledInts, predictedBlockSize);
        }
//        System.out.println(totalCost);
        return totalCost;
    }

    public static int[] processScaledInts(int[] scaledInts) {
        if (scaledInts == null || scaledInts.length < 2) {
            return new int[0];
        }

        int[] diffs = new int[scaledInts.length - 1];
        for (int i = 0; i < diffs.length; i++) {
            diffs[i] = scaledInts[i + 1] - scaledInts[i];
        }

        for (int i = 0; i < diffs.length; i++) {
            // Zigzag编码：将正负数转换为全正数
            diffs[i] = (diffs[i] << 1) ^ (diffs[i] >> 31);
        }

        return diffs;
    }
    private static int calculateOptimizedCost(List<String> numbers, List<Integer> decimalPlaces) {
        int totalCost = 0;
        for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

            List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
            if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                continue;
            int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                    .stream().max(Integer::compare).orElse(0);
            int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
//            System.out.println(Arrays.toString(scaledInts));
            scaledInts = processScaledInts(scaledInts);
            int result = findOptimalBlockSize(scaledInts);
            totalCost += result;
//            System.out.println(result);
//            System.out.println(i);
//            break;
        }

        return totalCost;
    }

    private static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);

        // 将字符串转换为缩放后的BigDecimal数组
        BigDecimal[] decimals = numbers.stream()
                .map(BigDecimal::new)
                .map(d -> d.multiply(scale))
                .toArray(BigDecimal[]::new);

        // 计算最小值
        BigDecimal min = Arrays.stream(decimals)
                .min(BigDecimal::compareTo)
                .orElse(BigDecimal.ZERO);

        // 转换为int数组（包含去最小值、类型转换）
        int[] result = new int[decimals.length];
        for (int i = 0; i < decimals.length; i++) {
            BigDecimal shifted = decimals[i].subtract(min);
            result[i] = shifted.toBigInteger().intValue();
        }

        return result;
    }

    private static int computeLog(int x) {
        return x > 0 ? (int) Math.ceil(Math.log(x + 1) / Math.log(2)) : 0;
    }

    private static int calculateCost(int[] arr, int blockSize) {
        if (arr.length == 0 || blockSize <= 0) return Integer.MAX_VALUE;

        // 计算分块数量和每块的最大值
        int arrLength = arr.length;
        int numBlocks = (arrLength + blockSize - 1) / blockSize;
        int[][] blocks = new int[numBlocks][];
        int[] blockMaxs = new int[numBlocks];
        int globalMax = Integer.MIN_VALUE;

        // 分块并计算最大值
        for (int i = 0; i < numBlocks; i++) {
            int start = i * blockSize;
            int end = Math.min(start + blockSize, arrLength);
            int[] block = Arrays.copyOfRange(arr, start, end);
            blocks[i] = block;

            int currentMax = Integer.MIN_VALUE;
            for (int num : block) {
                if (num > currentMax) currentMax = num;
            }
            blockMaxs[i] = currentMax;
            if (currentMax > globalMax) globalMax = currentMax;
        }

        // 计算总成本
        int totalCost = numBlocks * computeLog(globalMax);
        for (int i = 0; i < numBlocks; i++) {
            totalCost += blocks[i].length * computeLog(blockMaxs[i]);
        }

        return totalCost;
    }

    private static int findOptimalBlockSize(int[] arr) {
        int maxBlockSize = Math.min(arr.length, 4096);
        int bestSize = 8;
        int minCost = Integer.MAX_VALUE;

        for (int k = 8; k <= maxBlockSize; k += 8) {
            int cost = calculateCost(arr, k);
            if (cost < minCost) {
                minCost = cost;
                bestSize = k;
            }
        }

        return minCost;
    }

}