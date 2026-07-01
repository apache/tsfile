package org.apache.iotdb.tsfile.encoding;
import com.csvreader.CsvReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Qlearning {

    static class State {
        int remaining;  // 离散化的剩余长度（0-7,8-15,...）
        int prevK;      // 前一个k值（8,16,24...）
        int xMaxLevel;  // 离散化的x_max层级

        public State(int remaining, int prevK, int xMaxLevel) {
            this.remaining = remaining;
            this.prevK = prevK;
            this.xMaxLevel = xMaxLevel;
        }

        @Override
        public boolean equals(Object o) {
            State s = (State)o;
            return remaining == s.remaining && prevK == s.prevK && xMaxLevel == s.xMaxLevel;
        }

        @Override
        public int hashCode() {
            return (remaining << 16) | (prevK << 8) | xMaxLevel;
        }
    }

    // 环境参数
    static final int[] POSSIBLE_K = {32, 64, 128, 256}; // 候选k值
    static final double ALPHA = 0.1;  // 学习率
    static final double GAMMA = 0.9;  // 折扣因子
    static final double EPS_START = 0.9;
    static final double EPS_END = 0.1;
    private static final int CHUNK_SIZE = 10000;
    static Map<State, Map<Integer, Double>> qTable = new HashMap<>();
    static Random rand = new Random();

    public static void main(String[] args) throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        File outputDir = new File("/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output");
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
//            if(!file.getName().equals("Stocks-DE.csv")) continue;

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
            int time_of_repeat = 1;
//            System.out.println(numbers.size());


            // 方法1：模型预测（此处简化为固定值，需替换实际模型逻辑）
            long modelStart =  System.nanoTime();
            int modelCost = 0;

            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        continue;
                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);
                    int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                    if(i==0)
                        for (int episode = 0; episode < 10; episode++) {
                            trainEpisode(scaledInts, episode);
                        }
                    List<Integer> optimalK = predictOptimalK(scaledInts);
                    System.out.println("Optimal k sequence: " + optimalK);
                }

            }

            long modelTime = ( System.nanoTime() - modelStart)/time_of_repeat;
            double model_ratio = (double) modelCost / (double) (numbers.size()*64);
            double modelTime_throughput = (double)(numbers.size()*8000)/ (double) (modelTime);
            break;
        }


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

    static void trainEpisode(int[] data, int episode) {
        int index = 0;
        int prevK = 0;
        double eps = EPS_START - (EPS_START - EPS_END) * episode / 1000;

        while (index < data.length) {
            // 获取当前状态特征
            int remaining = (data.length - index + 7) / 8; // 离散化为8的倍数
            int currentXMax = calculateXMax(data, index, Math.min(index + 8, data.length));
            int xMaxLevel = (int)Math.ceil(Math.log(currentXMax + 1));

            State state = new State(remaining, prevK, xMaxLevel);

            // ε-greedy选择动作
            int k = chooseAction(state, eps);

            // 执行动作并计算奖励
            int realK = Math.min(k, data.length - index);
            int nextIndex = index + realK;
            double reward = -calculateCost(realK, xMaxLevel);

            // 如果与前一k相同则增加奖励
            if (k == prevK) reward += 2.0;

            // 更新Q值
            updateQTable(state, k, reward, nextIndex >= data.length);

            // 状态转移
            prevK = k;
            index = nextIndex;
        }
    }

    static int chooseAction(State state, double eps) {
        if (rand.nextDouble() < eps) {
            // 随机探索：从有效k中选择
            return POSSIBLE_K[rand.nextInt(POSSIBLE_K.length)];
        } else {
            // 利用：选择Q值最大的动作
            return qTable.getOrDefault(state, new HashMap<>()).entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey).orElse(8);
        }
    }

    static void updateQTable(State state, int action, double reward, boolean terminal) {
        double oldQ = qTable.getOrDefault(state, new HashMap<>()).getOrDefault(action, 0.0);
        double maxNextQ = terminal ? 0 : qTable.values().stream()
                .flatMap(m -> m.values().stream())
                .max(Double::compare).orElse(0.0);

        double newQ = oldQ + ALPHA * (reward + GAMMA * maxNextQ - oldQ);
        qTable.computeIfAbsent(state, k -> new HashMap<>()).put(action, newQ);
    }

    static List<Integer> predictOptimalK(int[] data) {
        List<Integer> result = new ArrayList<>();
        int index = 0;
        int prevK = 0;

        while (index < data.length) {
            int remaining = (data.length - index + 7) / 8;
            int currentXMax = calculateXMax(data, index, Math.min(index + 8, data.length));
            int xMaxLevel = (int)Math.ceil(Math.log(currentXMax + 1));

            State state = new State(remaining, prevK, xMaxLevel);
            int k = qTable.getOrDefault(state, new HashMap<>()).entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey).orElse(8);

            result.add(k);
            prevK = k;
            index += k;
        }
        return result;
    }

    // 以下为辅助方法
    static int calculateXMax(int[] data, int start, int end) {
        int max = 0;
        for (int i = start; i < end; i++) {
            if (data[i] > max) max = data[i];
        }
        return max;
    }

    static double calculateCost(int k, int xMaxLevel) {
        return 8 + k * xMaxLevel;
    }
}
