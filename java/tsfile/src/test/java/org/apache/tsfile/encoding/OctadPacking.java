package org.apache.iotdb.tsfile.encoding;

import java.util.*;
import java.util.stream.Collectors;

public class OctadPacking {

    // 表示一个Octad
    static class Octad {
        final int index;
        final int bitWidth;

        Octad(int index, int bitWidth) {
            this.index = index;
            this.bitWidth = bitWidth;
        }
    }

    // 表示一个Pack
    static class Pack {
        final List<Octad> octads = new ArrayList<>();
        int maxBitWidth = 0;
        int startIndex;
        int endIndex;

        void addOctad(Octad octad) {
            octads.add(octad);
            if (octads.size() == 1) {
                startIndex = octad.index;
                maxBitWidth = octad.bitWidth;
            } else {
                maxBitWidth = Math.max(maxBitWidth, octad.bitWidth);
            }
            endIndex = octad.index;
        }

        int size() {
            return octads.size();
        }

        int dataStorageCost() {
            return 8 * size() * maxBitWidth;
        }

        @Override
        public String toString() {
            return String.format("Pack[%d-%d]: size=%d, max_b=%d",
                    startIndex, endIndex, size(), maxBitWidth);
        }
    }

    // 打包结果
    static class PackingResult {
        final List<Pack> packs = new ArrayList<>();
        int totalCost;
        int dataCostA;
        int bitWidthCostB;
        int packSizeCostC;

        void calculateCost() {
            dataCostA = packs.stream().mapToInt(Pack::dataStorageCost).sum();
            bitWidthCostB = 5 * packs.size();

            int maxLog = packs.stream()
                    .mapToInt(p -> (int) Math.ceil(Math.log(p.size() + 1) / Math.log(2)))
                    .max().orElse(0);

            packSizeCostC = packs.size() * maxLog;
            totalCost = dataCostA + bitWidthCostB + packSizeCostC;
        }

        @Override
        public String toString() {
            return String.format("Packs: %d, Cost: %d (A=%d, B=%d, C=%d)",
                    packs.size(), totalCost, dataCostA, bitWidthCostB, packSizeCostC);
        }
    }

    // 决策模型接口
    interface DecisionModel {
        boolean shouldMerge(int currentPackSize, int currentPackMaxB,
                            int newOctadB, int packCount, int currentMaxLog);
    }

    // 随机决策模型（用于基线测试）
    static class RandomDecisionModel implements DecisionModel {
        private final Random random = new Random();
        private final double mergeProbability;

        RandomDecisionModel(double mergeProbability) {
            this.mergeProbability = mergeProbability;
        }

        @Override
        public boolean shouldMerge(int currentPackSize, int currentPackMaxB,
                                   int newOctadB, int packCount, int currentMaxLog) {
            return random.nextDouble() < mergeProbability;
        }
    }

    // 固定规则决策模型
    static class RuleBasedDecisionModel implements DecisionModel {
        @Override
        public boolean shouldMerge(int currentPackSize, int currentPackMaxB,
                                   int newOctadB, int packCount, int currentMaxLog) {
            // 简单启发式规则：如果新位宽不会增加当前最大位宽，则合并
            return newOctadB <= currentPackMaxB;
        }
    }

    // 强化学习模型（简化版）
    static class RLDecisionModel implements DecisionModel {
        // 在实际实现中，这里会有神经网络模型
        private final Random random = new Random();
        private double explorationRate = 0.3;

        @Override
        public boolean shouldMerge(int currentPackSize, int currentPackMaxB,
                                   int newOctadB, int packCount, int currentMaxLog) {
            // 探索：随机决策
            if (random.nextDouble() < explorationRate) {
                return random.nextBoolean();
            }

            // 利用：简单启发式策略（实际应用中替换为神经网络预测）
            double mergeBenefit = calculateMergeBenefit(
                    currentPackSize, currentPackMaxB, newOctadB, packCount
            );
            return mergeBenefit > 0.5;
        }

        private double calculateMergeBenefit(int size, int maxB, int newB, int packCount) {
            // 简化的启发式规则：
            // 1. 如果新位宽更小，合并可能有益
            // 2. 大pack可能增加C部分成本
            double sizeFactor = 1.0 - Math.tanh(size / 10.0);
            double bitWidthFactor = (newB <= maxB) ? 1.0 : 0.3;
            return 0.7 * sizeFactor + 0.3 * bitWidthFactor;
        }

        // 训练方法（简化版）
        public void train(List<PackingResult> results) {
            // 在实际实现中，这里会使用策略梯度更新
            // 简化的自适应：根据结果调整探索率
            double avgCost = results.stream()
                    .mapToInt(r -> r.totalCost)
                    .average()
                    .orElse(0);

            if (avgCost < 1000) {
                explorationRate = Math.max(0.1, explorationRate * 0.9);
            } else {
                explorationRate = Math.min(0.8, explorationRate * 1.1);
            }
            System.out.println("Updated exploration rate: " + explorationRate);
        }
    }

    // 打包算法
    static PackingResult packOctads(List<Integer> bitWidths, DecisionModel model) {
        PackingResult result = new PackingResult();
        Pack currentPack = null;
        int globalMaxLog = 0;

        for (int i = 0; i < bitWidths.size(); i++) {
            int b = bitWidths.get(i);
            Octad octad = new Octad(i, b);

            if (currentPack == null) {
                // 第一个octad
                currentPack = new Pack();
                currentPack.addOctad(octad);
            } else if (b == currentPack.maxBitWidth) {
                // 位宽相同必须合并
                currentPack.addOctad(octad);
            } else {
                // 位宽不同，使用模型决策
                int packSize = currentPack.size();
                int packCount = result.packs.size();

                boolean shouldMerge = model.shouldMerge(
                        packSize, currentPack.maxBitWidth, b, packCount, globalMaxLog
                );

                if (shouldMerge) {
                    currentPack.addOctad(octad);
                } else {
                    // 结束当前pack
                    finishPack(currentPack, result, globalMaxLog);
                    globalMaxLog = updateGlobalMaxLog(globalMaxLog, currentPack.size());

                    // 开始新pack
                    currentPack = new Pack();
                    currentPack.addOctad(octad);
                }
            }
        }

        // 处理最后一个pack
        if (currentPack != null) {
            finishPack(currentPack, result, globalMaxLog);
            globalMaxLog = updateGlobalMaxLog(globalMaxLog, currentPack.size());
        }

        result.calculateCost();
        return result;
    }

    private static void finishPack(Pack pack, PackingResult result, int currentGlobalMaxLog) {
        result.packs.add(pack);
        int logValue = (int) Math.ceil(Math.log(pack.size() + 1) / Math.log(2));
        if (logValue > currentGlobalMaxLog) {
            currentGlobalMaxLog = logValue;
        }
    }

    private static int updateGlobalMaxLog(int currentMaxLog, int packSize) {
        int logValue = (int) Math.ceil(Math.log(packSize + 1) / Math.log(2));
        return Math.max(currentMaxLog, logValue);
    }

    // 生成测试数据
    static List<Integer> generateBitWidths(int count) {
        Random random = new Random();
        return random.ints(count, 1, 33)
                .boxed()
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        // 生成测试数据
        List<Integer> bitWidths = generateBitWidths(100);
        System.out.println("Generated " + bitWidths.size() + " octads");

        // 使用不同策略测试
        testStrategy(new RuleBasedDecisionModel(), "Rule-based", bitWidths);
        testStrategy(new RandomDecisionModel(0.5), "Random 50%", bitWidths);
        testStrategy(new RLDecisionModel(), "RL Model", bitWidths);

        // 训练强化学习模型
        trainRLModel(5, 100);
    }

    private static void testStrategy(DecisionModel model, String name, List<Integer> bitWidths) {
        PackingResult result = packOctads(bitWidths, model);
        System.out.println("\n" + name + " Strategy:");
        System.out.println(result);

        // 显示前5个pack
        result.packs.stream().limit(5).forEach(System.out::println);
    }

    private static void trainRLModel(int epochs, int octadCount) {
        System.out.println("\nTraining RL model...");
        RLDecisionModel model = new RLDecisionModel();

        for (int epoch = 1; epoch <= epochs; epoch++) {
            List<PackingResult> results = new ArrayList<>();

            // 运行多个序列进行训练
            for (int i = 0; i < 10; i++) {
                List<Integer> bitWidths = generateBitWidths(octadCount);
                PackingResult result = packOctads(bitWidths, model);
                results.add(result);
            }

            // 更新模型
            model.train(results);

            // 报告平均成本
            double avgCost = results.stream()
                    .mapToInt(r -> r.totalCost)
                    .average()
                    .orElse(0);

            System.out.printf("Epoch %d: Avg Cost = %.2f%n", epoch, avgCost);
        }

        // 测试训练后的模型
        System.out.println("\nTesting trained model:");
        List<Integer> testData = generateBitWidths(100);
        PackingResult testResult = packOctads(testData, model);
        System.out.println(testResult);
    }
}