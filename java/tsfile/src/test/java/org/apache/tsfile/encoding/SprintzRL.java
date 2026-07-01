package org.apache.iotdb.tsfile.encoding;

import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class SprintzRL {

    static final int CHUNK_SIZE = 1024;
    static final int INPUT_DIM = 5;
    static final int HIDDEN_DIM = 48;

    // ========== Pack / Result / DecisionPoint ==========
    static class Pack {
        int size = 0;                    // 这个pack包含多少个octad
        int maxBitWidth = 0;             // 这个pack中所有octad的最大位宽
        int startIndex = 0;              // 第一个octad的索引
        List<Integer> indices = new ArrayList<>();    // octad索引列表
        List<Integer> bitWidths = new ArrayList<>();  // 每个octad的原始位宽

        void addOctad(int index, int bitWidth) {
            if (size == 0) {
                startIndex = index;
                maxBitWidth = bitWidth;
            } else {
                if (bitWidth > maxBitWidth) maxBitWidth = bitWidth;
            }
            indices.add(index);
            bitWidths.add(bitWidth);
            size++;
        }

        long dataCost(int octadSize) {
            // 数据成本: octad数量 × octadSize × pack的位宽
            return (long) size * octadSize * maxBitWidth;
        }

        int logSize() {
            if (size <= 0) return 0;
            return 32 - Integer.numberOfLeadingZeros(size);
        }
    }

    static class PackingResult {
        int packCount = 0;           // pack的数量
        long dataCostA = 0;          // 数据存储成本
        int bitWidthCostB = 0;       // 位宽存储成本 (每个pack 6 bits)
        int packSizeCostC = 0;       // pack大小存储成本
        long totalCost = 0;          // 总成本
        List<Pack> packs = new ArrayList<>();  // 所有的pack
        byte[] compressedData;

        void calculateCost(int maxLog) {
            bitWidthCostB = 6 * packCount;
            packSizeCostC = packCount * maxLog;
            totalCost = dataCostA + bitWidthCostB + packSizeCostC;
        }

        @Override
        public String toString() {
            return String.format("Packs: %d, Cost: %d (A=%d, B=%d, C=%d)",
                    packCount, totalCost, dataCostA, bitWidthCostB, packSizeCostC);
        }
    }

    static class DecisionPoint {
        int currentPackSize;     // 当前pack中的octad数量
        int currentPackMaxB;     // 当前pack的最大位宽
        int newOctadB;          // 新octad的位宽
        int packCount;          // 已创建的pack数量
        int currentMaxLog;      // 当前最大的log(octad数量)
        boolean action;         // 是否合并到当前pack
        float probability;      // 模型预测的概率

        DecisionPoint(int cps, int cpm, int nob, int pc, int cml, boolean a, float p) {
            currentPackSize = cps;
            currentPackMaxB = cpm;
            newOctadB = nob;
            packCount = pc;
            currentMaxLog = cml;
            action = a;
            probability = p;
        }
    }

    // ========== 新增：固定packsize=8方案的结果类 ==========
    static class FixedPackResult {
        long totalCost = 0;           // 总成本（比特数）
        long compressedBits = 0;      // 压缩后的比特数
        byte[] compressedData;        // 压缩数据

        @Override
        public String toString() {
            return String.format("FixedPack Cost: %d bits", totalCost);
        }
    }

    // ========== 改进的奖励函数类 ==========
    static class ImprovedRewardFunction {
        private float baselineCost = 0;
        private float bestCost = Float.MAX_VALUE;
        private float alpha = 0.1f;  // 基线更新率
        private int optimalDPCost = 0;  // 动态规划最优解

        public ImprovedRewardFunction() {}

        public ImprovedRewardFunction(int optimalDPCost) {
            this.optimalDPCost = optimalDPCost;
        }

        // 设置动态规划最优解
        public void setOptimalDPCost(int optimalDPCost) {
            this.optimalDPCost = optimalDPCost;
        }

        // 计算奖励
        public float calculateReward(PackingResult result) {
            float reward = 0.0f;

            // 1. 基础奖励：负的总成本（成本越低，奖励越高）
            float costReward = -result.totalCost / 100000.0f;
            reward += costReward;

            // 2. 如果动态规划最优解已知，计算相对改进
            if (optimalDPCost > 0) {
                float ratio = (float)result.totalCost / optimalDPCost;
                // 如果比动态规划好，给予正奖励；否则负奖励
                if (ratio < 1.0f) {
                    reward += (1.0f - ratio) * 2.0f;  // 优于动态规划，额外奖励
                } else {
                    reward -= (ratio - 1.0f) * 0.5f;  // 差于动态规划，惩罚
                }
            }

            // 3. 奖励压缩比提升
            if (baselineCost == 0) {
                baselineCost = result.totalCost;
            } else {
                float improvement = (baselineCost - result.totalCost) / baselineCost;
                reward += improvement * 5.0f;  // 改进越大，奖励越大

                // 更新基线
                baselineCost = baselineCost * (1 - alpha) + result.totalCost * alpha;
            }

            // 4. 如果创造了新的最好成绩，额外奖励
            if (result.totalCost < bestCost) {
                reward += 1.0f;
                bestCost = result.totalCost;
            }

            // 5. 惩罚过大的pack数量（鼓励合并）
            float packCountPenalty = -result.packCount * 0.01f;
            reward += packCountPenalty;

            // 6. 鼓励合理的pack大小分布
            float sizeBalanceReward = calculateSizeBalanceReward(result.packs);
            reward += sizeBalanceReward;

            return reward;
        }

        private float calculateSizeBalanceReward(List<Pack> packs) {
            if (packs.size() < 2) return 0.0f;

            float avgSize = 0;
            for (Pack pack : packs) {
                avgSize += pack.size;
            }
            avgSize /= packs.size();

            float variance = 0;
            for (Pack pack : packs) {
                float diff = pack.size - avgSize;
                variance += diff * diff;
            }
            variance /= packs.size();

            // 方差越小，奖励越高（鼓励均匀分组）
            return -variance / 1000.0f;
        }

        public void reset() {
            baselineCost = 0;
            bestCost = Float.MAX_VALUE;
        }
    }

    // ========== 2-layer MLP policy with REINFORCE ==========
    static class RLDecisionModel {
        float[] W1; // size HIDDEN_DIM * INPUT_DIM
        float[] b1; // size HIDDEN_DIM
        float[] W2; // size HIDDEN_DIM
        float b2;

        float explorationRate = 0.3f;
        float learningRate = 0.01f;
        ImprovedRewardFunction rewardFunction;  // 改进的奖励函数

        Random rng;

        RLDecisionModel() {
            rng = new Random();
            W1 = new float[HIDDEN_DIM * INPUT_DIM];
            b1 = new float[HIDDEN_DIM];
            W2 = new float[HIDDEN_DIM];
            for (int i = 0; i < W1.length; ++i) W1[i] = randUniform(-0.08f, 0.08f);
            for (int i = 0; i < b1.length; ++i) b1[i] = randUniform(-0.08f, 0.08f);
            for (int i = 0; i < W2.length; ++i) W2[i] = randUniform(-0.08f, 0.08f);
            b2 = randUniform(-0.08f, 0.08f);

            // 初始化奖励函数
            rewardFunction = new ImprovedRewardFunction();
        }

        // 设置动态规划最优解（用于奖励计算）
        public void setOptimalDPCost(int optimalDPCost) {
            rewardFunction.setOptimalDPCost(optimalDPCost);
        }

        private float randUniform(float a, float b) {
            return a + rng.nextFloat() * (b - a);
        }

        static float relu(float x) { return x > 0.0f ? x : 0.0f; }
        static float reluDeriv(float x) { return x > 0.0f ? 1.0f : 0.0f; }
        static float sigmoid(float x) {
            if (x >= 0) {
                double z = Math.exp(-x);
                return (float)(1.0 / (1.0 + z));
            } else {
                double z = Math.exp(x);
                return (float)(z / (1.0 + z));
            }
        }

        float forwardProb(float[] feat, float[] outHidden, float[] outZ1) {
            if (outHidden != null) Arrays.fill(outHidden, 0.0f);
            if (outZ1 != null) Arrays.fill(outZ1, 0.0f);

            for (int h = 0; h < HIDDEN_DIM; ++h) {
                float z = b1[h];
                int base = h * INPUT_DIM;
                for (int j = 0; j < INPUT_DIM; ++j) {
                    z += W1[base + j] * feat[j];
                }
                if (outZ1 != null) outZ1[h] = z;
                float hval = relu(z);
                if (outHidden != null) outHidden[h] = hval;
            }

            float z2 = b2;
            if (outHidden != null) {
                for (int h = 0; h < HIDDEN_DIM; ++h) z2 += W2[h] * outHidden[h];
            } else {
                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    float z = b1[h];
                    int base = h * INPUT_DIM;
                    for (int j = 0; j < INPUT_DIM; ++j) z += W1[base + j] * feat[j];
                    float hval = relu(z);
                    z2 += W2[h] * hval;
                }
            }
            return sigmoid(z2);
        }

        float forwardProb(float[] feat) {
            return forwardProb(feat, null, null);
        }

        float train(List<DecisionPoint> decisions, float reward) {
            if (decisions == null || decisions.isEmpty()) return 0.0f;

            explorationRate *= 0.99f;
            if (explorationRate < 0.05f) explorationRate = 0.05f;

            float[] dW1 = new float[W1.length];
            float[] db1 = new float[b1.length];
            float[] dW2 = new float[W2.length];
            float db2 = 0.0f;

            float totalLoss = 0.0f;

            float[] feat = new float[INPUT_DIM];
            float[] hidden = new float[HIDDEN_DIM];
            float[] z1 = new float[HIDDEN_DIM];

            for (DecisionPoint dp : decisions) {
                feat[0] = dp.currentPackSize / 1024.0f;
                feat[1] = dp.currentPackMaxB / 64.0f;
                feat[2] = dp.newOctadB / 64.0f;
                feat[3] = dp.packCount / 1024.0f;
                feat[4] = dp.currentMaxLog / 10.0f;

                float p = forwardProb(feat, hidden, z1);

                float pClipped = Math.min(Math.max(p, 1e-6f), 1.0f - 1e-6f);

                float piA = dp.action ? pClipped : (1.0f - pClipped);
                if (piA <= 0.0f) {
                    piA = 1e-6f;
                }
                float lossI = -reward * (float) Math.log(piA);

                if (Float.isNaN(lossI) || Float.isInfinite(lossI)) {
                    System.err.printf("Warning: loss is NaN or Infinite. p=%.8f, piA=%.8f, reward=%.8f\n", p, piA, reward);
                    lossI = 0.0f;
                }

                totalLoss += lossI;

                float dL_dz2 = reward * (p - (dp.action ? 1.0f : 0.0f));

                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    dW2[h] += dL_dz2 * hidden[h];
                }
                db2 += dL_dz2;

                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    float w2h = W2[h];
                    float dh = dL_dz2 * w2h;
                    float dReLU = reluDeriv(z1[h]);
                    float dZ1 = dh * dReLU;
                    int base = h * INPUT_DIM;
                    for (int j = 0; j < INPUT_DIM; ++j) {
                        dW1[base + j] += dZ1 * feat[j];
                    }
                    db1[h] += dZ1;
                }
            }

            float lr = learningRate;
            for (int i = 0; i < W1.length; ++i) {
                W1[i] -= lr * dW1[i];
                W1[i] = clip(W1[i], -10f, 10f);
            }
            for (int i = 0; i < b1.length; ++i) {
                b1[i] -= lr * db1[i];
                b1[i] = clip(b1[i], -10f, 10f);
            }
            for (int i = 0; i < W2.length; ++i) {
                W2[i] -= lr * dW2[i];
                W2[i] = clip(W2[i], -10f, 10f);
            }
            b2 -= lr * db2;
            b2 = clip(b2, -10f, 10f);

            return totalLoss;
        }

        static float clip(float v, float low, float high) {
            return Math.min(Math.max(v, low), high);
        }

        // 计算奖励（使用改进的奖励函数）
        public float calculateReward(PackingResult result) {
            return rewardFunction.calculateReward(result);
        }

        // 重置奖励函数状态
        public void resetRewardFunction() {
            rewardFunction.reset();
        }
    }

    // ========== 动态规划包装器 ==========
    static class DPPackingWrapper {
        public static int computeOptimalCostDP(int[] bitWidths, int pack_size) {
            int N = bitWidths.length;
            if (N == 0) return 0;

            // 预计算所有区间的最大位宽
            int[][] maxB = new int[N][N];
            for (int l = 0; l < N; l++) {
                maxB[l][l] = bitWidths[l];
                for (int r = l + 1; r < N; r++) {
                    maxB[l][r] = Math.max(maxB[l][r - 1], bitWidths[r]);
                }
            }

            int minTotalCost = Integer.MAX_VALUE;
            int maxPossibleC = 64 - Integer.numberOfLeadingZeros(N);

            for (int C = 1; C <= maxPossibleC; C++) {
                int low_C = (C == 1) ? 1 : (1 << (C - 1));
                int high_C = Math.min((1 << C) - 1, N);

                int[][] dp = new int[N + 1][2];
                for (int i = 0; i <= N; i++) {
                    dp[i][0] = Integer.MAX_VALUE / 2;
                    dp[i][1] = Integer.MAX_VALUE / 2;
                }
                dp[0][0] = 0;

                for (int i = 1; i <= N; i++) {
                    for (int k = Math.max(1, i - high_C + 1); k <= i; k++) {
                        int packLength = i - k + 1;
                        int currentMaxB = maxB[k - 1][i - 1];
                        int packCost = pack_size * packLength * currentMaxB + 6 + C;

                        if (packLength < low_C) {
                            // 小分组，不能改变状态
                            if (dp[k - 1][0] + packCost < dp[i][0]) {
                                dp[i][0] = dp[k - 1][0] + packCost;
                            }
                            if (dp[k - 1][1] + packCost < dp[i][1]) {
                                dp[i][1] = dp[k - 1][1] + packCost;
                            }
                        } else {
                            // 大分组，可以改变状态到1
                            if (dp[k - 1][0] + packCost < dp[i][1]) {
                                dp[i][1] = dp[k - 1][0] + packCost;
                            }
                            if (dp[k - 1][1] + packCost < dp[i][1]) {
                                dp[i][1] = dp[k - 1][1] + packCost;
                            }
                        }
                    }
                }

                if (dp[N][1] < minTotalCost) {
                    minTotalCost = dp[N][1];
                }
            }

            return minTotalCost;
        }
    }

    // ========== 新增：计算固定packsize=8方案的成本 ==========
    static FixedPackResult calculateFixedPackCost(long[] data, int originalLength) {
        FixedPackResult result = new FixedPackResult();

        // 确保数据长度是8的倍数
        int octadSize = 8;
        int remainder = data.length % octadSize;
        int padding = (remainder == 0) ? 0 : octadSize - remainder;
        long[] padded = new long[data.length + padding];
        System.arraycopy(data, 0, padded, 0, data.length);
        if (padding > 0) Arrays.fill(padded, data.length, padded.length, 0L);

        // 计算每个octad的bitwidth和存储成本
        long totalBits = 0;
        List<Integer> bitWidths = new ArrayList<>();

        for (int i = 0; i < padded.length; i += octadSize) {
            long maxInOctad = 0;
            for (int j = i; j < i + octadSize && j < padded.length; j++) {
                if (padded[j] > maxInOctad) maxInOctad = padded[j];
            }
            int bitWidth = 0;
            if (maxInOctad > 0) {
                bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
            }
            bitWidths.add(bitWidth);

            // 每个octad的成本：6位存储bitwidth + 8个值按bitwidth存储
            totalBits += 6 + (octadSize * bitWidth);
        }

        result.totalCost = totalBits;
        result.compressedBits = totalBits;

        // 生成压缩数据
        result.compressedData = performFixedPackCompression(padded, bitWidths, octadSize);

        return result;
    }

    // 执行固定packsize=8的压缩
    static byte[] performFixedPackCompression(long[] data, List<Integer> bitWidths, int octadSize) {
        BitWriter writer = new BitWriter();

        // 每个octad: 6位bitwidth + 8个值
        for (int octadIdx = 0; octadIdx < bitWidths.size(); octadIdx++) {
            int bitWidth = bitWidths.get(octadIdx);
            int startPos = octadIdx * octadSize;

            // 写入bitwidth (6位)
            writer.writeBits(bitWidth, 6);

            // 写入8个值
            for (int j = 0; j < octadSize; j++) {
                int pos = startPos + j;
                if (pos < data.length) {
                    long val = data[pos];
                    writer.writeBits(val, bitWidth);
                } else {
                    writer.writeBits(0L, bitWidth); // 填充
                }
            }
        }

        return writer.finish();
    }

    // ========== 训练方法（改进版） ==========
    static RLDecisionModel trainModelFromDirectory(int epochs, String directoryPath) {
        System.err.println("Training RL model from directory: " + directoryPath);
        RLDecisionModel model = new RLDecisionModel();

        // 加载训练数据
        List<List<Long>> sequences = loadRawValuesFromDirectory(directoryPath);
        if (sequences.isEmpty()) {
            System.err.println("No data loaded from directory. Returning initial model.");
            return model;
        }

        System.err.println("Loaded " + sequences.size() + " sequences of 1024 values from directory");

        List<DecisionPoint> decisionTrace = new ArrayList<>();

        // 为每个序列预计算动态规划最优解（用于奖励计算）
        Map<Integer, Integer> dpOptimalCosts = new HashMap<>();
        System.err.println("Pre-computing DP optimal costs for each sequence...");
        for (int seqIdx = 0; seqIdx < sequences.size(); seqIdx++) {
            List<Long> rawSequence = sequences.get(seqIdx);
            int octadSize = 8;

            // 转换为数组
            long[] sequenceArray = new long[rawSequence.size()];
            for (int i = 0; i < rawSequence.size(); i++) {
                sequenceArray[i] = rawSequence.get(i);
            }

            // 计算每个octad的bitwidth
            List<Integer> bitWidths = new ArrayList<>();
            for (int i = 0; i < sequenceArray.length; i += octadSize) {
                long maxInOctad = 0;
                for (int j = i; j < i + octadSize && j < sequenceArray.length; j++) {
                    if (sequenceArray[j] > maxInOctad) maxInOctad = sequenceArray[j];
                }
                int bitWidth = 0;
                if (maxInOctad > 0) {
                    bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
                }
                bitWidths.add(bitWidth);
            }

            // 转换为数组并计算动态规划最优解
            int[] bitWidthsArray = bitWidths.stream().mapToInt(Integer::intValue).toArray();
            int optimalDPCost = DPPackingWrapper.computeOptimalCostDP(bitWidthsArray, octadSize);
            dpOptimalCosts.put(seqIdx, optimalDPCost);

            if (seqIdx % 10 == 0) {
                System.err.println("  Sequence " + seqIdx + ": DP optimal cost = " + optimalDPCost);
            }
        }

        for (int epoch = 1; epoch <= epochs; ++epoch) {
            long startTime = System.nanoTime();
            float totalReward = 0.0f;
            float totalLoss = 0.0f;
            int processedSequences = 0;

            // 每5个epoch重置奖励函数
            if (epoch % 5 == 1) {
                model.resetRewardFunction();
            }

            for (int seqIdx = 0; seqIdx < sequences.size(); seqIdx++) {
                List<Long> rawSequence = sequences.get(seqIdx);
                int octadSize = 1;

                // 转换为数组
                long[] sequenceArray = new long[rawSequence.size()];
                for (int i = 0; i < rawSequence.size(); i++) {
                    sequenceArray[i] = rawSequence.get(i);
                }

                // 计算每个octad的bitwidth
                List<Integer> bitWidths = new ArrayList<>();
                for (int i = 0; i < sequenceArray.length; i += octadSize) {
                    long maxInOctad = 0;
                    for (int j = i; j < i + octadSize && j < sequenceArray.length; j++) {
                        if (sequenceArray[j] > maxInOctad) maxInOctad = sequenceArray[j];
                    }
                    int bitWidth = 0;
                    if (maxInOctad > 0) {
                        bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
                    }
                    bitWidths.add(bitWidth);
                }

                // 设置当前序列的动态规划最优解
                model.setOptimalDPCost(dpOptimalCosts.get(seqIdx));

                decisionTrace.clear();
                PackingResult result = packOctads(bitWidths, model, decisionTrace, octadSize, null, 0);

                // 使用改进的奖励函数计算奖励
                float reward = model.calculateReward(result);
                totalReward += reward;

                float loss = model.train(decisionTrace, reward);
                totalLoss += loss;
                processedSequences++;
            }

            long durationMs = (System.nanoTime() - startTime) / 1_000_000L;

            if (epoch % 10 == 0 || epoch == 1 || epoch == epochs) {
                System.out.printf("Epoch %d: Avg Reward = %.6f, Avg Loss = %.6f, Time = %d ms%n",
                        epoch,
                        totalReward / processedSequences,
                        totalLoss / processedSequences,
                        durationMs);
            } else {
                System.out.printf("Epoch %d done. Time = %d ms%n", epoch, durationMs);
            }
        }

        return model;
    }

    // ========== 以下是未修改的原有方法，为保持完整性包含 ==========

    static int getBitWidth(long num) {
        if (num == 0)
            return 1;
        else
            return 64 - Long.numberOfLeadingZeros(num);
    }

    public static int bitPacking(long[] values, int start, int length, int bitWidth, int encodePos,
                                 byte[] encodedResult) {
        if (values == null || length <= 0 || bitWidth == 0) {
            return encodePos;
        }

        int currentByte = 0;
        int bitsInCurrentByte = 0;
        int bytePos = encodePos;

        for (int i = 0; i < length; i++) {
            long value = values[start + i];
            int remainingBits = bitWidth;

            while (remainingBits > 0) {
                int bitsToWrite = Math.min(remainingBits, 8 - bitsInCurrentByte);
                int shift = remainingBits - bitsToWrite;
                long bits = (value >>> shift) & ((1L << bitsToWrite) - 1);
                currentByte = (currentByte << bitsToWrite) | (int)bits;
                bitsInCurrentByte += bitsToWrite;

                if (bitsInCurrentByte == 8) {
                    encodedResult[bytePos] = (byte) currentByte;
                    bytePos++;
                    currentByte = 0;
                    bitsInCurrentByte = 0;
                }

                remainingBits -= bitsToWrite;
            }
        }

        if (bitsInCurrentByte > 0) {
            currentByte = currentByte << (8 - bitsInCurrentByte);
            encodedResult[bytePos] = (byte) currentByte;
            bytePos++;
        }

        return bytePos;
    }

    public static long[] decodeBitPacking(byte[] encoded, int decodePos, int bitWidth,
                                          int numValues, int octadSize) {
        if (encoded == null || bitWidth == 0 || numValues == 0) {
            return new long[0];
        }

        long[] result = new long[numValues];
        int currentByte = 0;
        int bitsInCurrentByte = 0;
        int bytePos = decodePos;

        for (int i = 0; i < numValues; i++) {
            long value = 0;
            int bitsRead = 0;

            while (bitsRead < bitWidth) {
                if (bitsInCurrentByte == 0) {
                    if (bytePos >= encoded.length) {
                        return Arrays.copyOf(result, i);
                    }
                    currentByte = encoded[bytePos] & 0xFF;
                    bytePos++;
                    bitsInCurrentByte = 8;
                }

                int bitsToRead = Math.min(bitWidth - bitsRead, bitsInCurrentByte);
                int shift = bitsInCurrentByte - bitsToRead;
                int bits = (currentByte >>> shift) & ((1 << bitsToRead) - 1);
                value = (value << bitsToRead) | bits;
                bitsRead += bitsToRead;
                currentByte &= (1 << shift) - 1;
                bitsInCurrentByte -= bitsToRead;
            }

            result[i] = value;
        }

        return result;
    }

    static List<List<Long>> loadRawValuesFromDirectory(String directoryPath) {
        List<List<Long>> allSequences = new ArrayList<>();
        File dir = new File(directoryPath);

        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Directory not found: " + directoryPath);
            return allSequences;
        }

        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".csv"));
        if (files == null || files.length == 0) {
            System.err.println("No CSV files found in directory: " + directoryPath);
            return allSequences;
        }

        for (File file : files) {
            if (!BenchmarkDatasetFilter.includeDatasetFile(file.getName())) {
                continue;
            }

            System.out.println("Loading training data from: " + file.getName());

            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                List<Double> allValues = new ArrayList<>();
                String line;

                boolean hasHeader = true;
                int lineCount = 0;

                while ((line = br.readLine()) != null) {
                    lineCount++;
                    String[] tokens = line.split(",");

                    for (String token : tokens) {
                        String s = token.trim();
                        if (!s.isEmpty() && !s.equals("value") && !s.equals("timestamp")) {
                            try {
                                double val = Double.parseDouble(s);
                                allValues.add(val);
                            } catch (NumberFormatException e) {
                            }
                        }
                    }
                }

                System.out.println("  Total values in file: " + allValues.size());

                if (allValues.isEmpty()) {
                    continue;
                }

                int targetCount = (int) Math.ceil(allValues.size() * 0.1);
                System.out.println("  Target count (10%): " + targetCount);

                if (targetCount < 1024) {
                    targetCount = Math.min(1024, allValues.size());
                    System.out.println("  Adjusted to: " + targetCount + " (min 1024 or all if less)");
                } else {
                    targetCount = targetCount - (targetCount % 1024);
                    targetCount = Math.min(targetCount, allValues.size());
                    System.out.println("  Adjusted to: " + targetCount + " (1024 multiples)");
                }

                List<Long> selectedValues = new ArrayList<>();
                for (int i = 0; i < targetCount && i < allValues.size(); i++) {
                    selectedValues.add(Math.round(allValues.get(i)));
                }

                System.out.println("  Selected values: " + selectedValues.size());

                for (int i = 0; i < selectedValues.size(); i += 1024) {
                    int end = Math.min(i + 1024, selectedValues.size());
                    if (end - i == 1024) {
                        List<Long> sequence = new ArrayList<>(selectedValues.subList(i, end));
                        allSequences.add(sequence);
                    }
                }

                System.out.println("  Created " + (selectedValues.size() / 1024) + " sequences of 1024 values");

            } catch (IOException e) {
                System.err.println("Error reading file: " + file.getName() + " - " + e.getMessage());
            }
        }

        System.out.println("Total training sequences loaded: " + allSequences.size());
        return allSequences;
    }

    static String trimStr(String s) {
        if (s == null) return "";
        int a = 0;
        while (a < s.length() && Character.isWhitespace(s.charAt(a))) a++;
        if (a == s.length()) return "";
        int b = s.length() - 1;
        while (b >= 0 && Character.isWhitespace(s.charAt(b))) b--;
        return s.substring(a, b + 1);
    }

    static String stripEnclosingQuotes(String s) {
        if (s == null) return "";
        if (s.length() >= 2) {
            char f = s.charAt(0);
            char l = s.charAt(s.length() - 1);
            if ((f == '"' && l == '"') || (f == '\'' && l == '\'')) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }

    static long[] scaleNumbers(List<String> numbers, int decimalMax) {
        int n = numbers.size();
        long[] result = new long[n];
        if (n == 0) return result;

        BigDecimal scale = BigDecimal.ONE;
        for (int i = 0; i < decimalMax; ++i) scale = scale.multiply(BigDecimal.TEN);

        BigDecimal[] vals = new BigDecimal[n];
        for (int i = 0; i < n; ++i) {
            String s = trimStr(numbers.get(i));
            s = stripEnclosingQuotes(s);
            if (s.isEmpty()) { vals[i] = BigDecimal.ZERO; continue; }
            s = s.replace(",", "");

            try {
                BigDecimal bd = new BigDecimal(s);
                BigDecimal scaled = bd.multiply(scale);
                BigDecimal rounded = scaled.setScale(0, RoundingMode.HALF_UP);
                vals[i] = rounded;
            } catch (Exception ex) {
                try {
                    double dv = Double.parseDouble(s);
                    BigDecimal bd = BigDecimal.valueOf(dv).multiply(scale);
                    vals[i] = bd.setScale(0, RoundingMode.HALF_UP);
                } catch (Exception ex2) {
                    System.err.println("Warning: cannot parse token '" + numbers.get(i) + "', set to 0");
                    vals[i] = BigDecimal.ZERO;
                }
            }
        }

        BigDecimal minv = vals[0];
        for (int i = 1; i < n; ++i) if (vals[i].compareTo(minv) < 0) minv = vals[i];

        for (int i = 0; i < n; ++i) {
            BigDecimal shifted = vals[i].subtract(minv);
            try {
                BigInteger bi = shifted.toBigIntegerExact();
                if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) result[i] = Long.MAX_VALUE;
                else if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) result[i] = Long.MIN_VALUE;
                else result[i] = bi.longValue();
            } catch (ArithmeticException ae) {
                BigDecimal rounded = shifted.setScale(0, RoundingMode.HALF_UP);
                try {
                    BigInteger bi = rounded.toBigIntegerExact();
                    if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) result[i] = Long.MAX_VALUE;
                    else if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) result[i] = Long.MIN_VALUE;
                    else result[i] = bi.longValue();
                } catch (Exception ex) {
                    result[i] = 0;
                }
            }
        }
        return result;
    }

    public static long[] sprintz(long[] numbers) {
        int size = numbers.length;
        long[] result = new long[size];

        if (size == 0) return result;

        long first = numbers[0];
        result[0] = first;

        long prev = first;
        for (int i = 1; i < size; i++) {
            long current = numbers[i];
            long diff = current - prev;
            result[i] = (diff << 1) ^ (diff >> 63);
            prev = current;
        }

        return result;
    }

    public static long[] sprintzDecode(long[] encodedData) {
        int size = encodedData.length;
        long[] result = new long[size];

        if (size == 0) return result;

        result[0] = encodedData[0];
        long prev = result[0];
        for (int i = 1; i < size; i++) {
            long zigzagEncoded = encodedData[i];
            long diff = (zigzagEncoded >>> 1) ^ -(zigzagEncoded & 1);
            result[i] = prev + diff;
            prev = result[i];
        }

        return result;
    }

    private static class BitWriter {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private long acc = 0L;
        private int accBits = 0;

        void writeBits(long bits, int bitCount) {
            if (bitCount == 0) return;
            if (bitCount == 64) {
                writeBits((bits >>> 32) & 0xFFFFFFFFL, 32);
                writeBits(bits & 0xFFFFFFFFL, 32);
                return;
            }
            long mask = (bitCount == 64) ? ~0L : ((1L << bitCount) - 1L);
            long v = bits & mask;
            acc = (acc << bitCount) | v;
            accBits += bitCount;
            while (accBits >= 8) {
                int shift = accBits - 8;
                int outb = (int) ((acc >>> shift) & 0xFFL);
                out.write(outb);
                if (shift > 0) {
                    acc &= ((1L << shift) - 1L);
                } else {
                    acc = 0L;
                }
                accBits = shift;
            }
        }

        byte[] finish() {
            if (accBits > 0) {
                int outb = (int) ((acc << (8 - accBits)) & 0xFFL);
                out.write(outb);
                acc = 0L;
                accBits = 0;
            }
            return out.toByteArray();
        }
    }

    public static final class BitReader {
        private final byte[] data;
        private int bitPos;

        public BitReader(byte[] data) {
            this(data, 0);
        }

        public BitReader(byte[] data, int byteOffset) {
            this.data = data;
            this.bitPos = byteOffset * 8;
        }

        public long readBits(int n) {
            if (n == 0) return 0L;
            if (n < 0 || n > 64) {
                throw new IllegalArgumentException("n must be between 0 and 64");
            }

            long result = 0L;
            int bitsRemaining = n;

            while (bitsRemaining > 0) {
                int byteIndex = bitPos >>> 3;
                int bitOffset = bitPos & 7;

                if (byteIndex >= data.length) {
                    result = (result << bitsRemaining);
                    bitPos += bitsRemaining;
                    return result;
                }

                int bitsFromCurrentByte = Math.min(8 - bitOffset, bitsRemaining);
                int curByte = data[byteIndex] & 0xFF;
                int shift = 8 - bitOffset - bitsFromCurrentByte;
                int chunk = (curByte >>> shift) & ((1 << bitsFromCurrentByte) - 1);

                result = (result << bitsFromCurrentByte) | chunk;

                bitPos += bitsFromCurrentByte;
                bitsRemaining -= bitsFromCurrentByte;
            }

            return result;
        }

        public int consumedBits() {
            return bitPos;
        }

        public int bitPosition() {
            return bitPos;
        }

        public int remainingBits() {
            return (data.length * 8) - bitPos;
        }
    }

    private static byte[] performBitPackingCompression(long[] dataArray, List<Pack> packs,
                                                       int octadSize, int originalLength) throws IOException {
        int totalPacks = packs.size();
        int maxOctadsInAnyPack = 0;
        for (Pack p : packs) {
            if (p.size > maxOctadsInAnyPack) maxOctadsInAnyPack = p.size;
        }

        int bitsForCount = 1;
        while ((1L << bitsForCount) <= maxOctadsInAnyPack) bitsForCount++;
        if (bitsForCount <= 0) bitsForCount = 1;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeByte(totalPacks);
        dos.writeByte(bitsForCount);
        dos.flush();

        BitWriter metaWriter = new BitWriter();

        for (Pack pack : packs) {
            metaWriter.writeBits(pack.size, bitsForCount);
            metaWriter.writeBits(pack.maxBitWidth, 6);
        }

        byte[] metaBytes = metaWriter.finish();
        dos.write(metaBytes);
        dos.flush();

        BitWriter dataWriter = new BitWriter();

        for (Pack pack : packs) {
            int packBitWidth = pack.maxBitWidth;

            for (int octadIdx = 0; octadIdx < pack.size; ++octadIdx) {
                int originalOctadIndex = pack.indices.get(octadIdx);
                int startPos = originalOctadIndex * octadSize;

                for (int j = 0; j < octadSize; ++j) {
                    if (startPos + j >= dataArray.length) {
                        dataWriter.writeBits(0L, packBitWidth);
                    } else {
                        long val = dataArray[startPos + j];
                        if (packBitWidth == 64) {
                            dataWriter.writeBits(val, 64);
                        } else {
                            long mask = (1L << packBitWidth) - 1L;
                            long masked = val & mask;
                            dataWriter.writeBits(masked, packBitWidth);
                        }
                    }
                }
            }
        }

        byte[] dataBytes = dataWriter.finish();
        dos.write(dataBytes);
        dos.flush();

        return baos.toByteArray();
    }

    static PackingResult packOctads(List<Integer> bitWidths, RLDecisionModel model,
                                    List<DecisionPoint> decisionTrace, int octadSize,
                                    long[] dataArray, int originalLength) {
        PackingResult result = new PackingResult();
        Pack currentPack = new Pack();
        int globalMaxLog = 0;
        int packCount = 0;

        Random localRng = ThreadLocalRandom.current();

        for (int i = 0; i < bitWidths.size(); ++i) {
            int b = bitWidths.get(i);

            if (currentPack.size == 0) {
                currentPack.addOctad(i, b);
            } else if (b == currentPack.maxBitWidth) {
                currentPack.addOctad(i, b);
            } else {
                float[] feat = new float[INPUT_DIM];
                feat[0] = currentPack.size / 1024.0f;
                feat[1] = currentPack.maxBitWidth / 64.0f;
                feat[2] = b / 64.0f;
                feat[3] = packCount / 1024.0f;
                feat[4] = globalMaxLog / 10.0f;

                float probability = model.forwardProb(feat);

                boolean shouldMerge;
                if (localRng.nextFloat() < model.explorationRate) {
                    shouldMerge = (localRng.nextFloat() > 0.5f);
                } else {
                    shouldMerge = probability > 0.5f;
                }

                if (decisionTrace != null) {
                    decisionTrace.add(new DecisionPoint(currentPack.size, currentPack.maxBitWidth,
                            b, packCount, globalMaxLog, shouldMerge, probability));
                }

                if (shouldMerge) {
                    currentPack.addOctad(i, b);
                } else {
                    result.dataCostA += currentPack.dataCost(octadSize);
                    int logSize = currentPack.logSize();
                    if (logSize > globalMaxLog) globalMaxLog = logSize;
                    result.packs.add(currentPack);
                    packCount++;

                    currentPack = new Pack();
                    currentPack.addOctad(i, b);
                }
            }
        }

        if (currentPack.size > 0) {
            result.dataCostA += currentPack.dataCost(octadSize);
            int logSize = currentPack.logSize();
            if (logSize > globalMaxLog) globalMaxLog = logSize;
            result.packs.add(currentPack);
            packCount++;
        }

        result.packCount = packCount;
        result.calculateCost(globalMaxLog);

        if (dataArray != null) {
            try {
                result.compressedData = performBitPackingCompression(dataArray, result.packs,
                        octadSize, originalLength);
            } catch (IOException e) {
                System.err.println("Compression failed: " + e.getMessage());
                result.compressedData = null;
            }
        }

        return result;
    }

    public static long[] fastDecompress(byte[] compressedData, int octadSize, int originalLength) {
        if (compressedData == null || compressedData.length == 0) {
            System.err.println("Compressed data is null or empty");
            return new long[0];
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
            DataInputStream dis = new DataInputStream(bais);

            int totalPacks = dis.readUnsignedByte();
            int bitsForCount = dis.readUnsignedByte();
            int storedOctadSize = dis.readUnsignedByte();

            if (storedOctadSize != octadSize) {
                System.err.println("Warning: octadSize mismatch. Expected " + octadSize +
                        ", got " + storedOctadSize);
            }

            int metaStartOffset = 3;
            BitReader metaReader = new BitReader(compressedData, metaStartOffset);

            List<PackInfo> packInfos = new ArrayList<>();
            int totalValuesToDecode = 0;

            for (int p = 0; p < totalPacks; ++p) {
                int octadCount = (int) metaReader.readBits(bitsForCount);
                int packBitWidth = (int) metaReader.readBits(6);
                if (packBitWidth == 63) packBitWidth = 64;

                packInfos.add(new PackInfo(octadCount, packBitWidth));
                totalValuesToDecode += octadCount * octadSize;
            }

            int metaBitsUsed = metaReader.consumedBits();
            int dataStartByte = metaStartOffset + (metaBitsUsed + 7) / 8;

            if (dataStartByte >= compressedData.length) {
                return new long[0];
            }

            BitReader dataReader = new BitReader(compressedData, dataStartByte);
            List<Long> resultList = new ArrayList<>();

            for (PackInfo packInfo : packInfos) {
                int octadCount = packInfo.octadCount;
                int bitWidth = packInfo.bitWidth;

                if (dataReader.remainingBits() < (long) octadCount * octadSize * bitWidth) {
                    break;
                }

                for (int i = 0; i < octadCount; ++i) {
                    for (int j = 0; j < octadSize; ++j) {
                        long value;
                        if (bitWidth == 0) {
                            value = 0L;
                        } else if (bitWidth == 64) {
                            long high = dataReader.readBits(32);
                            long low = dataReader.readBits(32);
                            value = (high << 32) | low;
                        } else {
                            value = dataReader.readBits(bitWidth);
                        }
                        resultList.add(value);
                    }
                }
            }

            long[] result = new long[Math.min(resultList.size(), originalLength)];
            for (int i = 0; i < result.length; i++) {
                result[i] = resultList.get(i);
            }

            return result;

        } catch (Exception e) {
            System.err.println("Fast decompression failed: " + e.getMessage());
            e.printStackTrace();
            return new long[0];
        }
    }

    static class PackInfo {
        int octadCount;
        int bitWidth;

        PackInfo(int octadCount, int bitWidth) {
            this.octadCount = octadCount;
            this.bitWidth = bitWidth;
        }
    }

    // ========== 修改后的性能测试方法（选择更优方案） ==========
    static void performanceTest(RLDecisionModel model, String directory, String outputDirStr) {
        System.out.println("\nPerformance Testing with varying octadSize...");
        Path outdir = Paths.get(outputDirStr);
        try {
            if (!Files.exists(outdir)) Files.createDirectories(outdir);
        } catch (IOException e) {
            System.err.println("Cannot create output dir: " + outputDirStr);
            return;
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path entry : ds) {
                if (!Files.isRegularFile(entry)) continue;
                String fname = entry.getFileName().toString();
                if (!BenchmarkDatasetFilter.includeDatasetFile(fname)) continue;

                System.out.println("Processing " + fname + "...");
                List<String> numbers = new ArrayList<>();
                List<Integer> decimalPlaces = new ArrayList<>();

                try (BufferedReader br = Files.newBufferedReader(entry)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] tokens = line.split(",");
                        for (String token : tokens) {
                            String t = trimStr(token);
                            if (!t.isEmpty()) {
                                numbers.add(t);
                                int dec = 0;
                                int pos = t.indexOf('.');
                                if (pos != -1) dec = t.length() - pos - 1;
                                decimalPlaces.add(dec);
                            }
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Cannot open " + entry.toString());
                    continue;
                }

                if (numbers.isEmpty()) continue;

                Path outPath = outdir.resolve(fname);
                try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
                    writer.write("Pack Size,Input Direction,Encoding Algorithm,Encoding Time,Points,Compressed Size,Compression Ratio,RL Better Count,Fixed Better Count\n");

                    int time_of_repeat = 10;

                    for (int octadSizeExp = 3; octadSizeExp < 4; octadSizeExp++) {
                        int octadSize = (int) Math.pow(2, octadSizeExp);
                        System.out.println("Testing octadSize = " + octadSize);

                        long hybridCost = 0;
                        long hybridTime = 0;
                        int rlBetterCount = 0;
                        int fixedBetterCount = 0;

                        for (int rep = 0; rep < time_of_repeat; ++rep) {
                            for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                                int end = Math.min(numbers.size(), i + CHUNK_SIZE);
                                if (end - i <= 2) continue;

                                List<String> chunkNumbers = numbers.subList(i, end);
                                int decimalMax = 0;
                                for (int k = i; k < end; ++k) {
                                    if (decimalPlaces.get(k) > decimalMax) decimalMax = decimalPlaces.get(k);
                                }

                                long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);
                                long startTime = System.nanoTime();

                                long[] scaledInts = sprintz(scaledInt);

                                // 只在octadSize=8时考虑固定方案
                                if (octadSize == 8) {
                                    // 计算固定方案成本
                                    FixedPackResult fixedResult = calculateFixedPackCost(scaledInts, scaledInts.length);

                                    // 计算RL方案成本
                                    int remainder = scaledInts.length % octadSize;
                                    int padding = (remainder == 0) ? 0 : octadSize - remainder;
                                    long[] padded = new long[scaledInts.length + padding];
                                    System.arraycopy(scaledInts, 0, padded, 0, scaledInts.length);
                                    if (padding > 0) Arrays.fill(padded, scaledInts.length, padded.length, 0L);

                                    int octadCount = padded.length / octadSize;
                                    List<Integer> bitWidths = new ArrayList<>(octadCount);

                                    for (int si = 0; si < padded.length; si += octadSize) {
                                        long maxInOctad = 0;
                                        for (int sj = si; sj < si + octadSize; ++sj) {
                                            long v = padded[sj];
                                            if (v > maxInOctad) maxInOctad = v;
                                        }
                                        int bitWidth = 0;
                                        if (maxInOctad > 0) {
                                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
                                        }
                                        bitWidths.add(bitWidth);
                                    }

                                    PackingResult res = packOctads(bitWidths, model, null, octadSize, padded, scaledInts.length);
                                    long duration = System.nanoTime() - startTime;
                                    hybridTime += duration;

                                    // 选择更优方案
                                    long rlCostBits = res.compressedData.length * 8;
                                    long fixedCostBits = fixedResult.totalCost;

                                    if (rlCostBits <= fixedCostBits) {
                                        hybridCost += rlCostBits;
                                        rlBetterCount++;
                                    } else {
                                        hybridCost += fixedCostBits;
                                        fixedBetterCount++;
                                    }
                                } else {
                                    // 对于非8的octadSize，只使用RL方案
                                    int remainder = scaledInts.length % octadSize;
                                    int padding = (remainder == 0) ? 0 : octadSize - remainder;
                                    long[] padded = new long[scaledInts.length + padding];
                                    System.arraycopy(scaledInts, 0, padded, 0, scaledInts.length);
                                    if (padding > 0) Arrays.fill(padded, scaledInts.length, padded.length, 0L);

                                    int octadCount = padded.length / octadSize;
                                    List<Integer> bitWidths = new ArrayList<>(octadCount);

                                    for (int si = 0; si < padded.length; si += octadSize) {
                                        long maxInOctad = 0;
                                        for (int sj = si; sj < si + octadSize; ++sj) {
                                            long v = padded[sj];
                                            if (v > maxInOctad) maxInOctad = v;
                                        }
                                        int bitWidth = 0;
                                        if (maxInOctad > 0) {
                                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
                                        }
                                        bitWidths.add(bitWidth);
                                    }

                                    PackingResult res = packOctads(bitWidths, model, null, octadSize, padded, scaledInts.length);
                                    long duration = System.nanoTime() - startTime;
                                    hybridTime += duration;
                                    hybridCost += (res.compressedData.length * 8);
                                }
                            }
                        }

                        hybridCost /= time_of_repeat;
                        hybridTime /= time_of_repeat;

                        double hybrid_ratio = (double) hybridCost / (double) (numbers.size() * 64);
                        double hybridTime_throughput = (double) (numbers.size() * 8000) / (double) hybridTime;

                        System.out.println("  Compression ratio: " + (1.0/hybrid_ratio));
                        if (octadSize == 8) {
                            System.out.println("  RL better in " + rlBetterCount + " cases, Fixed better in " + fixedBetterCount + " cases");
                        }

                        writer.write(String.valueOf(octadSize) + ",");
                        writer.write(entry.toString() + ",");
                        writer.write("SPRINTZ-RL-FixedHybrid,");
                        writer.write(String.valueOf(hybridTime_throughput) + ",");
                        writer.write(String.valueOf(numbers.size()) + ",");
                        writer.write(String.valueOf(hybridCost) + ",");
                        writer.write(String.valueOf(hybrid_ratio) + ",");
                        writer.write(String.valueOf(rlBetterCount) + ",");
                        writer.write(String.valueOf(fixedBetterCount) + "\n");
                    }
                } catch (IOException e) {
                    System.err.println("Error writing output file for " + fname);
                }
            }
        } catch (IOException e) {
            System.err.println("Error iterating directory: " + directory);
        }
    }

    public static void main(String[] args) {
        String trainDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_rl";

        int epochs = 20;

        if (args.length >= 1) trainDir = args[0];
        if (args.length >= 2) dataDir = args[1];
        if (args.length >= 3) outDir = args[2];

        RLDecisionModel model = new RLDecisionModel();
        if (!trainDir.isEmpty()) {
            model = trainModelFromDirectory(epochs, trainDir);
        } else {
            System.err.println("No training directory given. Using randomly initialized RL model.");
        }

        if (!dataDir.isEmpty()) {
            performanceTest(model, dataDir, outDir);
        } else {
            System.err.println("No data directory provided for performanceTest. Exiting.");
        }
    }

    static void performanceTestVaryPackSize(RLDecisionModel model, String directory, String outputDirStr) {
        System.out.println("\nPerformance Testing with varying octadSize...");
        Path outdir = Paths.get(outputDirStr);
        try {
            if (!Files.exists(outdir)) Files.createDirectories(outdir);
        } catch (IOException e) {
            System.err.println("Cannot create output dir: " + outputDirStr);
            return;
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path entry : ds) {
                if (!Files.isRegularFile(entry)) continue;
                String fname = entry.getFileName().toString();
                if (!BenchmarkDatasetFilter.includeDatasetFile(fname)) continue;

                System.out.println("Processing " + fname + "...");
                List<String> numbers = new ArrayList<>();
                List<Integer> decimalPlaces = new ArrayList<>();

                try (BufferedReader br = Files.newBufferedReader(entry)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] tokens = line.split(",");
                        for (String token : tokens) {
                            String t = trimStr(token);
                            if (!t.isEmpty()) {
                                numbers.add(t);
                                int dec = 0;
                                int pos = t.indexOf('.');
                                if (pos != -1) dec = t.length() - pos - 1;
                                decimalPlaces.add(dec);
                            }
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Cannot open " + entry.toString());
                    continue;
                }

                if (numbers.isEmpty()) continue;

                Path outPath = outdir.resolve(fname);
                try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
                    writer.write("Pack Size,Input Direction,Encoding Algorithm,Encoding Time,Points,Compressed Size,Compression Ratio,RL Better Count,Fixed Better Count\n");

                    int time_of_repeat = 10;

                    for (int octadSizeExp = 0; octadSizeExp <= 9; octadSizeExp++) {
                        int octadSize = (int) Math.pow(2, octadSizeExp);
                        System.out.println("Testing octadSize = " + octadSize);

                        long hybridCost = 0;
                        long hybridTime = 0;
                        int rlBetterCount = 0;
                        int fixedBetterCount = 0;

                        for (int rep = 0; rep < time_of_repeat; ++rep) {
                            for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                                int end = Math.min(numbers.size(), i + CHUNK_SIZE);
                                if (end - i <= 2) continue;

                                List<String> chunkNumbers = numbers.subList(i, end);
                                int decimalMax = 0;
                                for (int k = i; k < end; ++k) {
                                    if (decimalPlaces.get(k) > decimalMax) decimalMax = decimalPlaces.get(k);
                                }

                                long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);
                                long startTime = System.nanoTime();

                                long[] scaledInts = sprintz(scaledInt);

                                // 只在octadSize=8时考虑固定方案
                                if (octadSize == 8) {
                                    // 计算固定方案成本
                                    FixedPackResult fixedResult = calculateFixedPackCost(scaledInts, scaledInts.length);

                                    // 计算RL方案成本
                                    int remainder = scaledInts.length % octadSize;
                                    int padding = (remainder == 0) ? 0 : octadSize - remainder;
                                    long[] padded = new long[scaledInts.length + padding];
                                    System.arraycopy(scaledInts, 0, padded, 0, scaledInts.length);
                                    if (padding > 0) Arrays.fill(padded, scaledInts.length, padded.length, 0L);

                                    int octadCount = padded.length / octadSize;
                                    List<Integer> bitWidths = new ArrayList<>(octadCount);

                                    for (int si = 0; si < padded.length; si += octadSize) {
                                        long maxInOctad = 0;
                                        for (int sj = si; sj < si + octadSize; ++sj) {
                                            long v = padded[sj];
                                            if (v > maxInOctad) maxInOctad = v;
                                        }
                                        int bitWidth = 0;
                                        if (maxInOctad > 0) {
                                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
                                        }
                                        bitWidths.add(bitWidth);
                                    }

                                    PackingResult res = packOctads(bitWidths, model, null, octadSize, padded, scaledInts.length);
                                    long duration = System.nanoTime() - startTime;
                                    hybridTime += duration;

                                    // 选择更优方案
                                    long rlCostBits = res.compressedData.length * 8;
                                    long fixedCostBits = fixedResult.totalCost;

                                    if (rlCostBits <= fixedCostBits) {
                                        hybridCost += rlCostBits;
                                        rlBetterCount++;
                                    } else {
                                        hybridCost += fixedCostBits;
                                        fixedBetterCount++;
                                    }
                                } else {
                                    // 对于非8的octadSize，只使用RL方案
                                    int remainder = scaledInts.length % octadSize;
                                    int padding = (remainder == 0) ? 0 : octadSize - remainder;
                                    long[] padded = new long[scaledInts.length + padding];
                                    System.arraycopy(scaledInts, 0, padded, 0, scaledInts.length);
                                    if (padding > 0) Arrays.fill(padded, scaledInts.length, padded.length, 0L);

                                    int octadCount = padded.length / octadSize;
                                    List<Integer> bitWidths = new ArrayList<>(octadCount);

                                    for (int si = 0; si < padded.length; si += octadSize) {
                                        long maxInOctad = 0;
                                        for (int sj = si; sj < si + octadSize; ++sj) {
                                            long v = padded[sj];
                                            if (v > maxInOctad) maxInOctad = v;
                                        }
                                        int bitWidth = 0;
                                        if (maxInOctad > 0) {
                                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
                                        }
                                        bitWidths.add(bitWidth);
                                    }

                                    PackingResult res = packOctads(bitWidths, model, null, octadSize, padded, scaledInts.length);
                                    long duration = System.nanoTime() - startTime;
                                    hybridTime += duration;
                                    hybridCost += (res.compressedData.length * 8);
                                }
                            }
                        }

                        hybridCost /= time_of_repeat;
                        hybridTime /= time_of_repeat;

                        double hybrid_ratio = (double) hybridCost / (double) (numbers.size() * 64);
                        double hybridTime_throughput = (double) (numbers.size() * 8000) / (double) hybridTime;

                        System.out.println("  Compression ratio: " + (1.0/hybrid_ratio));
                        if (octadSize == 8) {
                            System.out.println("  RL better in " + rlBetterCount + " cases, Fixed better in " + fixedBetterCount + " cases");
                        }

                        writer.write(String.valueOf(octadSize) + ",");
                        writer.write(entry.toString() + ",");
                        writer.write("SPRINTZ-RL-FixedHybrid,");
                        writer.write(String.valueOf(hybridTime_throughput) + ",");
                        writer.write(String.valueOf(numbers.size()) + ",");
                        writer.write(String.valueOf(hybridCost) + ",");
                        writer.write(String.valueOf(hybrid_ratio) + ",");
                        writer.write(String.valueOf(rlBetterCount) + ",");
                        writer.write(String.valueOf(fixedBetterCount) + "\n");
                    }
                } catch (IOException e) {
                    System.err.println("Error writing output file for " + fname);
                }
            }
        } catch (IOException e) {
            System.err.println("Error iterating directory: " + directory);
        }
    }
    @Test
    public void TestVarOctadSize() {
        String trainDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_RL_vary_pack_size";

        int epochs = 100;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir);
        performanceTestVaryPackSize(model, dataDir, outDir);
    }

    // ========== 测试不同chunk size的方法（修改版） ==========
    static void performanceTestVariableChunkSize(RLDecisionModel model, String directory, String outputDirStr) {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        Path outdir = Paths.get(outputDirStr);
        try {
            if (!Files.exists(outdir)) Files.createDirectories(outdir);
        } catch (IOException e) {
            System.err.println("Cannot create output dir: " + outputDirStr);
            return;
        }

        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8, 1024*8};

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path entry : ds) {
                if (!Files.isRegularFile(entry)) continue;
                String fname = entry.getFileName().toString();
                if (!BenchmarkDatasetFilter.includeDatasetFile(fname)) continue;

                System.out.println("Processing " + fname + " with variable chunk sizes...");
                List<String> numbers = new ArrayList<>();
                List<Integer> decimalPlaces = new ArrayList<>();

                try (BufferedReader br = Files.newBufferedReader(entry)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] tokens = line.split(",");
                        for (String token : tokens) {
                            String t = trimStr(token);
                            if (!t.isEmpty()) {
                                numbers.add(t);
                                int dec = 0;
                                int pos = t.indexOf('.');
                                if (pos != -1) dec = t.length() - pos - 1;
                                decimalPlaces.add(dec);
                            }
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Cannot open " + entry.toString());
                    continue;
                }

                if (numbers.isEmpty()) continue;

                Path outPath = outdir.resolve(fname.replace(".", "_chunksize_test."));
                try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
                    writer.write("m,Pack Size,Input Direction,Encoding Algorithm,Encoding Time,Points,Compressed Size,Compression Ratio,RL Better Count,Fixed Better Count\n");

                    int time_of_repeat = 10;
                    int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);
                    int octadSize = 8;

                    // 分批处理，每1024个元素一批进行scaling
                    int batchSize = 1024;
                    List<long[]> batches = new ArrayList<>();

                    for (int i = 0; i < numbers.size(); i += batchSize) {
                        int end = Math.min(numbers.size(), i + batchSize);
                        List<String> batch = numbers.subList(i, end);
                        long[] scaledBatch = scaleNumbers(batch, decimalMax);
                        batches.add(scaledBatch);
                    }

                    // 计算总长度并拼接所有批次的结果
                    int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
                    long[] scaledInts_all = new long[totalLength];

                    int currentIndex = 0;
                    for (long[] batch : batches) {
                        System.arraycopy(batch, 0, scaledInts_all, currentIndex, batch.length);
                        currentIndex += batch.length;
                    }

                    for (int chunkSize : chunkSizes) {
                        System.out.println("Testing chunk size: " + chunkSize);

                        long hybridCost = 0;
                        long hybridTime = 0;
                        int rlBetterCount = 0;
                        int fixedBetterCount = 0;

                        for (int rep = 0; rep < time_of_repeat; ++rep) {
                            for (int i = 0; i < numbers.size(); i += chunkSize) {
                                int end = Math.min(i + chunkSize, scaledInts_all.length);
                                long[] chunkData = new long[end - i];
                                System.arraycopy(scaledInts_all, i, chunkData, 0, end - i);

                                long startTime = System.nanoTime();

                                long[] scaledInts = sprintz(chunkData);

                                // 计算固定方案成本
                                FixedPackResult fixedResult = calculateFixedPackCost(scaledInts, scaledInts.length);

                                // 计算RL方案成本
                                int remainder = scaledInts.length % octadSize;
                                int padding = (remainder == 0) ? 0 : octadSize - remainder;
                                long[] padded = new long[scaledInts.length + padding];
                                System.arraycopy(scaledInts, 0, padded, 0, scaledInts.length);
                                if (padding > 0) Arrays.fill(padded, scaledInts.length, padded.length, 0L);

                                int octadCount = padded.length / octadSize;
                                List<Integer> bitWidths = new ArrayList<>(octadCount);

                                for (int si = 0; si < padded.length; si += octadSize) {
                                    long maxInOctad = 0;
                                    for (int sj = si; sj < si + octadSize; ++sj) {
                                        long v = padded[sj];
                                        if (v > maxInOctad) maxInOctad = v;
                                    }
                                    int bitWidth = 0;
                                    if (maxInOctad > 0) {
                                        bitWidth = 64 - Long.numberOfLeadingZeros(maxInOctad);
                                    }
                                    bitWidths.add(bitWidth);
                                }

                                PackingResult res = packOctads(bitWidths, model, null, octadSize, padded, scaledInts.length);
                                long duration = System.nanoTime() - startTime;
                                hybridTime += duration;

                                // 选择更优方案
                                long rlCostBits = res.compressedData.length * 8L;
                                long fixedCostBits = fixedResult.totalCost;

                                if (rlCostBits <= fixedCostBits) {
                                    hybridCost += rlCostBits;
                                    rlBetterCount++;
                                } else {
                                    hybridCost += fixedCostBits;
                                    fixedBetterCount++;
                                }
                            }
                        }

                        hybridCost /= time_of_repeat;
                        hybridTime /= time_of_repeat;

                        double hybrid_ratio = (double) hybridCost / (double) (numbers.size() * 64);
                        double hybridTime_throughput = (double) (numbers.size() * 8000) / (double) hybridTime;

                        System.out.println("  Hybrid: RL better " + rlBetterCount + " times, Fixed better " + fixedBetterCount + " times");
                        System.out.println("  Compression ratio: " + (1.0/hybrid_ratio));

                        writer.write(String.valueOf(chunkSize) + ",");
                        writer.write(String.valueOf(octadSize) + ",");
                        writer.write(entry.toString() + ",");
                        writer.write("sprintz-RL-FixedHybrid,");
                        writer.write(String.valueOf(hybridTime_throughput) + ",");
                        writer.write(String.valueOf(numbers.size()) + ",");
                        writer.write(String.valueOf(hybridCost) + ",");
                        writer.write(String.valueOf(hybrid_ratio) + ",");
                        writer.write(String.valueOf(rlBetterCount) + ",");
                        writer.write(String.valueOf(fixedBetterCount) + "\n");
                    }
                } catch (IOException e) {
                    System.err.println("Error writing output file for " + fname);
                }
            }
        } catch (IOException e) {
            System.err.println("Error iterating directory: " + directory);
        }
    }

    @Test
    public void TestVariableChunkSize() {
        String trainDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_RL_vary_m";

        int epochs = 100;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir);
        performanceTestVariableChunkSize(model, dataDir, outDir);
    }
}