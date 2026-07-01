package org.apache.iotdb.tsfile.encoding;

import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class EfficientOctadPackingMLPImproveSprintz {

    static final int CHUNK_SIZE = 1024;
    static final int INPUT_DIM = 16;  // 从12增加到16维特征
    static final int HIDDEN_DIM = 64;

    // Baseline配置
    static final int BASELINE_PACK_SIZE = 8;
    static final int BASELINE_BITWIDTH_BITS = 6;

    // ========== Pack / Result / DecisionPoint ==========
    static class Pack {
        int size = 0;
        int maxBitWidth = 0;
        int startIndex = 0;
        List<Integer> indices = new ArrayList<>();
        List<Integer> bitWidths = new ArrayList<>();

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

        long dataCost(long pack_size) {
            return pack_size * size * (long) maxBitWidth;
        }

        int logSize() {
            if (size <= 0) return 0;
            return 32 - Integer.numberOfLeadingZeros(size);
        }

        float efficiency(int packSize) {
            if (size == 0 || packSize == 0) return 0.0f;
            return (float) (size * maxBitWidth) / (packSize * 64.0f);
        }

        int mergedBitWidth(int newBitWidth) {
            return Math.max(maxBitWidth, newBitWidth);
        }
    }

    static class PackingResult {
        int packCount = 0;
        long dataCostA = 0;
        int bitWidthCostB = 0;
        int packSizeCostC = 0;
        long totalCost = 0;
        List<Pack> packs = new ArrayList<>();
        byte[] compressedData;
        boolean isBaseline = false;

        void calculateCost(int maxLog) {
            bitWidthCostB = 6 * packCount;
            packSizeCostC = packCount * maxLog;
            totalCost = dataCostA + bitWidthCostB + packSizeCostC;
        }

        @Override
        public String toString() {
            return String.format("Packs: %d, Cost: %d (A=%d, B=%d, C=%d), Strategy: %s",
                    packCount, totalCost, dataCostA, bitWidthCostB, packSizeCostC,
                    isBaseline ? "Baseline" : "RL");
        }

        float compressionRatio(long originalSize) {
            if (originalSize == 0) return 0.0f;
            return 1.0f - (float) totalCost / originalSize;
        }

        float averagePackSize() {
            if (packCount == 0) return 0.0f;
            float totalSize = 0;
            for (Pack pack : packs) {
                totalSize += pack.size;
            }
            return totalSize / packCount;
        }
    }

    static class DecisionPoint {
        int currentPackSize;
        int currentPackMaxB;
        int newOctadB;
        int packCount;
        int currentMaxLog;
        float packEfficiency;
        float remainingRatio;
        float packSizeRatio;
        boolean action;
        float probability;

        DecisionPoint(int cps, int cpm, int nob, int pc, int cml, float pe, float rr, float psr, boolean a, float p) {
            currentPackSize = cps;
            currentPackMaxB = cpm;
            newOctadB = nob;
            packCount = pc;
            currentMaxLog = cml;
            packEfficiency = pe;
            remainingRatio = rr;
            packSizeRatio = psr;
            action = a;
            probability = p;
        }
    }

    // ========== 自适应阈值模型 ==========
    static class AdaptiveThresholdModel {
        private float threshold = 0.5f;
        private float learningRate = 0.01f;
        private int windowSize = 100;
        private Queue<Boolean> decisions = new LinkedList<>();
        private Queue<Float> rewards = new LinkedList<>();

        boolean shouldMerge(float probability, float currentEfficiency, int currentSize, int packSize) {
            float dynamicThreshold = threshold;

            float fillRatio = (float) currentSize / packSize;
            if (fillRatio < 0.3f) {
                dynamicThreshold *= 0.7f;
            } else if (fillRatio > 0.8f) {
                dynamicThreshold *= 1.3f;
            }

            if (currentEfficiency > 0.9f) {
                dynamicThreshold *= 0.8f;
            }

            return probability > dynamicThreshold;
        }

        void update(boolean decision, float reward) {
            decisions.add(decision);
            rewards.add(reward);

            if (decisions.size() > windowSize) {
                decisions.poll();
                rewards.poll();
            }

            float avgReward = 0.0f;
            for (float r : rewards) avgReward += r;
            avgReward /= rewards.size();

            if (avgReward < 0.5f) {
                threshold += learningRate * (0.5f - avgReward);
            }
            threshold = Math.max(0.3f, Math.min(0.7f, threshold));
        }
    }

    // ========== 在线优化器 ==========
    static class OnlineOptimizer {
        private float[] lastFeatures;
        private boolean lastDecision;
        private long lastCost;

        void recordDecision(float[] feat, boolean decision, long rlCost, long baselineCost) {
            lastFeatures = feat.clone();
            lastDecision = decision;
            lastCost = Math.min(rlCost, baselineCost);
        }

//        boolean optimizeNextDecision(float[] currentFeat, float baseProbability) {
//            if (lastFeatures == null) {
//                return baseProbability > 0.5f;
//            }
//
//            float similarity = computeSimilarity(lastFeatures, currentFeat);
//
//            if (similarity > 0.8f && lastCost < baselineCost * 0.9f) {
//                return lastDecision;
//            }
//
//            return baseProbability > 0.5f;
//        }

        private float computeSimilarity(float[] feat1, float[] feat2) {
            float dot = 0.0f;
            float norm1 = 0.0f;
            float norm2 = 0.0f;

            for (int i = 0; i < feat1.length; i++) {
                dot += feat1[i] * feat2[i];
                norm1 += feat1[i] * feat1[i];
                norm2 += feat2[i] * feat2[i];
            }

            return dot / (float)(Math.sqrt(norm1) * Math.sqrt(norm2));
        }
    }

    // ========== 改进的RL模型 ==========
    static class RLDecisionModel implements Serializable {
        private static final long serialVersionUID = 1L;

        float[] W1;
        float[] b1;
        float[] W2;
        float b2;

        float explorationRate = 0.3f;
        float learningRate = 0.001f;
        float lambda = 0.001f;
        float gradientClip = 5.0f;

        transient Random rng;
        float[] adamM1, adamM2;
        float beta1 = 0.9f, beta2 = 0.999f, epsilon = 1e-8f;
        int t = 0;

        RLDecisionModel() {
            rng = new Random();
            W1 = new float[HIDDEN_DIM * INPUT_DIM];
            b1 = new float[HIDDEN_DIM];
            W2 = new float[HIDDEN_DIM];
            adamM1 = new float[W1.length + b1.length + W2.length + 1];
            adamM2 = new float[W1.length + b1.length + W2.length + 1];

            float stddev1 = (float) (1.0 / Math.sqrt(INPUT_DIM));
            float stddev2 = (float) (1.0 / Math.sqrt(HIDDEN_DIM));

            for (int i = 0; i < W1.length; ++i) W1[i] = randUniform(-stddev1, stddev1);
            for (int i = 0; i < b1.length; ++i) b1[i] = randUniform(-stddev1, stddev1);
            for (int i = 0; i < W2.length; ++i) W2[i] = randUniform(-stddev2, stddev2);
            b2 = randUniform(-stddev2, stddev2);
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            rng = new Random();
        }

        private float randUniform(float a, float b) {
            return a + rng.nextFloat() * (b - a);
        }

        static float leakyRelu(float x) {
            return x > 0.0f ? x : 0.01f * x;
        }

        static float leakyReluDeriv(float x) {
            return x > 0.0f ? 1.0f : 0.01f;
        }

        static float sigmoid(float x) {
            if (x >= 0) {
                double z = Math.exp(-x);
                return (float) (1.0 / (1.0 + z));
            } else {
                double z = Math.exp(x);
                return (float) (z / (1.0 + z));
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
                float hval = leakyRelu(z);
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
                    float hval = leakyRelu(z);
                    z2 += W2[h] * hval;
                }
            }
            return sigmoid(z2);
        }

        float forwardProb(float[] feat) {
            return forwardProb(feat, null, null);
        }

        float train(List<DecisionPoint> decisions, float reward, int totalOctads, int packSize) {
            if (decisions == null || decisions.isEmpty()) return 0.0f;

            explorationRate = Math.max(0.05f, explorationRate * 0.995f);

            float[] dW1 = new float[W1.length];
            float[] db1 = new float[b1.length];
            float[] dW2 = new float[W2.length];
            float db2 = 0.0f;

            float totalLoss = 0.0f;

            float[] feat = new float[INPUT_DIM];
            float[] hidden = new float[HIDDEN_DIM];
            float[] z1 = new float[HIDDEN_DIM];

            for (DecisionPoint dp : decisions) {
                // 特征提取
                feat[0] = dp.currentPackSize / 100.0f;
                feat[1] = dp.currentPackMaxB / 64.0f;
                feat[2] = dp.newOctadB / 64.0f;
                feat[3] = Math.abs(dp.newOctadB - dp.currentPackMaxB) / 64.0f;
                feat[4] = dp.packCount / 100.0f;
                feat[5] = dp.currentMaxLog / 10.0f;
                feat[6] = dp.packEfficiency;
                feat[7] = dp.remainingRatio;
                feat[8] = (float) dp.currentPackSize * dp.currentPackMaxB / (packSize * 64.0f);
                feat[9] = (float) Math.max(dp.newOctadB, dp.currentPackMaxB) / 64.0f;
                feat[10] = packSize / 1024.0f;
                feat[11] = dp.packSizeRatio;
                // 新增特征
                feat[12] = (float) dp.newOctadB / (dp.currentPackMaxB + 1.0f);
                feat[13] = dp.currentPackSize / (float) packSize;
                feat[14] = totalOctads / 1000.0f;
                feat[15] = 1.0f - dp.remainingRatio; // 处理进度

                float p = forwardProb(feat, hidden, z1);

                float pClipped = Math.min(Math.max(p, 1e-7f), 1.0f - 1e-7f);

                float piA = dp.action ? pClipped : (1.0f - pClipped);
                if (piA <= 0.0f) {
                    piA = 1e-7f;
                }

                float entropy = -pClipped * (float) Math.log(pClipped) - (1 - pClipped) * (float) Math.log(1 - pClipped);
                float l2Reg = computeL2Reg();

                float lossI = -reward * (float) Math.log(piA) - 0.01f * entropy + lambda * l2Reg;

                if (Float.isNaN(lossI) || Float.isInfinite(lossI)) {
                    lossI = 0.0f;
                }

                totalLoss += lossI;

                float dL_dz2 = reward * (p - (dp.action ? 1.0f : 0.0f));

                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    float grad = dL_dz2 * hidden[h] + 2 * lambda * W2[h];
                    dW2[h] += clipGradient(grad);
                }
                db2 += clipGradient(dL_dz2);

                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    float w2h = W2[h];
                    float dh = dL_dz2 * w2h;
                    float dReLU = leakyReluDeriv(z1[h]);
                    float dZ1 = dh * dReLU;
                    int base = h * INPUT_DIM;
                    for (int j = 0; j < INPUT_DIM; ++j) {
                        float grad = dZ1 * feat[j] + 2 * lambda * W1[base + j];
                        dW1[base + j] += clipGradient(grad);
                    }
                    float gradBias = clipGradient(dZ1);
                    db1[h] += gradBias;
                }
            }

            t++;
            float lr = learningRate * (float) Math.sqrt(1 - Math.pow(beta2, t)) / (1 - (float) Math.pow(beta1, t));

            int paramIdx = 0;

            for (int i = 0; i < W1.length; ++i, ++paramIdx) {
                adamM1[paramIdx] = beta1 * adamM1[paramIdx] + (1 - beta1) * dW1[i];
                adamM2[paramIdx] = beta2 * adamM2[paramIdx] + (1 - beta2) * dW1[i] * dW1[i];
                float mHat = adamM1[paramIdx] / (1 - (float) Math.pow(beta1, t));
                float vHat = adamM2[paramIdx] / (1 - (float) Math.pow(beta2, t));
                W1[i] -= lr * mHat / (float) (Math.sqrt(vHat) + epsilon);
                W1[i] = clip(W1[i], -5f, 5f);
            }

            for (int i = 0; i < b1.length; ++i, ++paramIdx) {
                adamM1[paramIdx] = beta1 * adamM1[paramIdx] + (1 - beta1) * db1[i];
                adamM2[paramIdx] = beta2 * adamM2[paramIdx] + (1 - beta2) * db1[i] * db1[i];
                float mHat = adamM1[paramIdx] / (1 - (float) Math.pow(beta1, t));
                float vHat = adamM2[paramIdx] / (1 - (float) Math.pow(beta2, t));
                b1[i] -= lr * mHat / (float) (Math.sqrt(vHat) + epsilon);
                b1[i] = clip(b1[i], -5f, 5f);
            }

            for (int i = 0; i < W2.length; ++i, ++paramIdx) {
                adamM1[paramIdx] = beta1 * adamM1[paramIdx] + (1 - beta1) * dW2[i];
                adamM2[paramIdx] = beta2 * adamM2[paramIdx] + (1 - beta2) * dW2[i] * dW2[i];
                float mHat = adamM1[paramIdx] / (1 - (float) Math.pow(beta1, t));
                float vHat = adamM2[paramIdx] / (1 - (float) Math.pow(beta2, t));
                W2[i] -= lr * mHat / (float) (Math.sqrt(vHat) + epsilon);
                W2[i] = clip(W2[i], -5f, 5f);
            }

            adamM1[paramIdx] = beta1 * adamM1[paramIdx] + (1 - beta1) * db2;
            adamM2[paramIdx] = beta2 * adamM2[paramIdx] + (1 - beta2) * db2 * db2;
            float mHat = adamM1[paramIdx] / (1 - (float) Math.pow(beta1, t));
            float vHat = adamM2[paramIdx] / (1 - (float) Math.pow(beta2, t));
            b2 -= lr * mHat / (float) (Math.sqrt(vHat) + epsilon);
            b2 = clip(b2, -5f, 5f);

            return totalLoss;
        }

        private float computeL2Reg() {
            float reg = 0.0f;
            for (float w : W1) reg += w * w;
            for (float w : W2) reg += w * w;
            return reg;
        }

        private float clipGradient(float grad) {
            if (grad > gradientClip) return gradientClip;
            if (grad < -gradientClip) return -gradientClip;
            return grad;
        }

        static float clip(float v, float low, float high) {
            return Math.min(Math.max(v, low), high);
        }

        public void saveModel(String filePath) throws IOException {
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
                oos.writeObject(this);
                System.out.println("Model saved to: " + filePath);
            }
        }

        public static RLDecisionModel loadModel(String filePath) throws IOException, ClassNotFoundException {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
                RLDecisionModel model = (RLDecisionModel) ois.readObject();
                System.out.println("Model loaded from: " + filePath);
                return model;
            }
        }

        public static boolean modelExists(String filePath) {
            File modelFile = new File(filePath);
            return modelFile.exists() && modelFile.length() > 0;
        }
    }

    // ========== 增强的决策函数 ==========
    static boolean enhancedDecision(RLDecisionModel model, float[] feat,
                                    int currentPackSize, int currentMaxB, int newB,
                                    int packSize, float currentEfficiency) {
        float baseProb = model.forwardProb(feat);

        // 规则1: 如果bitwidth完全相同，总是合并
        if (newB == currentMaxB) {
            return true;
        }

        // 规则2: 如果pack接近满，倾向于新建
        float fillRatio = (float) currentPackSize / packSize;
        if (fillRatio > 0.9f) {
            return false;
        }

        // 规则3: 如果bitwidth差异很小，倾向于合并
        int bitwidthDiff = Math.abs(newB - currentMaxB);
        if (bitwidthDiff <= 2 && currentPackSize < packSize * 0.7f) {
            return true;
        }

        // 规则4: 如果效率已经很高，倾向于合并
        if (currentEfficiency > 0.85f) {
            return baseProb > 0.4f;
        }

        // 动态阈值
        float dynamicThreshold = 0.5f;
        if (bitwidthDiff > 8) {
            dynamicThreshold = 0.6f;
        } else if (bitwidthDiff < 4) {
            dynamicThreshold = 0.45f;
        }

        return baseProb > dynamicThreshold;
    }

    // ========== Sprintz编码解码 ==========
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

    // ========== BitWriter/BitReader ==========
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
    }

    // ========== Baseline压缩（固定packsize=8） ==========
    private static byte[] performBaselineCompression(long[] dataArray, int originalLength) {
        int packSize = BASELINE_PACK_SIZE;
        int totalGroups = (originalLength + packSize - 1) / packSize;

        int[] bitWidths = new int[totalGroups];
        for (int i = 0; i < totalGroups; i++) {
            int start = i * packSize;
            int end = Math.min(start + packSize, originalLength);
            long maxVal = 0;
            for (int j = start; j < end; j++) {
                maxVal = Math.max(maxVal, dataArray[j]);
            }
            int bw = 0;
            if (maxVal > 0) {
                bw = 64 - Long.numberOfLeadingZeros(maxVal);
            }
            bitWidths[i] = bw;
        }

        BitWriter writer = new BitWriter();

        for (int bw : bitWidths) {
            int storedBw = bw == 64 ? 63 : bw;
            writer.writeBits(storedBw, BASELINE_BITWIDTH_BITS);
        }

        for (int i = 0; i < totalGroups; i++) {
            int start = i * packSize;
            int end = Math.min(start + packSize, originalLength);
            int bw = bitWidths[i];

            for (int j = start; j < end; j++) {
                if (bw > 0) {
                    long val = dataArray[j];
                    if (bw == 64) {
                        writer.writeBits((val >>> 32) & 0xFFFFFFFFL, 32);
                        writer.writeBits(val & 0xFFFFFFFFL, 32);
                    } else {
                        writer.writeBits(val, bw);
                    }
                }
            }

            for (int j = end; j < start + packSize; j++) {
                if (bw > 0) {
                    writer.writeBits(0L, bw);
                }
            }
        }

        return writer.finish();
    }

    // ========== Baseline解压 ==========
    private static long[] performBaselineDecompression(byte[] compressedData, int originalLength) {
        int packSize = BASELINE_PACK_SIZE;
        int totalGroups = (originalLength + packSize - 1) / packSize;

        BitReader reader = new BitReader(compressedData);
        long[] result = new long[originalLength];
        int resultIndex = 0;

        int[] bitWidths = new int[totalGroups];
        for (int i = 0; i < totalGroups; i++) {
            int storedBw = (int) reader.readBits(BASELINE_BITWIDTH_BITS);
            bitWidths[i] = storedBw == 63 ? 64 : storedBw;
        }

        for (int i = 0; i < totalGroups && resultIndex < originalLength; i++) {
            int bw = bitWidths[i];
            int valuesToRead = Math.min(packSize, originalLength - resultIndex);

            for (int j = 0; j < valuesToRead; j++) {
                if (bw == 0) {
                    result[resultIndex++] = 0;
                } else if (bw == 64) {
                    long high = reader.readBits(32);
                    long low = reader.readBits(32);
                    result[resultIndex++] = (high << 32) | low;
                } else {
                    result[resultIndex++] = reader.readBits(bw);
                }
            }

            int paddingValues = packSize - valuesToRead;
            for (int j = 0; j < paddingValues; j++) {
                if (bw > 0) {
                    reader.readBits(bw);
                }
            }
        }

        return result;
    }

    // ========== RL压缩 ==========
    private static byte[] performBitPackingCompression64_fast(long[] dataArray, List<Pack> packs, int pack_size, int originalLength) throws IOException {
        int totalPacks = packs.size();
        int maxOctadsInAnyPack = 0;
        for (Pack p : packs) if (p.size > maxOctadsInAnyPack) maxOctadsInAnyPack = p.size;

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
            metaWriter.writeBits(pack.bitWidths.get(0), 6);
        }

        byte[] metaBytes = metaWriter.finish();
        dos.write(metaBytes);
        dos.flush();

        BitWriter dataWriter = new BitWriter();

        for (Pack pack : packs) {
            int packMaxBW = pack.maxBitWidth;
            for (int i = 0; i < pack.size; ++i) {
                int originalGroupIndex = pack.indices.get(0);
                int startPos = originalGroupIndex * pack_size;
                for (int j = 0; j < pack_size; ++j) {
                    long val = dataArray[startPos + j];
                    if (packMaxBW == 0) {
                        dataWriter.writeBits(0L, 0);
                    } else {
                        if (packMaxBW == 64) {
                            dataWriter.writeBits(val, 64);
                        } else {
                            long mask = (1L << packMaxBW) - 1L;
                            long masked = val & mask;
                            dataWriter.writeBits(masked, packMaxBW);
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

    // ========== 自适应压缩 ==========
    private static byte[] performAdaptiveCompression(long[] dataArray, List<Pack> packs, int pack_size, int originalLength,
                                                     RLDecisionModel model, List<Integer> bitWidthsList) throws IOException {
        byte[] rlCompressed = performBitPackingCompression64_fast(dataArray, packs, pack_size, originalLength);
        long rlCost = rlCompressed.length * 8L;

        byte[] baselineCompressed = performBaselineCompression(dataArray, originalLength);
        long baselineCost = baselineCompressed.length * 8L;

        boolean useBaseline = baselineCost < rlCost;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeByte(useBaseline ? 1 : 0);

        if (useBaseline) {
            dos.write(baselineCompressed);
        } else {
            dos.write(rlCompressed);
        }

        dos.flush();
        return baos.toByteArray();
    }

    // ========== 自适应解压 ==========
    public static long[] performAdaptiveDecompression(byte[] compressedData, int[] bitWidths, int packSize, int originalLength) {
        if (compressedData == null || compressedData.length == 0) {
            System.err.println("Compressed data is null or empty");
            return new long[0];
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
            DataInputStream dis = new DataInputStream(bais);

            int strategy = dis.readUnsignedByte();

            byte[] actualCompressed = new byte[compressedData.length - 1];
            dis.readFully(actualCompressed);

            if (strategy == 1) {
                return performBaselineDecompression(actualCompressed, originalLength);
            } else {
                return fastDecompress(actualCompressed, bitWidths, packSize, originalLength);
            }

        } catch (Exception e) {
            System.err.println("Adaptive decompression failed: " + e.getMessage());
            e.printStackTrace();
            return new long[0];
        }
    }

    // ========== RL解压 ==========
    public static long[] fastDecompress(byte[] compressedData, int[] bitWidths, int packSize, int originalLength) {
        if (compressedData == null || compressedData.length == 0) {
            return new long[0];
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
            DataInputStream dis = new DataInputStream(bais);

            int totalPacks = dis.readUnsignedByte();
            int bitsForCount = dis.readUnsignedByte();

            int metaStartOffset = 2;
            BitReader metaReader = new BitReader(compressedData, metaStartOffset);

            List<PackInfo> packInfos = new ArrayList<>();

            for (int p = 0; p < totalPacks; ++p) {
                int octadCount = (int) metaReader.readBits(bitsForCount);
                int packBitWidth = (int) metaReader.readBits(6);
                if (packBitWidth == 63) packBitWidth = 64;
                packInfos.add(new PackInfo(octadCount, packBitWidth));
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

                for (int i = 0; i < octadCount; ++i) {
                    for (int j = 0; j < packSize; ++j) {
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

    // ========== 优化的packOctads方法 ==========
    static PackingResult packOctadsOptimized(List<Integer> bitWidths, RLDecisionModel model, List<DecisionPoint> decisionTrace,
                                             int pack_size, long[] dataArray, int originalLength) {
        PackingResult result = new PackingResult();
        Pack currentPack = new Pack();
        int globalMaxLog = 0;
        int packCount = 0;
        int totalOctads = bitWidths.size();

        AdaptiveThresholdModel thresholdModel = new AdaptiveThresholdModel();
        OnlineOptimizer onlineOptimizer = new OnlineOptimizer();

        Random localRng = ThreadLocalRandom.current();

        long baselineCostEstimate = 0;
        for (int bw : bitWidths) {
            baselineCostEstimate += pack_size * 64 + 6; // 每个octad的最大可能成本
        }

        for (int i = 0; i < bitWidths.size(); ++i) {
            int b = bitWidths.get(i);

            if (currentPack.size == 0) {
                currentPack.addOctad(i, b);
            } else if (b == currentPack.maxBitWidth) {
                currentPack.addOctad(i, b);
            } else {
                float currentEfficiency = currentPack.efficiency(pack_size);
                float remainingRatio = (float) (totalOctads - i) / totalOctads;
                float packSizeRatio = (float) currentPack.size / pack_size;

                float[] feat = new float[INPUT_DIM];
                feat[0] = currentPack.size / 100.0f;
                feat[1] = currentPack.maxBitWidth / 64.0f;
                feat[2] = b / 64.0f;
                feat[3] = Math.abs(b - currentPack.maxBitWidth) / 64.0f;
                feat[4] = packCount / 100.0f;
                feat[5] = globalMaxLog / 10.0f;
                feat[6] = currentEfficiency;
                feat[7] = remainingRatio;
                feat[8] = (float) currentPack.size * currentPack.maxBitWidth / (pack_size * 64.0f);
                feat[9] = (float) Math.max(b, currentPack.maxBitWidth) / 64.0f;
                feat[10] = (float) pack_size / 1024.0f;
                feat[11] = packSizeRatio;
                feat[12] = (float) b / (currentPack.maxBitWidth + 1.0f);
                feat[13] = (float) currentPack.size / (pack_size + 1.0f);
                feat[14] = (float) totalOctads / 1000.0f;
                feat[15] = (float) i / totalOctads;

                float probability = model.forwardProb(feat);

                boolean shouldMerge;
                if (localRng.nextFloat() < model.explorationRate) {
                    float temperature = 0.3f * (1.0f + (float) Math.log(pack_size + 1) / 10.0f);
                    float prob = (float) (1.0 / (1.0 + Math.exp(-probability / temperature)));
                    shouldMerge = localRng.nextFloat() < prob;
                } else {
                    shouldMerge = enhancedDecision(model, feat, currentPack.size,
                            currentPack.maxBitWidth, b, pack_size, currentEfficiency);
                    shouldMerge = thresholdModel.shouldMerge(probability, currentEfficiency,
                            currentPack.size, pack_size);
                }

                if (decisionTrace != null) {
                    decisionTrace.add(new DecisionPoint(currentPack.size, currentPack.maxBitWidth, b,
                            packCount, globalMaxLog, currentEfficiency, remainingRatio,
                            packSizeRatio, shouldMerge, probability));
                }

                long rlCostEstimate = currentPack.dataCost(pack_size);
                onlineOptimizer.recordDecision(feat, shouldMerge, rlCostEstimate, baselineCostEstimate);

                if (shouldMerge) {
                    currentPack.addOctad(i, b);
                } else {
                    result.dataCostA += currentPack.dataCost(pack_size);
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
            result.dataCostA += currentPack.dataCost(pack_size);
            int logSize = currentPack.logSize();
            if (logSize > globalMaxLog) globalMaxLog = logSize;
            result.packs.add(currentPack);
            packCount++;
        }

        result.packCount = packCount;
        result.calculateCost(globalMaxLog);

        if (dataArray != null) {
            try {
                result.compressedData = performAdaptiveCompression(dataArray, result.packs, pack_size,
                        originalLength, model, bitWidths);
            } catch (IOException e) {
                System.err.println("Compression failed: " + e.getMessage());
                result.compressedData = null;
            }
        }

        return result;
    }

    // ========== 从目录生成训练数据 ==========
    static List<List<Long>> generateTrainingDataFromDirectory(String directoryPath) {
        List<List<Long>> sequences = new ArrayList<>();

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directoryPath))) {
            for (Path entry : ds) {
                if (!Files.isRegularFile(entry)) continue;
                String fname = entry.getFileName().toString();
                if (!BenchmarkDatasetFilter.includeDatasetFile(fname)) continue;
                if (!fname.toLowerCase().endsWith(".csv")) continue;

                System.out.println("Reading training data from: " + fname);

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

                int totalDataPoints = numbers.size();
                int dataToTake = totalDataPoints / 10;
                if (dataToTake < CHUNK_SIZE) {
                    dataToTake = Math.min(CHUNK_SIZE, totalDataPoints);
                }
                dataToTake = (dataToTake / CHUNK_SIZE) * CHUNK_SIZE;

                if (dataToTake < CHUNK_SIZE) {
                    System.out.println("Skipping " + fname + ": not enough data points");
                    continue;
                }

                System.out.println("Taking " + dataToTake + " data points from " + fname);

                List<String> selectedNumbers = numbers.subList(0, dataToTake);
                List<Integer> selectedDecimalPlaces = decimalPlaces.subList(0, dataToTake);

                for (int i = 0; i < selectedNumbers.size(); i += CHUNK_SIZE) {
                    int end = Math.min(i + CHUNK_SIZE, selectedNumbers.size());
                    if (end - i == CHUNK_SIZE) {
                        List<String> chunkNumbers = selectedNumbers.subList(i, end);
                        List<Integer> chunkDecimalPlaces = selectedDecimalPlaces.subList(i, end);

                        int decimalMax = 0;
                        for (int dec : chunkDecimalPlaces) {
                            if (dec > decimalMax) decimalMax = dec;
                        }

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);
                        long[] sprintzEncoded = sprintz(scaledInt);

                        List<Long> encodedList = new ArrayList<>();
                        for (long val : sprintzEncoded) {
                            encodedList.add(val);
                        }

                        sequences.add(encodedList);

                        if (sequences.size() >= 50) {
                            break;
                        }
                    }
                }

                System.out.println("Generated " + sequences.size() + " sequences from " + fname);
            }
        } catch (IOException e) {
            System.err.println("Error iterating directory: " + directoryPath);
            e.printStackTrace();
        }

        System.out.println("Total training sequences generated: " + sequences.size());
        return sequences;
    }

    // ========== 计算bitWidths ==========
    static List<Integer> computeBitWidthsForPackSize(List<Long> data, int packSize) {
        List<Integer> bitWidths = new ArrayList<>();
        for (int i = 0; i < data.size(); i += packSize) {
            int end = Math.min(i + packSize, data.size());
            long maxVal = 0;
            for (int j = i; j < end; j++) {
                maxVal = Math.max(maxVal, data.get(j));
            }
            int bw = 0;
            if (maxVal > 0) {
                bw = 64 - Long.numberOfLeadingZeros(maxVal);
            }
            bitWidths.add(bw);
        }
        return bitWidths;
    }

    // ========== 训练函数 ==========
    static RLDecisionModel trainModelMultiPackSize(int epochs, String dataDirectoryPath, String modelSavePath) {
        if (RLDecisionModel.modelExists(modelSavePath)) {
            try {
                System.out.println("Loading existing model from: " + modelSavePath);
                return RLDecisionModel.loadModel(modelSavePath);
            } catch (Exception e) {
                System.err.println("Failed to load model, starting new training: " + e.getMessage());
            }
        }

        System.err.println("Training RL model with multiple pack sizes...");
        RLDecisionModel model = new RLDecisionModel();

        List<List<Long>> sequences = generateTrainingDataFromDirectory(dataDirectoryPath);
        if (sequences.isEmpty()) {
            System.err.println("No data loaded. Returning initial model.");
            return model;
        }

        System.err.println("Loaded " + sequences.size() + " sequences for training");

        List<Experience> replayBuffer = new ArrayList<>();
        int bufferCapacity = 10000;
        int batchSize = 32;

        int[] trainPackSizes = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512};

        for (int epoch = 1; epoch <= epochs; ++epoch) {
            long startTime = System.nanoTime();
            float totalReward = 0.0f;
            float totalLoss = 0.0f;
            int processedSequences = 0;

            float progress = (float) epoch / epochs;
            int startPackSizeIdx = Math.max(0, (int) (trainPackSizes.length * (1.0f - progress) * 0.3f));

            for (List<Long> data : sequences) {
                int packSize;
                if (Math.random() < 0.7f) {
                    int idx = startPackSizeIdx + (int) (Math.random() * (trainPackSizes.length - startPackSizeIdx));
                    packSize = trainPackSizes[Math.min(idx, trainPackSizes.length - 1)];
                } else {
                    packSize = trainPackSizes[ThreadLocalRandom.current().nextInt(trainPackSizes.length)];
                }

                List<Integer> bitWidths = computeBitWidthsForPackSize(data, packSize);

                List<DecisionPoint> decisionTrace = new ArrayList<>();

                long originalSize = data.size() * 64L;

                PackingResult result = packOctadsOptimized(bitWidths, model, decisionTrace, packSize, null, 0);

                float reward = computeAdaptiveReward(result, originalSize, packSize, bitWidths.size());

                totalReward += reward;

                for (DecisionPoint dp : decisionTrace) {
                    replayBuffer.add(new Experience(dp, reward));
                    if (replayBuffer.size() > bufferCapacity) {
                        replayBuffer.remove(0);
                    }
                }

                if (replayBuffer.size() >= batchSize) {
                    Collections.shuffle(replayBuffer);
                    List<DecisionPoint> batchDecisions = new ArrayList<>();
                    float batchReward = 0.0f;
                    for (int i = 0; i < Math.min(batchSize, replayBuffer.size()); i++) {
                        Experience exp = replayBuffer.get(i);
                        batchDecisions.add(exp.decision);
                        batchReward += exp.reward;
                    }
                    batchReward /= batchSize;

                    float loss = model.train(batchDecisions, batchReward, bitWidths.size(), packSize);
                    totalLoss += loss;
                }

                processedSequences++;
            }

            long durationMs = (System.nanoTime() - startTime) / 1_000_000L;

            if (epoch % 10 == 0 || epoch == 1 || epoch == epochs) {
                System.out.printf("Epoch %d: Avg Reward = %.6f, Avg Loss = %.6f, Exploration Rate = %.4f, Time = %d ms%n",
                        epoch,
                        totalReward / processedSequences,
                        totalLoss / processedSequences,
                        model.explorationRate,
                        durationMs);

                if (epoch % 10 == 0) {
                    try {
                        model.saveModel(modelSavePath + "_epoch_" + epoch + ".model");
                    } catch (IOException e) {
                        System.err.println("Failed to save model at epoch " + epoch + ": " + e.getMessage());
                    }
                }
            }
        }

        try {
            model.saveModel(modelSavePath);
        } catch (IOException e) {
            System.err.println("Failed to save final model: " + e.getMessage());
        }

        return model;
    }

    static float computeAdaptiveReward(PackingResult result, long originalSize, int packSize, int totalGroups) {
        float compressionRatio = result.compressionRatio(originalSize);

        float optimalPackCount;
        if (packSize <= 8) {
            optimalPackCount = totalGroups * 0.8f;
        } else if (packSize <= 64) {
            optimalPackCount = totalGroups * 0.6f;
        } else {
            optimalPackCount = totalGroups * 0.4f;
        }

        float packCountPenalty = Math.abs(result.packCount - optimalPackCount) / totalGroups;

        float avgEfficiency = 0.0f;
        for (Pack pack : result.packs) {
            avgEfficiency += pack.efficiency(packSize);
        }
        if (result.packCount > 0) {
            avgEfficiency /= result.packCount;
        }

        float reward = compressionRatio * 0.6f +
                avgEfficiency * 0.3f -
                packCountPenalty * 0.1f;

        return Math.max(0.0f, reward);
    }

    static class Experience {
        DecisionPoint decision;
        float reward;

        Experience(DecisionPoint decision, float reward) {
            this.decision = decision;
            this.reward = reward;
        }
    }

    // ========== 性能测试 ==========
    static void performanceTestPackVarPackSize(RLDecisionModel model, String directory, String outputDirStr) {
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
                    String[] head = {
                            "Input Direction",
                            "Encoding Algorithm",
                            "Encoding Time",
                            "Points",
                            "Compressed Size",
                            "Pack Size",
                            "Compression Ratio",
                            "Selected Scheme"
                    };
                    writer.write(String.join(",", head) + "\n");

                    int time_of_repeat = 10;

                    for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                        int pack_size = (int) Math.pow(2, pack_size_exp);
                        BigDecimal modelCost = BigDecimal.ZERO;
                        BigDecimal modelTime = BigDecimal.ZERO;
                        BigDecimal modelDecodeTime = BigDecimal.ZERO;
                        int finalPackCount = 0;
                        float finalAvgPackSize = 0.0f;
                        String selectedScheme = "";

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

                                int remainder = scaledInts.length % pack_size;
                                int padding = (remainder == 0) ? 0 : pack_size - remainder;
                                long[] padded = new long[scaledInts.length + padding];
                                System.arraycopy(scaledInts, 0, padded, 0, scaledInts.length);
                                if (padding > 0) Arrays.fill(padded, scaledInts.length, padded.length, 0L);

                                int groups = padded.length / pack_size;
                                int[] bitWidths = new int[groups];
                                int gidx = 0;
                                for (int si = 0; si < padded.length; si += pack_size) {
                                    long maxInGroup = 0;
                                    for (int sj = si; sj < si + pack_size; ++sj) {
                                        long v = padded[sj];
                                        if (v > maxInGroup) maxInGroup = v;
                                    }
                                    int bitWidth = 0;
                                    if (maxInGroup > 0) {
                                        bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
                                    } else {
                                        bitWidth = 0;
                                    }
                                    bitWidths[gidx++] = bitWidth;
                                }

                                List<Integer> bitWidthsList = new ArrayList<>(groups);
                                for (int x = 0; x < groups; ++x) bitWidthsList.add(bitWidths[x]);

                                PackingResult res = packOctadsOptimized(bitWidthsList, model, null, pack_size, padded, scaledInts.length);
                                long duration = System.nanoTime() - startTime;

                                if (rep == 0) {
                                    finalPackCount = res.packCount;
                                    finalAvgPackSize = res.averagePackSize();
                                    if (res.compressedData != null && res.compressedData.length > 0) {
                                        int strategy = res.compressedData[0] & 0xFF;
                                        selectedScheme = strategy == 1 ? "Baseline" : "RL";
                                    }
                                }

                                if (res.compressedData != null) {
                                    long decodeStartTime = System.nanoTime();
                                    long[] decompressed = performAdaptiveDecompression(res.compressedData, bitWidths, pack_size, scaledInts.length);
                                    long[] decompressed_final = sprintzDecode(decompressed);
                                    long decodeDuration = System.nanoTime() - decodeStartTime;
                                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                                }

                                modelTime = modelTime.add(BigDecimal.valueOf(duration));
                                int actualCompressedSize = Math.max(0, (res.compressedData != null ? res.compressedData.length : 0) - 1);
                                modelCost = modelCost.add(BigDecimal.valueOf(actualCompressedSize * 8L));
                            }
                        }

                        BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                        modelCost = modelCost.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                        modelTime = modelTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                        modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);

                        BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                        BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, RoundingMode.HALF_UP);
                        BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelTime, 10, RoundingMode.HALF_UP);

                        writer.write(entry.toString() + ",");
                        writer.write("SPRINTZ-RL-OPTIMIZED,");
                        writer.write(modelTime_throughput.toPlainString() + ",");
                        writer.write(String.valueOf(numbers.size()) + ",");
                        writer.write(modelCost.toPlainString() + ",");
                        writer.write(String.valueOf(pack_size) + ",");
                        writer.write(model_ratio.toPlainString() + ",");
                        writer.write(selectedScheme + "\n");

                        System.out.printf("Pack Size %d: Compression Ratio = %.4f, Pack Count = %d, Avg Pack Size = %.2f, Encode Throughput = %.2f points/s, Scheme = %s\n",
                                pack_size, model_ratio.doubleValue(), finalPackCount, finalAvgPackSize,
                                modelTime_throughput.doubleValue(), selectedScheme);
                    }
                } catch (IOException e) {
                    System.err.println("Error writing output file for " + fname);
                }
            }
        } catch (IOException e) {
            System.err.println("Error iterating directory: " + directory);
        }
    }

    // ========== 工具函数 ==========
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
            if (s.isEmpty()) {
                vals[i] = BigDecimal.ZERO;
                continue;
            }
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

    // ========== 主函数 ==========
    public static void main(String[] args) {
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_rl_vary_pack_size";
        String modelPath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/sprintz_rl_optimized.model";

        int epochs = 200;

        if (args.length >= 1) dataDir = args[0];
        if (args.length >= 2) outDir = args[1];
        if (args.length >= 3) modelPath = args[2];

        System.out.println("Starting optimized RL training with multiple pack sizes...");
        System.out.println("Using data directory for training: " + dataDir);

        long startTime = System.nanoTime();
        RLDecisionModel model = trainModelMultiPackSize(epochs, dataDir, modelPath);
        long trainingTime = System.nanoTime() - startTime;

        System.out.println("Training completed in " + trainingTime / 1_000_000 + " ms");

        if (!dataDir.isEmpty()) {
            performanceTestPackVarPackSize(model, dataDir, outDir);
        } else {
            System.err.println("No data directory provided for performanceTest. Exiting.");
        }
    }

    @Test
    public void TestOptimizedModelVarPackSize() {
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_RL_vary_pack_size";
        String modelPath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/sprintz_rl_optimized.model";

        int epochs = 200;

        long startTime = System.nanoTime();
        RLDecisionModel model = trainModelMultiPackSize(epochs, dataDir, modelPath);
        long modelTime = System.nanoTime() - startTime;
        System.out.println("Training time: " + modelTime / 1_000_000 + " ms");
        performanceTestPackVarPackSize(model, dataDir, outDir);
    }

    @Test
    public void TestLoadAndUseOptimizedModel() {
        String modelPath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/sprintz_rl_optimized.model";
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_optimized_model_test";

        System.out.println("\nPerformance Testing with Optimized Model...");
        RLDecisionModel model;
        try {
            model = RLDecisionModel.loadModel(modelPath);
        } catch (Exception e) {
            System.err.println("Failed to load model from " + modelPath + ": " + e.getMessage());
            model = new RLDecisionModel();
        }

        performanceTestPackVarPackSize(model, dataDir, outDir);
    }
}