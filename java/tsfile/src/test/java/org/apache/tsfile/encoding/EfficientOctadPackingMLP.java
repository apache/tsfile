package org.apache.iotdb.tsfile.encoding;
// EfficientOctadPackingMLP_optimized.java
// Java 17+

import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.*;

public class EfficientOctadPackingMLP {

    static final int CHUNK_SIZE = 1024;
    static final int INPUT_DIM = 5;
    static final int HIDDEN_DIM = 48;

    // ========== CSV loader & scaling helpers ==========
    static List<List<Integer>> loadDataFromCSV(String filename) {
        List<List<Integer>> sequences = new ArrayList<>();
        Pattern pattern = Pattern.compile("\"?\\[([0-9,\\s]+)\\]\"?");
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            boolean firstLine = true;
            while ((line = br.readLine()) != null) {
                if (firstLine) { firstLine = false; continue; }
                Matcher m = pattern.matcher(line);
                if (m.find()) {
                    String data = m.group(1);
                    List<Integer> arr = new ArrayList<>();
                    String[] tokens = data.split(",");
                    for (String t : tokens) {
                        String s = t.trim();
                        if (s.isEmpty()) continue;
                        try {
                            arr.add(Integer.parseInt(s));
                        } catch (Exception ex) { /* ignore */ }
                    }
                    if (!arr.isEmpty()) sequences.add(arr);
                }
            }
        } catch (IOException e) {
            System.err.println("Error opening CSV: " + filename);
        }
        return sequences;
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

    // scaleNumbers: use BigDecimal to parse, scale by 10^decimalMax, shift so min becomes 0, return long[] with clipping
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
            s = s.replace(",", ""); // remove thousands sep

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
    }

    static class PackingResult {
        int packCount = 0;
        long dataCostA = 0;
        int bitWidthCostB = 0;
        int packSizeCostC = 0;
        long totalCost = 0;
        List<Pack> packs = new ArrayList<>();
        byte[] compressedData;

        void calculateCost(int maxLog) {
            bitWidthCostB = 6 * packCount;
            packSizeCostC = packCount * maxLog;
            totalCost = dataCostA + bitWidthCostB + packSizeCostC;
        }

        @Override
        public String toString() {
            return String.format("Packs: %d, Cost: %d (A=%d, B=%d, C=%d)", packCount, totalCost, dataCostA, bitWidthCostB, packSizeCostC);
        }
    }

    private static double safeLog(double p) {
        return Math.log(Math.max(p, 1e-8));
    }

    static class DecisionPoint {
        int currentPackSize;
        int currentPackMaxB;
        int newOctadB;
        int packCount;
        int currentMaxLog;
        boolean action;
        float probability;

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

    // ========== 2-layer MLP policy with REINFORCE ==========
    static class RLDecisionModel {
        float[] W1; // size HIDDEN_DIM * INPUT_DIM
        float[] b1; // size HIDDEN_DIM
        float[] W2; // size HIDDEN_DIM
        float b2;

        float explorationRate = 0.3f;
        float learningRate = 0.01f;

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
                feat[0] = dp.currentPackSize / 100.0f;
                feat[1] = dp.currentPackMaxB / 64.0f;
                feat[2] = dp.newOctadB / 64.0f;
                feat[3] = dp.packCount / 100.0f;
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

    }

    // ========== Bitpacking utility methods (legacy 8-values helpers kept) ==========
    public static int getBitWidth(int num) {
        if (num == 0)
            return 1;
        else
            return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,
                                   byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            int buffer = 0;
            int leftSize = 32;

            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                buffer |= (values.get(valueIdx) << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            if (leftSize > 0 && valueIdx < 8 + offset) {
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
                leftBit = width - leftSize;
            }

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
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            while (totalBits >= width && valueIdx < 8) {
                result_list.add((int) (buffer >>> (totalBits - width)));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    // 修复后的bitPacking方法，支持任意数量的数据（不只是8的倍数），逐bit拼接
    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int totalCount = numbers.size() - start;
        int currentBytePos = encode_pos;
        int currentBitPos = 0; // 当前字节中的位位置 (0-7)

        // 处理所有数据，不只是8的倍数
        for (int i = 0; i < totalCount; i++) {
            int value = numbers.get(start + i);

            // 按位写入
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

    // 修复后的decodeBitPacking方法，支持逐bit读取任意数量的数据
    public static ArrayList<Integer> decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int currentBytePos = decode_pos;
        int currentBitPos = 0; // 当前字节中的位位置 (0-7)

        // 读取 block_size 个值
        for (int i = 0; i < block_size; i++) {
            int value = 0;

            // 按位读取
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

    private static class BitWriter {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private long acc = 0L; // holds currently buffered bits (lowest "accBits" bits are valid)
        private int accBits = 0; // number of bits in acc

        // writeBits expects bits in LSB-aligned form (i.e., "masked" value). We append MSB-first as: acc = (acc << bitCount) | bits
        void writeBits(long bits, int bitCount) {
            if (bitCount == 0) return;
            if (bitCount == 64) {
                // split into two 32-bit writes to avoid shifting by 64
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
//    private static class BitReader {
//        final byte[] data;
//        private long acc = 0L;
//        private int accBits = 0;
//        private int idx = 0;
//
//        BitReader(byte[] data) {
//            this.data = data;
//        }
//
//        long readBits(int bitCount) throws IOException {
//            if (bitCount == 0) return 0L;
//            while (accBits < bitCount) {
//                if (idx < data.length) {
//                    acc = (acc << 8) | (data[idx++] & 0xFFL);
//                    accBits += 8;
//                } else {
//                    // pad with zeros if stream ends prematurely
//                    acc = (acc << (bitCount - accBits));
//                    accBits = bitCount;
//                }
//            }
//            int shift = accBits - bitCount;
//            long mask = (bitCount == 64) ? ~0L : ((1L << bitCount) - 1L);
//            long v = (acc >>> shift) & mask;
//            if (shift > 0) {
//                acc &= ((1L << shift) - 1L);
//            } else {
//                acc = 0L;
//            }
//            accBits = shift;
//            return v;
//        }
//    }
public static final class BitReader {
    private final byte[] data;
    private int bitPos;  // global bit position from start of data[]

    public BitReader(byte[] data) {
        this(data, 0);
    }

    public BitReader(byte[] data, int byteOffset) {
        this.data = data;
        this.bitPos = byteOffset * 8;
    }

    /**
     * Read n bits (0 <= n <= 64), return as unsigned long.
     */
    public long readBits(int n) {
        if (n == 0) return 0L;
        if (n < 0 || n > 64) {
            throw new IllegalArgumentException("n must be between 0 and 64");
        }

        long result = 0L;
        int bitsRemaining = n;

        while (bitsRemaining > 0) {
            int byteIndex = bitPos >>> 3;     // current byte
            int bitOffset = bitPos & 7;       // offset inside byte [0..7]

            // 添加边界检查
            if (byteIndex >= data.length) {
                // 如果已经超出数据范围，填充0并返回
                result = (result << bitsRemaining);
                bitPos += bitsRemaining;
                return result;
            }

            int bitsFromCurrentByte = Math.min(8 - bitOffset, bitsRemaining);

            // Load byte as unsigned
            int curByte = data[byteIndex] & 0xFF;

            // Shift to get the relevant bits
            int shift = 8 - bitOffset - bitsFromCurrentByte;
            int chunk = (curByte >>> shift) & ((1 << bitsFromCurrentByte) - 1);

            result = (result << bitsFromCurrentByte) | chunk;

            bitPos += bitsFromCurrentByte;
            bitsRemaining -= bitsFromCurrentByte;
        }

        return result;
    }

    /**
     * @return total bits consumed since creation / since byteOffset
     */
    public int consumedBits() {
        return bitPos;
    }

    /**
     * @return current bit position (alias)
     */
    public int bitPosition() {
        return bitPos;
    }

    /**
     * @return remaining bits available for reading
     */
    public int remainingBits() {
        return (data.length * 8) - bitPos;
    }
}
    private static byte[] performBitPackingCompression64_fast(long[] dataArray, List<Pack> packs, int pack_size, int originalLength) throws IOException {
        // 计算一些元信息
        int totalPacks = packs.size();
        int maxOctadsInAnyPack = 0;
        for (Pack p : packs) if (p.size > maxOctadsInAnyPack) maxOctadsInAnyPack = p.size;

        // bits needed to encode counts in range [0..maxOctadsInAnyPack]
        int bitsForCount = 1;
        while ((1L << bitsForCount) <= maxOctadsInAnyPack) bitsForCount++;
        if (bitsForCount <= 0) bitsForCount = 1;

        // 准备输出缓冲（先写 header 的整数字段，meta/data 用 BitWriter 位流）
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeByte(totalPacks);
        dos.writeByte(bitsForCount); // 1 byte is enough to carry this small number
        dos.flush();

        // --- Meta bitstream ---
        BitWriter metaWriter = new BitWriter();

        // For each pack: write pack.size using bitsForCount bits, then for each octad write its bitWidth using 6 bits
        for (Pack pack : packs) {
            // write octad count
            metaWriter.writeBits(pack.size, bitsForCount);
            metaWriter.writeBits(pack.bitWidths.get(0), 6);
//            // write each octad's bitWidth using 6 bits.
//            // NOTE: we map bitWidth==64 -> store 63 (as sentinel). 解码端须按此约定把 63 映射回 64。
//            for (int i = 0; i < pack.size; ++i) {
//                int bw = pack.bitWidths.get(i);
//                int store = bw;
//                if (bw == 64) store = 63;
//                if (store < 0) store = 0;
//                if (store > 63) store = 63; // safety clamp
//                metaWriter.writeBits(store, 5);
//            }
        }

        byte[] metaBytes = metaWriter.finish();
        dos.write(metaBytes);
        dos.flush();

        // --- Data bitstream ---
        BitWriter dataWriter = new BitWriter();

        // For each pack, find packMaxBitWidth and write each group's pack_size values using packMaxBitWidth bits
        for (Pack pack : packs) {
            int packMaxBW = pack.maxBitWidth;
            // no change for packMaxBW == 64: BitWriter supports splitting 64 into two 32-bit writes
            for (int i = 0; i < pack.size; ++i) {
                int originalGroupIndex = pack.indices.get(0);
                int startPos = originalGroupIndex * pack_size;
                for (int j = 0; j < pack_size; ++j) {
                    long val;
//                    if (startPos + j < dataArray.length) {
                        val = dataArray[startPos + j];
//                    } else {
//                        val = 0L;
//                    }
                    // mask value to packMaxBW bits (if packMaxBW == 64, mask preserves full 64 bits)
                    long mask;
                    if (packMaxBW == 0) {
                        dataWriter.writeBits(0L, 0); // nothing to write
                    } else {
                        if (packMaxBW == 64) {
                            // write full 64-bit value (BitWriter handles split)
                            dataWriter.writeBits(val, 64);
                        } else {
                            mask = (1L << packMaxBW) - 1L;
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

    // ========== packOctads (updated to accept originalLength for compression) ==========
    static PackingResult packOctads(List<Integer> bitWidths, RLDecisionModel model, List<DecisionPoint> decisionTrace, int pack_size, long[] dataArray, int originalLength) {
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
                    decisionTrace.add(new DecisionPoint(currentPack.size, currentPack.maxBitWidth, b, packCount, globalMaxLog, shouldMerge, probability));
                }

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

        // 执行实际的bitpacking压缩（如果提供了 dataArray）
        if (dataArray != null) {
            try {
                result.compressedData = performBitPackingCompression64_fast(dataArray, result.packs, pack_size, originalLength);
            } catch (IOException e) {
                System.err.println("Compression failed: " + e.getMessage());
                result.compressedData = null;
            }
        }

        return result;
    }

//    public static long[] fastDecompress(byte[] compressedData, int[] bitWidths, int packSize, int originalLength) {
//        try {
//            // 这里需要根据实际的压缩格式来解析
//            // 假设compressedData包含bit-packed数据
//            ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
//            DataInputStream dis = new DataInputStream(bais);
//
//            // 读取bit-packed数据
//            int totalGroups = bitWidths.length;
//            long[] result = new long[totalGroups * packSize];
//            int resultIndex = 0;
//
//            BitReader reader = new BitReader(compressedData);
//
//            for (int g = 0; g < totalGroups; ++g) {
//                int bw = bitWidths[g];
//                for (int k = 0; k < packSize; ++k) {
//                    if (bw == 0) {
//                        result[resultIndex++] = 0L;
//                    } else if (bw == 64) {
//                        long high = reader.readBits(32);
//                        long low = reader.readBits(32);
//                        long v = (high << 32) | (low & 0xFFFFFFFFL);
//                        result[resultIndex++] = v;
//                    } else {
//                        long v = reader.readBits(bw);
//                        result[resultIndex++] = v;
//                    }
//                }
//            }
//
//            // 只取原始长度的数据并Sprintz解码
//            return Arrays.copyOf(result, originalLength);
//
//        } catch (IOException e) {
//            System.err.println("Fast decompression failed: " + e.getMessage());
//            return new long[0];
//        }
//    }

    public static long[] fastDecompress(byte[] compressedData, int[] bitWidths, int packSize, int originalLength) {
        if (compressedData == null || compressedData.length == 0) {
            System.err.println("Compressed data is null or empty");
            return new long[0];
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
            DataInputStream dis = new DataInputStream(bais);

            // === Header ===
            int totalPacks = dis.readUnsignedByte();
            int bitsForCount = dis.readUnsignedByte();

            // === Read meta bitstream ===
            int metaStartOffset = 2; // two bytes read
            BitReader metaReader = new BitReader(compressedData, metaStartOffset);

            // 解析每个pack的信息
            List<PackInfo> packInfos = new ArrayList<>();
            int totalValuesToDecode = 0;

            for (int p = 0; p < totalPacks; ++p) {
                int octadCount = (int) metaReader.readBits(bitsForCount);
                int packBitWidth = (int) metaReader.readBits(6);
                if (packBitWidth == 63) packBitWidth = 64;

                packInfos.add(new PackInfo(octadCount, packBitWidth));
                totalValuesToDecode += octadCount * packSize;
            }

            // 计算数据部分的起始位置
            int metaBitsUsed = metaReader.consumedBits();
            int dataStartByte = metaStartOffset + (metaBitsUsed + 7) / 8;

            // 检查数据起始位置是否超出压缩数据范围
            if (dataStartByte >= compressedData.length) {
//                System.err.println("Data start position exceeds compressed data length");
                return new long[0];
            }

            // === Data bitstream ===
            BitReader dataReader = new BitReader(compressedData, dataStartByte);
            List<Long> resultList = new ArrayList<>();

            // === 按pack解码数据 ===
            for (PackInfo packInfo : packInfos) {
                int octadCount = packInfo.octadCount;
                int bitWidth = packInfo.bitWidth;

                // 检查剩余数据是否足够
                if (dataReader.remainingBits() < (long) octadCount * packSize * bitWidth) {
//                    System.err.println("Insufficient data for decoding pack. Expected: " +
//                            (octadCount * packSize * bitWidth) + " bits, Available: " +
//                            dataReader.remainingBits() + " bits");
                    break;
                }

                // 每个octad包含packSize个值
                for (int i = 0; i < octadCount; ++i) {
                    for (int j = 0; j < packSize; ++j) {
                        long value;
                        if (bitWidth == 0) {
                            value = 0L;
                        } else if (bitWidth == 64) {
                            // 64位特殊处理：分成两个32位读取
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

            // 转换为数组并截取到原始长度
            long[] result = new long[Math.min(resultList.size(), originalLength)];
            for (int i = 0; i < result.length; i++) {
                result[i] = resultList.get(i);
            }

//            System.out.println("Decompression completed: " + result.length + " values decoded");
            return result;

        } catch (Exception e) {
            System.err.println("Fast decompression failed: " + e.getMessage());
            e.printStackTrace();
            return new long[0];
        }
    }

    // 辅助类，存储pack信息
    static class PackInfo {
        int octadCount;
        int bitWidth;

        PackInfo(int octadCount, int bitWidth) {
            this.octadCount = octadCount;
            this.bitWidth = bitWidth;
        }
    }
    // ========== Training loop (trainModel) ==========
    static RLDecisionModel trainModel(int epochs, String csvFilePath) {
        System.err.println("Training RL model from CSV data...");
        RLDecisionModel model = new RLDecisionModel();
        List<List<Integer>> sequences = loadDataFromCSV(csvFilePath);
        if (sequences.isEmpty()) {
            System.err.println("No data loaded from CSV. Returning initial model.");
            return model;
        }
        System.err.println("Loaded " + sequences.size() + " sequences from CSV");

        List<DecisionPoint> decisionTrace = new ArrayList<>();
        for (int epoch = 1; epoch <= epochs; ++epoch) {
            long startTime = System.nanoTime();
            float totalReward = 0.0f;
            float totalLoss = 0.0f;
            int processedSequences = 0;

            for (List<Integer> bitWidths : sequences) {
                decisionTrace.clear();
                // training does not perform actual compression, pass dataArray=null and originalLength=0
                PackingResult result = packOctads(bitWidths, model, decisionTrace, 1, null, 0);

                float reward = (float) result.totalCost / 500000.0f;
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


    // ========== performanceTest (更新版本，包含解压测试) ==========
    static void performanceTest(RLDecisionModel model, String directory, String outputDirStr) {
        System.out.println("\nPerformance Testing...");
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
                    // 更新表头，增加解压吞吐率列
                    writer.write("Input Direction,Encoding Algorithm,Encoding Time,Decoding Time,Points,Compressed Size,Pack Size,Compression Ratio\n");

                    int time_of_repeat = 50;

                    for(int pack_size_exp = 3; pack_size_exp < 4; pack_size_exp++) {
                        int pack_size = (int) Math.pow(2, pack_size_exp);
                        BigDecimal modelCost = BigDecimal.ZERO;
                        BigDecimal modelTime = BigDecimal.ZERO;
                        BigDecimal modelDecodeTime = BigDecimal.ZERO; // 新增：解压时间统计
                        long compressedSize = 0;

                        for (int rep = 0; rep < time_of_repeat; ++rep) {
                            for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                                int end = Math.min(numbers.size(), i + CHUNK_SIZE);
                                if (end - i <= 2) continue;
                                List<String> chunkNumbers = numbers.subList(i, end);
                                int decimalMax = 0;
                                for (int k = i; k < end; ++k) {
                                    if (decimalPlaces.get(k) > decimalMax) decimalMax = decimalPlaces.get(k);
                                }

                                long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
                                long startTime = System.nanoTime();

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

                                // pass original length (un-padded) so decoder can trim
                                List<Integer> bitWidthsList = new ArrayList<>(groups);
                                for (int x = 0; x < groups; ++x) bitWidthsList.add(bitWidths[x]);

                                PackingResult res = packOctads(bitWidthsList, model, null, pack_size, padded, scaledInts.length);
                                long duration = System.nanoTime() - startTime;
                                modelTime = modelTime.add(BigDecimal.valueOf(duration));
                                modelCost = modelCost.add(BigDecimal.valueOf(res.compressedData.length * 8L));

                                // 新增：测试解压性能
                                if (res.compressedData != null) {
                                    long startDecodeTime = System.nanoTime();
                                    long[] decompressed = fastDecompress(res.compressedData, bitWidths, pack_size, scaledInts.length);
                                    long decodeDuration = System.nanoTime() - startDecodeTime;
                                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                                }

//                                if (rep == 0) {
//                                    compressedSize += (res.compressedData != null) ? res.compressedData.length : 0;
//                                }
                            }
                        }

                        BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                        modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                        modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                        modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP); // 平均解压时间

                        BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                        BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP); // compressed / original bytes
                        BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP); // points/s
                        BigDecimal modelDecodeTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP); // points/s

                        writer.write(entry.toString() + ",");
                        writer.write("BP-RL,");
                        writer.write(modelTime_throughput.toPlainString() + ",");
                        writer.write(modelDecodeTime_throughput.toPlainString() + ","); // 解压吞吐率
                        writer.write(String.valueOf(numbers.size()) + ",");
                        writer.write(modelCost.toPlainString() + ",");
                        writer.write(String.valueOf(pack_size) + ",");
                        writer.write(model_ratio.toPlainString() + "\n");

//                        System.out.println("Pack Size: " + pack_size);
//                        System.out.println("Encoding throughput: " + modelTime_throughput + " points/s");
//                        System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " points/s");
//                        System.out.println("Compression ratio: " + model_ratio);
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
        String trainCsv = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRL";

        int epochs = 20;

        if (args.length >= 1) trainCsv = args[0];
        if (args.length >= 2) dataDir = args[1];
        if (args.length >= 3) outDir = args[2];

        RLDecisionModel model = new RLDecisionModel();
        if (!trainCsv.isEmpty()) {
            model = trainModel(epochs, trainCsv);
        } else {
            System.err.println("No training CSV given. Using randomly initialized RL model.");
        }

        if (!dataDir.isEmpty()) {
            performanceTest(model, dataDir, outDir);
        } else {
            System.err.println("No data directory provided for performanceTest. Exiting.");
        }
    }
    
        // ========== performanceTest (更新版本，包含解压测试) ==========
    static void performanceTestVarPackSize(RLDecisionModel model, String directory, String outputDirStr) {
            System.out.println("\nPerformance Testing...");
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
                        // 更新表头，增加解压吞吐率列
                        writer.write("Input Direction,Encoding Algorithm,Encoding Time,Decoding Time,Points,Compressed Size,Pack Size,Compression Ratio\n");
    
                        int time_of_repeat = 50;
    
                        for(int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                            int pack_size = (int) Math.pow(2, pack_size_exp);
                            BigDecimal modelCost = BigDecimal.ZERO;
                            BigDecimal modelTime = BigDecimal.ZERO;
                            BigDecimal modelDecodeTime = BigDecimal.ZERO; // 新增：解压时间统计
                            long compressedSize = 0;
    
                            for (int rep = 0; rep < time_of_repeat; ++rep) {
                                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                                    int end = Math.min(numbers.size(), i + CHUNK_SIZE);
                                    if (end - i <= 2) continue;
                                    List<String> chunkNumbers = numbers.subList(i, end);
                                    int decimalMax = 0;
                                    for (int k = i; k < end; ++k) {
                                        if (decimalPlaces.get(k) > decimalMax) decimalMax = decimalPlaces.get(k);
                                    }
    
                                    long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
                                    long startTime = System.nanoTime();
    
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
    
                                    // pass original length (un-padded) so decoder can trim
                                    List<Integer> bitWidthsList = new ArrayList<>(groups);
                                    for (int x = 0; x < groups; ++x) bitWidthsList.add(bitWidths[x]);
    
                                    PackingResult res = packOctads(bitWidthsList, model, null, pack_size, padded, scaledInts.length);
                                    long duration = System.nanoTime() - startTime;
                                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                                    modelCost = modelCost.add(BigDecimal.valueOf(res.compressedData.length * 8L));
    
                                    // 新增：测试解压性能
                                    if (res.compressedData != null) {
                                        long startDecodeTime = System.nanoTime();
                                        long[] decompressed = fastDecompress(res.compressedData, bitWidths, pack_size, scaledInts.length);
                                        long decodeDuration = System.nanoTime() - startDecodeTime;
                                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                                    }
    
    //                                if (rep == 0) {
    //                                    compressedSize += (res.compressedData != null) ? res.compressedData.length : 0;
    //                                }
                                }
                            }
    
                            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                            modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                            modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP); // 平均解压时间
    
                            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP); // compressed / original bytes
                            BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP); // points/s
                            BigDecimal modelDecodeTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP); // points/s
    
                            writer.write(entry.toString() + ",");
                            writer.write("BP-RL,");
                            writer.write(modelTime_throughput.toPlainString() + ",");
                            writer.write(modelDecodeTime_throughput.toPlainString() + ","); // 解压吞吐率
                            writer.write(String.valueOf(numbers.size()) + ",");
                            writer.write(modelCost.toPlainString() + ",");
                            writer.write(String.valueOf(pack_size) + ",");
                            writer.write(model_ratio.toPlainString() + "\n");
    
    //                        System.out.println("Pack Size: " + pack_size);
    //                        System.out.println("Encoding throughput: " + modelTime_throughput + " points/s");
    //                        System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " points/s");
    //                        System.out.println("Compression ratio: " + model_ratio);
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
    public void TestVarPackSize() {
        String trainCsv = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRL_vary_pack_size";

        int epochs = 80;

        long startTime = System.nanoTime();
        RLDecisionModel model = new RLDecisionModel();
        model = trainModel(epochs, trainCsv);
        long modelTime = System.nanoTime() - startTime;
        System.out.println("training time: " +  modelTime);
        performanceTestVarPackSize(model, dataDir, outDir);
    }
    // ========== performanceTest with variable chunk sizes ==========
    static void performanceTestVariableChunkSize(RLDecisionModel model, String directory, String outputDirStr) {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        Path outdir = Paths.get(outputDirStr);
        try {
            if (!Files.exists(outdir)) Files.createDirectories(outdir);
        } catch (IOException e) {
            System.err.println("Cannot create output dir: " + outputDirStr);
            return;
        }

        // Define the chunk sizes to test (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
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
                    writer.write("m,Input Direction,Encoding Algorithm,Encoding Time,Points,Compressed Size,Pack Size,Compression Ratio\n");

                    int time_of_repeat = 50; // Reduced for faster testing with multiple chunk sizes
                    int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

                    // 分批处理，每1024个元素一批
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

                    // Test each chunk size
                    for (int chunkSize : chunkSizes) {
                        System.out.println("Testing chunk size: " + chunkSize);

                        for (int pack_size_exp = 3; pack_size_exp < 4; pack_size_exp++) {
                            int pack_size = (int) Math.pow(2, pack_size_exp);
                            BigDecimal modelCost = BigDecimal.ZERO;
                            BigDecimal modelTime = BigDecimal.ZERO;
                            long compressedSize = 0;

                            for (int rep = 0; rep < time_of_repeat; ++rep) {
                                for (int i = 0; i < numbers.size(); i += chunkSize) {
//                                    int end = Math.min(numbers.size(), i + chunkSize);
//                                    if (end - i <= 2) continue;
//                                    List<String> chunkNumbers = numbers.subList(i, end);
//                                    int decimalMax = 0;
//                                    for (int k = i; k < end; ++k) {
//                                        if (decimalPlaces.get(k) > decimalMax) decimalMax = decimalPlaces.get(k);
//                                    }
//
//                                    long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                                    int end = Math.min(i + chunkSize, numbers.size());
                                    long[] scaledInts = new long[end-i];
                                    if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInts, 0, end - i);

                                    long startTime = System.nanoTime();

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

                                    // pass original length (un-padded) so decoder can trim
                                    List<Integer> bitWidthsList = new ArrayList<>(groups);
                                    for (int x = 0; x < groups; ++x) bitWidthsList.add(bitWidths[x]);

                                    PackingResult res = packOctads(bitWidthsList, model, null, pack_size, padded, scaledInts.length);
                                    long duration = System.nanoTime() - startTime;
                                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                                    modelCost = modelCost.add(BigDecimal.valueOf(res.compressedData.length * 8L));

//                                    if (rep == 0) {
//                                        compressedSize += (res.compressedData != null) ? res.compressedData.length : 0;
//                                    }
                                }
                            }

                            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                            modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP); // compressed / original bytes
                            BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(1000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP); // points/ms

                            writer.write(String.valueOf(chunkSize/8) + ",");
                            writer.write(entry.toString() + ",");
                            writer.write("BP-RL,");
                            writer.write(modelTime_throughput.toPlainString() + ",");
                            writer.write(String.valueOf(numbers.size()) + ",");
                            writer.write(modelCost.toPlainString() + ",");
                            writer.write(String.valueOf(pack_size) + ",");
                            writer.write(model_ratio.toPlainString() + "\n");
                        }
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
        String trainCsv = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String dataDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outDir = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRL_vary_m";

        int epochs = 20;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModel(epochs, trainCsv);
        performanceTestVariableChunkSize(model, dataDir, outDir);
    }
}