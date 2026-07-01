package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * BDC（dynamic bit packing）实现 - 无 Delta / 无 ZigZag 版本
 *
 * 核心函数：
 * - encodeDynamicPacking(int[] paddedArray, int packSize, int qmbdLen) -> byte[]
 * - decodeDynamicPacking(byte[] encoded, int packSize, int originalLength) -> int[]
 *
 * 主要改动：bit-packing 部分改用“位缓冲（bit buffer）连续写入 / 读取”的实现，
 * 与之前你给出的 pack8Values / unpack8Values 思路一致（按 width 连续写位流）。
 */
public class BDCTest {
    // EMA for estimating S
//    static class EMA {
//        private final double alpha;
//        private double value = -1;
//
//        EMA(double alpha) { this.alpha = alpha; }
//
//        void add(double x) {
//            if (value < 0) value = x;
//            else value = alpha * x + (1 - alpha) * value;
//        }
//
//        double get() {
//            return value < 0 ? 0 : value;
//        }
//    }
//    public static byte[] encodeDynamicPacking(long[] paddedArray, int packSize, int qmbdLen) {
//        if (paddedArray == null) return new byte[0];
//
//        int totalGroups = paddedArray.length / packSize;
//
//        ArrayList<Long> queueValues = new ArrayList<>();
//        ArrayList<Integer> queueBDs = new ArrayList<>(); // 每个 group 的 BD
//
//        int maxBD = 0;
//        int SP = -1; // split position (group index)
//
//        final int SUBHEADER_BITS_ESTIMATE = 80;
//        EMA emaS = new EMA(0.5);   // 论文建议 SMA/EMA，这里用 EMA
//
//        List<byte[]> packPayloads = new ArrayList<>();
//        List<Integer> packNumGroups = new ArrayList<>();
//        List<Integer> packBitWidths = new ArrayList<>();
//
//        for (int g = 0; g < totalGroups; g++) {
//            int start = g * packSize;
//            long groupMax = 0;
//            for (int k = 0; k < packSize; k++) {
//                long v = paddedArray[start + k];
//                if (v > groupMax) groupMax = v;
//            }
//            int groupBD = getBitWidth(groupMax);
//
//            /* ---------- 空队列 ---------- */
//            if (queueBDs.isEmpty()) {
//                for (int k = 0; k < packSize; k++) {
//                    queueValues.add(paddedArray[start + k]);
//                }
//                queueBDs.add(groupBD);
//                maxBD = groupBD;
//                SP = 0;
//                continue;
//            }
//
//            int oldMaxBD = maxBD;
//
//            /* ---------- Case 1/2: BD >= maxBD ---------- */
//            if (groupBD >= maxBD) {
//                int wastedBits = (groupBD - maxBD) * queueValues.size();
//
//                if (wastedBits >= SUBHEADER_BITS_ESTIMATE ||
//                        queueBDs.size() >= qmbdLen) {
//
//                    flushPack(queueValues, queueBDs, maxBD,
//                            packSize, packPayloads, packNumGroups, packBitWidths);
//
//                    queueValues.clear();
//                    queueBDs.clear();
//
//                    for (int k = 0; k < packSize; k++) {
//                        queueValues.add(paddedArray[start + k]);
//                    }
//                    queueBDs.add(groupBD);
//                    maxBD = groupBD;
//                    SP = 0;
//                    continue;
//                }
//
//                for (int k = 0; k < packSize; k++) {
//                    queueValues.add(paddedArray[start + k]);
//                }
//                queueBDs.add(groupBD);
//                maxBD = Math.max(maxBD, groupBD);
//                SP = queueBDs.size() - 1;
//                continue;
//            }
//
//            /* ---------- Case 3: groupBD < maxBD ---------- */
//
//            int N = queueBDs.size();
//            int rightLen = N - (SP + 1);
//
//            int rightMaxBD = 0;
//            for (int i = SP + 1; i < N; i++) {
//                rightMaxBD = Math.max(rightMaxBD, queueBDs.get(i));
//            }
//
//            double S = emaS.get();
//            if (S == 0) S = packSize; // fallback
//
//            double A = (oldMaxBD - groupBD) * S;
//            double B = (rightMaxBD - groupBD) * rightLen;
//
//            if (B >= A && rightLen > 0) {
//                SP = N - 1; // 移动 SP
//            }
//
//            for (int k = 0; k < packSize; k++) {
//                queueValues.add(paddedArray[start + k]);
//            }
//            queueBDs.add(groupBD);
//
//            maxBD = Math.max(maxBD, groupBD);
//
//            emaS.add(queueBDs.size());
//        }
//
//        /* ---------- flush remaining ---------- */
//        if (!queueValues.isEmpty()) {
//            flushPack(queueValues, queueBDs, maxBD,
//                    packSize, packPayloads, packNumGroups, packBitWidths);
//        }
//
//        /* ---------- serialize output（与你原实现一致） ---------- */
//        int packCount = packPayloads.size();
//        int totalLen = 1;
//        for (int i = 0; i < packCount; i++) {
//            totalLen += 2 + packPayloads.get(i).length;
//        }
//
//        byte[] result = new byte[totalLen];
//        int pos = 0;
//        result[pos++] = (byte) packCount;
//
//        for (int i = 0; i < packCount; i++) {
//            result[pos++] = (byte) (packBitWidths.get(i) & 0xFF);
//            result[pos++] = (byte) (packNumGroups.get(i) & 0xFF);
//            byte[] payload = packPayloads.get(i);
//            System.arraycopy(payload, 0, result, pos, payload.length);
//            pos += payload.length;
//        }
//
//        return result;
//    }
//    private static void flushPack(
//            ArrayList<Long> queueValues,
//            ArrayList<Integer> queueBDs,
//            int maxBD,
//            int packSize,
//            List<byte[]> packPayloads,
//            List<Integer> packNumGroups,
//            List<Integer> packBitWidths) {
//
//        int groupCount = queueBDs.size();
//        byte[] payload = bitPackList(queueValues, Math.max(1, maxBD));
//
//        packPayloads.add(payload);
//        packNumGroups.add(groupCount);
//        packBitWidths.add(Math.max(1, maxBD));
//    }

    private static final int CHUNK_SIZE = 1024;
    static int all_max_pack_size = 1;

    // -------------------- 工具 --------------------
    public static int getBitWidth(long num) {
        if (num == 0) return 1;
        return 64 - Long.numberOfLeadingZeros(num);
    }

    public static int bytesToPackCount(byte[] src, int pos) {
        int v = 0;
        v |= (src[pos] & 0xFF);
        return v;
    }

    // 8. 修复：sprintz编码解码
    public static long[] zigzag(long[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new long[0];
        }

        long[] result = new long[numbers.length];

        for (int i = 0; i < numbers.length; i++) {
            // ZigZag编码
            result[i] = (numbers[i] << 1) ^ (numbers[i] >> 31);
        }

        return result;
    }

    public static long[] zigzagDecode(long[] encodedData) {
        if (encodedData == null || encodedData.length == 0) {
            return new long[0];
        }

        long[] result = new long[encodedData.length+1];

        for (int i = 0; i < encodedData.length; i++) {
            // ZigZag解码
            long zigzag = encodedData[i];
            result[i] = (zigzag >>> 1) ^ -(zigzag & 1);
        }

        return result;
    }

    // 封装结果类
    public static class SprintzEncodedResult {
        private final long[] encodedData;  // 编码后的数据
        private final long firstValue;     // 第一个原始值

        public SprintzEncodedResult(long[] encodedData, long firstValue) {
            this.encodedData = encodedData;
            this.firstValue = firstValue;
        }

        public long[] getEncodedData() {
            return encodedData;
        }

        public long getFirstValue() {
            return firstValue;
        }


        @Override
        public String toString() {
            return String.format("EncodedResult{firstValue=%d, minDiff=%d, encodedData=%s}",
                    firstValue, java.util.Arrays.toString(encodedData));
        }
    }

    // 8. 修复：sprintz编码解码
    public static SprintzEncodedResult sprintz(long[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new SprintzEncodedResult(new long[0],  0);
        }

        long[] result = new long[numbers.length];

        for (int i = 1; i < numbers.length; i++) {
            long diff = numbers[i] - numbers[i-1];
            // ZigZag编码
            result[i-1] = (diff << 1) ^ (diff >> 31);
        }

        return new SprintzEncodedResult(result, numbers[0]);
    }

    public static long[] sprintzDecode(long[] encodedData, long firstValue) {
        if (encodedData == null || encodedData.length == 0) {
            return new long[0];
        }

        long[] result = new long[encodedData.length+1];
        result[0] = firstValue;

        for (int i = 0; i < encodedData.length; i++) {
            // ZigZag解码
            long zigzag = encodedData[i];
            long diff = (zigzag >>> 1) ^ -(zigzag & 1);
            result[i+1] = result[i] + diff;
        }

        return result;
    }

    // 封装结果类
    public static class TSDIFFEncodedResult {
        private final long[] encodedData;  // 编码后的数据
        private final long firstValue;     // 第一个原始值
        private final long minDiff;        // 最小差分值

        public TSDIFFEncodedResult(long[] encodedData, long firstValue, long minDiff) {
            this.encodedData = encodedData;
            this.firstValue = firstValue;
            this.minDiff = minDiff;
        }

        public long[] getEncodedData() {
            return encodedData;
        }

        public long getFirstValue() {
            return firstValue;
        }

        public long getMinDiff() {
            return minDiff;
        }


        @Override
        public String toString() {
            return String.format("EncodedResult{firstValue=%d, minDiff=%d, encodedData=%s}",
                    firstValue, java.util.Arrays.toString(encodedData));
        }
    }

    public static TSDIFFEncodedResult ts2diff(long[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new TSDIFFEncodedResult(new long[0],0,0);
        }

        long[] result = new long[numbers.length-1];

        // 第一个值保持不变
        long firstValue = numbers[0];
//        result[0] = numbers[0];

        // 计算差分并找到最小差分
        long minDiff = Long.MAX_VALUE;
        long[] diffs = new long[numbers.length - 1];

        for (int i = 1; i < numbers.length; i++) {
            long diff = numbers[i] - numbers[i - 1];
            diffs[i - 1] = diff;

            if (diff < minDiff) {
                minDiff = diff;
            }
        }

        // 如果数组长度大于1，处理差分值
        if (numbers.length > 1) {
            // 使用最小差分进行归一化处理
            for (int i = 1; i < numbers.length; i++) {
                long normalizedDiff = diffs[i - 1] - minDiff;
                // ZigZag编码
                result[i-1] = normalizedDiff; // << 1) ^ (normalizedDiff >> 31);
            }
        }

        return new TSDIFFEncodedResult(result, firstValue, minDiff);
    }
    public static long[] ts2diffDecode(long[] result, long firstValue, long minDiff) {
        if (result == null || result.length == 0) {
            return new long[0];
        }

        long[] numbers = new long[result.length];
        numbers[0] = firstValue;

        for (int i = 1; i < result.length; i++) {
            // ZigZag解码
            long n = result[i];
            long normalizedDiff = (n >>> 1) ^ -(n & 1);

            // 还原原始差分
            long diff = normalizedDiff + minDiff;

            // 累加得到原始值
            numbers[i] = numbers[i - 1] + diff;
        }

        return numbers;
    }

    private static long[] scaleNumbers(List<String> numbers, int decimalMax) {
        // 1. 预先计算缩放因子
        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
        int size = numbers.size();
        long[] result = new long[size];

        if (size == 0) {
            return result;
        }

        // 2. 直接缩放所有数值
        for (int i = 0; i < size; i++) {
            // 缩放并转换为long
            BigDecimal scaledVal = new BigDecimal(numbers.get(i)).multiply(scale);
            result[i] = scaledVal.longValue();
        }

        return result;
    }

    // -------------------- bit-packing (基于位缓冲，行为与上次 pack8/unpack8 思路一致) --------------------
//    /**
//     * 将 values（任意长度，但通常为 packSize 的整数倍）以 width 位连续写成字节流（高位先出）。
//     * 返回字节数组（末尾会按位补齐到整字节）。
//     */
//    private static byte[] bitPackList(ArrayList<Long> values, int width) {
//        if (values == null || values.isEmpty()) return new byte[0];
//
//        // 计算需要的字节数
//        int totalBits = values.size() * width;
//        int totalBytes = (totalBits + 7) / 8;
//        byte[] encoded_result = new byte[totalBytes];
//
//        // 处理8的倍数个数值
//        int encode_pos = bitPacking(values, 0, width, 0, encoded_result);
//
//        // 处理剩余的值（如果不是8的倍数）
//        int processedCount = (values.size() / 8) * 8;
//        int remaining = values.size() - processedCount;
//
//        if (remaining > 0) {
//            // 使用原来的位打包逻辑处理剩余的值
//            long bitBuffer = 0L;
//            int bitCount = 0;
//            long mask = (width >= 63) ? -1L : ((1L << width) - 1L);
//
//            for (int i = processedCount; i < values.size(); i++) {
//                long v = values.get(i);
//                long vv = v & mask;
//                bitBuffer = (bitBuffer << width) | vv;
//                bitCount += width;
//
//                while (bitCount >= 8) {
//                    int shift = bitCount - 8;
//                    int b = (int) ((bitBuffer >>> shift) & 0xFFL);
//                    encoded_result[encode_pos++] = (byte) b;
//                    bitCount -= 8;
//                    if (bitCount == 0) {
//                        bitBuffer = 0L;
//                    } else {
//                        bitBuffer = bitBuffer & ((1L << bitCount) - 1L);
//                    }
//                }
//            }
//
//            // 写剩余的不足一字节的位
//            if (bitCount > 0) {
//                int b = (int) ((bitBuffer << (8 - bitCount)) & 0xFFL);
//                encoded_result[encode_pos++] = (byte) b;
//            }
//        }
//
//        // 创建正确大小的结果数组
//        if (encode_pos < totalBytes) {
//            byte[] result = new byte[encode_pos];
//            System.arraycopy(encoded_result, 0, result, 0, encode_pos);
//            return result;
//        }
//
//        return encoded_result;
//    }

    public static void pack8Values(ArrayList<Long> values, int offset, int width, int encode_pos,
                                   byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Long
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Long to 'buffer'
            if (leftBit > 0) {
                buffer |= ((int) (values.get(valueIdx) << (32 - leftBit)));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Long to the 'buffer'
                buffer |= ((int) (values.get(valueIdx) << (leftSize - width)));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Long,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Long into remaining space of the buffer
                buffer |= ((int) (values.get(valueIdx) >>> (width - leftSize)));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
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

    public static int bitPacking(ArrayList<Long> numbers, int start, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int block_num = (numbers.size() - start) / 8;
        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, start + i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        return encode_pos;

    }

    /**
     * 从 encoded[pos..] 连续读取 count 个 width-bit 的值，返回 UnpackResult 包含读出的值与消耗的字节数。
     */
    private static class UnpackResult {
        ArrayList<Integer> values;
        int bytesConsumed;
        UnpackResult(ArrayList<Integer> v, int c) { values = v; bytesConsumed = c; }
    }

//    private static UnpackResult bitUnpackToList(byte[] encoded, int pos, int width, int count) {
//        ArrayList<Integer> out = new ArrayList<>(count);
//        long buffer = 0L;
//        int totalBits = 0;
//        int idx = pos;
//        int consumed = 0;
//        long mask = (width >= 63) ? -1L : ((1L << width) - 1L);
//
//        while (out.size() < count) {
//            // 确保 buffer 中至少有 width 位
//            while (totalBits < width) {
//                if (idx >= encoded.length) {
//                    // 输入不足 —— 返回当前已解出的
//                    return new UnpackResult(out, consumed);
//                }
//                buffer = (buffer << 8) | (encoded[idx] & 0xFFL);
//                idx++;
//                consumed++;
//                totalBits += 8;
//            }
//            int shift = totalBits - width;
//            long val = (buffer >>> shift) & mask;
//            out.add((int) val);
//            totalBits -= width;
//            if (totalBits == 0) {
//                buffer = 0L;
//            } else {
//                buffer = buffer & ((1L << totalBits) - 1L);
//            }
//        }
//        return new UnpackResult(out, consumed);
//    }

    private static byte[] bitPackList(ArrayList<Long> values, int width) {
        if (values == null || values.isEmpty()) return new byte[0];

        // 计算需要的字节数
        int totalBits = values.size() * width;
        int totalBytes = (totalBits + 7) / 8;
        byte[] encoded_result = new byte[totalBytes];

        long bitBuffer = 0L;
        int bitCount = 0;
        int bytePos = 0;
        long mask = (width >= 63) ? -1L : ((1L << width) - 1L);

        for (long value : values) {
            // 获取当前值的低width位
            long v = value & mask;

            // 将值的位放入缓冲区
            bitBuffer = (bitBuffer << width) | v;
            bitCount += width;

            // 每当缓冲区有8位或更多时，输出一个字节
            while (bitCount >= 8) {
                int shift = bitCount - 8;
                int b = (int) ((bitBuffer >>> shift) & 0xFFL);
                encoded_result[bytePos++] = (byte) b;
                bitCount -= 8;

                // 清除已输出的高位
                if (bitCount == 0) {
                    bitBuffer = 0L;
                } else {
                    bitBuffer = bitBuffer & ((1L << bitCount) - 1L);
                }
            }
        }

        // 处理剩余的不足一字节的位
        if (bitCount > 0) {
            // 左移剩余的位使其对齐到字节的最高位
            int b = (int) ((bitBuffer << (8 - bitCount)) & 0xFFL);
            encoded_result[bytePos] = (byte) b;
        }

        return encoded_result;
    }

    /**
     * 使用位缓冲的解包版本（与bitPackList对应）
     */
    private static UnpackResult bitUnpackToList(byte[] encoded, int pos, int width, int count) {
        ArrayList<Integer> out = new ArrayList<>(count);
        long buffer = 0L;
        int totalBits = 0;
        int idx = pos;
        int consumed = 0;
        long mask = (width >= 63) ? -1L : ((1L << width) - 1L);

        while (out.size() < count) {
            // 确保 buffer 中至少有 width 位
            while (totalBits < width) {
                if (idx >= encoded.length) {
                    // 输入不足 —— 返回当前已解出的
                    return new UnpackResult(out, consumed);
                }
                buffer = (buffer << 8) | (encoded[idx] & 0xFFL);
                idx++;
                consumed++;
                totalBits += 8;
            }
            int shift = totalBits - width;
            long val = (buffer >>> shift) & mask;
            out.add((int) val);
            totalBits -= width;
            if (totalBits == 0) {
                buffer = 0L;
            } else {
                buffer = buffer & ((1L << totalBits) - 1L);
            }
        }
        return new UnpackResult(out, consumed);
    }
    // -------------------- 动态分包（针对每组 group_size 个值的 group-bitwidth 序列） --------------------
    /**
     * encodeDynamicPacking:
     * 输出格式:
     * [0] packCount (1 byte)  （原来你写成 header 1 byte）
     * for each pack:
     *   [1 byte] packBitWidth
     *   [1 byte] numGroups  (每组包含 packSize 个值)
     *   [payload bytes] bit-packed (numGroups * packSize values) using packBitWidth
     *
     * 说明：为了和你现有调用兼容，header 使用了 1 字节 packCount（如需更大 packCount，请改为 4 字节）。
     */
    public static byte[] encodeDynamicPacking(long[] paddedArray, int packSize, int qmbdLen) {
        if (paddedArray == null) return new byte[0];
        int totalGroups = paddedArray.length / packSize;

        ArrayList<Long> queueValues = new ArrayList<>();
        int maxBD = 0;
        final int SUBHEADER_BITS_ESTIMATE = 40; // 5 bytes header ~= 40 bits

        List<byte[]> packPayloads = new ArrayList<>();
        List<Integer> packNumGroups = new ArrayList<>();
        List<Integer> packBitWidths = new ArrayList<>();

        for (int g = 0; g < totalGroups; g++) {
            int start = g * packSize;
            long groupMax = 0;
            for (int k = 0; k < packSize; k++) {
                long val = paddedArray[start + k];
                if (val > groupMax) groupMax = val;
            }
            int groupBD = getBitWidth(groupMax);

            if (queueValues.isEmpty()) {
                for (int k = 0; k < packSize; k++) queueValues.add(paddedArray[start + k]);
                maxBD = groupBD;
                if (queueValues.size() / packSize >= qmbdLen) {
                    int groupCount = queueValues.size() / packSize;
                    byte[] payload = bitPackList(queueValues, Math.max(1, maxBD));
                    packPayloads.add(payload);
                    packNumGroups.add(groupCount);
                    packBitWidths.add(Math.max(1, maxBD));
                    queueValues.clear();
                    maxBD = 0;
                }
                continue;
            }

            if (groupBD > maxBD) {
                int wastedBits = (groupBD - maxBD) * queueValues.size();
                if (wastedBits >= SUBHEADER_BITS_ESTIMATE || (queueValues.size() / packSize) >= qmbdLen) {
                    int groupCount = queueValues.size() / packSize;
                    byte[] payload = bitPackList(queueValues, Math.max(1, maxBD));
                    packPayloads.add(payload);
                    packNumGroups.add(groupCount);
                    packBitWidths.add(Math.max(1, maxBD));
                    queueValues.clear();
                    maxBD = 0;

                    for (int k = 0; k < packSize; k++) queueValues.add(paddedArray[start + k]);
                    maxBD = groupBD;
                } else {
                    for (int k = 0; k < packSize; k++) queueValues.add(paddedArray[start + k]);
                    maxBD = Math.max(maxBD, groupBD);
                    if ((queueValues.size() / packSize) >= qmbdLen) {
                        int groupCount = queueValues.size() / packSize;
                        byte[] payload = bitPackList(queueValues, Math.max(1, maxBD));
                        packPayloads.add(payload);
                        packNumGroups.add(groupCount);
                        packBitWidths.add(Math.max(1, maxBD));
                        queueValues.clear();
                        maxBD = 0;
                    }
                }
            } else {
                for (int k = 0; k < packSize; k++) queueValues.add(paddedArray[start + k]);
                if ((queueValues.size() / packSize) >= qmbdLen) {
                    int groupCount = queueValues.size() / packSize;
                    byte[] payload = bitPackList(queueValues, Math.max(1, maxBD));
                    packPayloads.add(payload);
                    packNumGroups.add(groupCount);
                    packBitWidths.add(Math.max(1, maxBD));
                    queueValues.clear();
                    maxBD = 0;
                }
            }
        }

        if (!queueValues.isEmpty()) {
            int groupCount = queueValues.size() / packSize;
            byte[] payload = bitPackList(queueValues, Math.max(1, maxBD));
            packPayloads.add(payload);
            packNumGroups.add(groupCount);
            packBitWidths.add(Math.max(1, maxBD));
            queueValues.clear();
            maxBD = 0;
        }

//        // 写出最终 byte[]
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        int packCount = packPayloads.size();
//        // 1 byte packCount
//        out.write(packCount & 0xFF);
//        try {
//            for (int i = 0; i < packCount; i++) {
//                int bw = packBitWidths.get(i);
//                int ng = packNumGroups.get(i);
//                out.write((byte) (bw & 0xFF));
//                out.write((byte) (ng & 0xFF));
//                out.write(packPayloads.get(i));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        byte[] result = new byte[out.toByteArray().length];
//        for (int i = 0; i < result.length; i++) {
//            result[i] = out.toByteArray()[i];
//        }
//        return result;
        int packCount = packPayloads.size();

// 先计算总长度：1 byte packCount + for each pack (1 byte bw + 1 byte numGroups + payload length)
        int totalLen = 1; // packCount
        for (int i = 0; i < packCount; i++) {
            totalLen += 1; // bit width
            totalLen += 1; // numGroups
            totalLen += packPayloads.get(i).length; // payload
        }

// 分配数组并填充
        byte[] result = new byte[totalLen];
        int pos = 0;
        result[pos++] = (byte) (packCount & 0xFF);

        for (int i = 0; i < packCount; i++) {
            int bw = packBitWidths.get(i);
            int ng = packNumGroups.get(i);
            byte[] payload = packPayloads.get(i);

            result[pos++] = (byte) (bw & 0xFF);
            result[pos++] = (byte) (ng & 0xFF);
//            for (byte b : payload) {
//                result[pos++] = (byte) (b & 0xFF);
//            }
            for (int bi = 0; bi < payload.length; bi++) {
                result[pos + bi] = payload[bi];
            }
//            System.arraycopy(payload, 0, result, pos, payload.length);
            pos += payload.length;
        }

// 返回结果
        return result;
    }

//     decode 对应的 encodeDynamicPacking 格式
    public static long[] decodeDynamicPacking(byte[] encoded, int packSize, int originalLength) {
        if (encoded == null || encoded.length < 1) return new long[0];
        int pos = 0;
        int packCount = bytesToPackCount(encoded, pos); pos += 1;

        ArrayList<Integer> all = new ArrayList<>(originalLength);
        for (int p = 0; p < packCount; p++) {
            if (pos >= encoded.length) break;
            int bw = encoded[pos++] & 0xFF;
            int numGroups = bytesToPackCount(encoded, pos); pos += 1;
            int numValues = numGroups * packSize;
            UnpackResult ur = bitUnpackToList(encoded, pos, bw, numValues);
            all.addAll(ur.values);
            pos += ur.bytesConsumed;
        }
        long[] out = new long[originalLength];
        for (int i = 0; i < originalLength && i < all.size(); i++) out[i] = all.get(i);
        if (all.size() < originalLength) {
            for (int i = all.size(); i < originalLength; i++) out[i] = 0;
        }
        return out;
    }

    // -------------------- 主测试（参考你原 main） --------------------
    @Test
    public void BDC() throws IOException {
        System.out.println("\nPerformance Testing (Dynamic pack over 8-values groups)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC";
        File outputDir = new File(outputDirstr);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        if (!dir.exists()) {
            System.err.println("Directory not found: " + directory);
            return;
        }

        final int groupSize = 4; // 每8个值一个 group
        final int qmbdLen = 16;  // QMBD 队列上限（可调）

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
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
            csvReader.close();

            int time_of_repeat = 10;
            long modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0;

            for (int j = 0; j < time_of_repeat; j++) {
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long startTime = System.nanoTime();
                    long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);


                    // pad to multiple of groupSize
                    int remainder = scaledInts.length % groupSize;
                    int paddingLength = (remainder == 0) ? 0 : groupSize - remainder;
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
//                    int groups = paddedArray.length / groupSize;
//                    int[] bitWidths = new int[groups];
//                    int gidx = 0;
//                    for (int si = 0; si < paddedArray.length; si += groupSize) {
//                        long maxInGroup = 0;
//                        for (int sj = si; sj < si + groupSize; ++sj) {
//                            long v = paddedArray[sj];
//                            if (v > maxInGroup) maxInGroup = v;
//                        }
//                        int bitWidth = 0;
//                        if (maxInGroup > 0) {
//                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
//                        } else {
//                            bitWidth = 0;
//                        }
//                        bitWidths[gidx++] = bitWidth;
//                    }
                    // encode using dynamic packing over groups of size 8
                    byte[] compressed = encodeDynamicPacking(paddedArray, groupSize, qmbdLen);
                    long duration = System.nanoTime() - startTime;
                    modelTime += duration;
                    modelCost += (long) compressed.length * 8L;

                    // decode time
                    long startDec = System.nanoTime();
                    long[] decoded = decodeDynamicPacking(compressed, groupSize,paddedArray.length);
                    long decDur = System.nanoTime() - startDec;
                    modelDecodeTime += decDur;

                }
            }

            modelCost = modelCost / time_of_repeat;
            modelTime = modelTime / time_of_repeat;
            modelDecodeTime = modelDecodeTime / time_of_repeat;

            double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
            double modelTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelTime == 0 ? 1 : modelTime);
            double modelDecodeTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelDecodeTime == 0 ? 1 : modelDecodeTime);

            String[] record = {
                    file.toString(),
                    "BP_dynamic",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelDecodeTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();

            System.out.println("Encoding throughput: " + modelTime_throughput + " MB/s");
            System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " MB/s");
            System.out.println("Compression ratio: " + model_ratio);
        }
    }

    @Test
    public void testLosslessCompression() throws IOException {
        System.out.println("\nTesting Lossless Compression for BDC...");

        // 测试1: 使用随机数据
        Random rand = new Random(42);
        int[] testData = new int[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = rand.nextInt(10000);
        }

        int groupSize = 8;
        int qmbdLen = 8;

        // 填充数据，使其长度为groupSize的倍数
        int remainder = testData.length % groupSize;
        int paddingLength = (remainder == 0) ? 0 : groupSize - remainder;
        long[] paddedArray = new long[testData.length + paddingLength];
        System.arraycopy(testData, 0, paddedArray, 0, testData.length);

        System.out.println("Original data length: " + testData.length);
        System.out.println("Padded data length: " + paddedArray.length);

        // 编码
        long startTime = System.nanoTime();
        byte[] compressed = encodeDynamicPacking(paddedArray, groupSize, qmbdLen);
        long encodeTime = System.nanoTime() - startTime;

        System.out.println("Compressed size: " + compressed.length + " bytes");
        System.out.println("Compression ratio: " +
                String.format("%.2f", (double)(testData.length * 4) / compressed.length) + "x");
        System.out.println("Encode time: " + (encodeTime / 1000000.0) + " ms");

        // 解码
        startTime = System.nanoTime();
        long[] decoded = decodeDynamicPacking(compressed, groupSize, paddedArray.length);
        long decodeTime = System.nanoTime() - startTime;

        System.out.println("Decode time: " + (decodeTime / 1000000.0) + " ms");

        // 验证无损性（只比较原始长度部分）
        boolean lossless = true;
        for (int i = 0; i < testData.length; i++) {
            if (testData[i] != decoded[i]) {
                System.err.println("Loss detected at index " + i +
                        ": original=" + testData[i] + ", decoded=" + decoded[i]);
                lossless = false;
                break;
            }
        }

        if (lossless) {
            System.out.println("✓ Lossless compression verified!");
        } else {
            System.out.println("✗ Loss detected!");
        }

        // 测试2: 使用小数据集
        System.out.println("\nTesting with small dataset...");
        long[] smallData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        int smallRemainder = smallData.length % groupSize;
        int smallPaddingLength = (smallRemainder == 0) ? 0 : groupSize - smallRemainder;
        long[] smallPadded = new long[smallData.length + smallPaddingLength];
        System.arraycopy(smallData, 0, smallPadded, 0, smallData.length);

        byte[] smallCompressed = encodeDynamicPacking(smallPadded, groupSize, qmbdLen);
        long[] smallDecoded = decodeDynamicPacking(smallCompressed, groupSize, smallPadded.length);

        lossless = true;
        for (int i = 0; i < smallData.length; i++) {
            if (smallData[i] != smallDecoded[i]) {
                lossless = false;
                break;
            }
        }

        System.out.println("Small dataset lossless: " + (lossless ? "✓" : "✗"));
        System.out.println("All tests completed!");
    }

    @Test
    public void TestVariablePackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable pack sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Pack Size",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 1;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定chunk size为1024
            int chunkSize = 1024;
            int qmbdLen = 64;

            // 测试不同的pack size: 2^0 到 2^9 (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
            for (int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int packSize = (int) Math.pow(2, pack_size_exp);

                System.out.println("Testing pack size: " + packSize + " (2^" + pack_size_exp + ")");

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }
                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        long decodeDuration = System.nanoTime() - decodeStartTime;

                        // 验证无损性（只比较原始长度部分）
                        boolean lossless = true;
                        for (int k = 0; k < scaledInts.length; k++) {
                            if (scaledInts[k] != decoded[k]) {
                                lossless = false;
                                break;
                            }
                        }
                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（MB/s）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(4000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（MB/s）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(packSize),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable pack size testing completed!");
    }
    @Test
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8, 1024*8};

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Pack Size",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 10; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定pack size为8，QMBD长度也为8
            int packSize = 1;
            int qmbdLen = 32;

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }
                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        long decodeDuration = System.nanoTime() - decodeStartTime;

                        // 验证无损性（只比较原始长度部分）
                        boolean lossless = true;
                        for (int k = 0; k < scaledInts.length; k++) {
                            if (scaledInts[k] != decoded[k]) {
                                lossless = false;
                                break;
                            }
                        }
                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(packSize),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable chunk size testing completed!");
    }

    @Test
    public void ZigzagBDC() throws IOException {
        System.out.println("\nPerformance Testing (Dynamic pack over 8-values groups)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_Zigzag";
        File outputDir = new File(outputDirstr);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        if (!dir.exists()) {
            System.err.println("Directory not found: " + directory);
            return;
        }

        final int groupSize = 8; // 每8个值一个 group
        final int qmbdLen = 16;  // QMBD 队列上限（可调）

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
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
            csvReader.close();

            int time_of_repeat = 10;
            long modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0;

            for (int j = 0; j < time_of_repeat; j++) {
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long startTime = System.nanoTime();
                    long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);
                    long[] scaledInts = zigzag(scaledInt);


                    // pad to multiple of groupSize
                    int remainder = scaledInts.length % groupSize;
                    int paddingLength = (remainder == 0) ? 0 : groupSize - remainder;
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
//                    int groups = paddedArray.length / groupSize;
//                    int[] bitWidths = new int[groups];
//                    int gidx = 0;
//                    for (int si = 0; si < paddedArray.length; si += groupSize) {
//                        long maxInGroup = 0;
//                        for (int sj = si; sj < si + groupSize; ++sj) {
//                            long v = paddedArray[sj];
//                            if (v > maxInGroup) maxInGroup = v;
//                        }
//                        int bitWidth = 0;
//                        if (maxInGroup > 0) {
//                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
//                        } else {
//                            bitWidth = 0;
//                        }
//                        bitWidths[gidx++] = bitWidth;
//                    }
                    // encode using dynamic packing over groups of size 8
                    byte[] compressed = encodeDynamicPacking(paddedArray, groupSize, qmbdLen);
                    long duration = System.nanoTime() - startTime;
                    modelTime += duration;
                    modelCost += (long) compressed.length * 8L;

                    // decode time
                    long startDec = System.nanoTime();
                    long[] decoded = decodeDynamicPacking(compressed, groupSize,paddedArray.length);
                    long decDur = System.nanoTime() - startDec;
                    modelDecodeTime += decDur;

                }
            }

            modelCost = modelCost / time_of_repeat;
            modelTime = modelTime / time_of_repeat;
            modelDecodeTime = modelDecodeTime / time_of_repeat;

            double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
            double modelTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelTime == 0 ? 1 : modelTime);
            double modelDecodeTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelDecodeTime == 0 ? 1 : modelDecodeTime);

            String[] record = {
                    file.toString(),
                    "BP_dynamic",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelDecodeTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();

            System.out.println("Encoding throughput: " + modelTime_throughput + " MB/s");
            System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " MB/s");
            System.out.println("Compression ratio: " + model_ratio);
        }
    }

    @Test
    public void ZigzagVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_Zigzag_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable pack sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Pack Size",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 1;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定chunk size为1024
            int chunkSize = 1024;
            int qmbdLen = 256;

            // 测试不同的pack size: 2^0 到 2^9 (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
            for (int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int packSize = (int) Math.pow(2, pack_size_exp);

                System.out.println("Testing pack size: " + packSize + " (2^" + pack_size_exp + ")");

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        long[] scaledInts = zigzag(scaledInt);

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }
                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        decoded = zigzagDecode(decoded);
                        long decodeDuration = System.nanoTime() - decodeStartTime;

                        // 验证无损性（只比较原始长度部分）
                        boolean lossless = true;
                        for (int k = 0; k < scaledInts.length; k++) {
                            if (scaledInts[k] != decoded[k]) {
                                lossless = false;
                                break;
                            }
                        }
                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(1000000000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.multiply(BigDecimal.valueOf(8)).divide(BigDecimal.valueOf(1000000)).divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(packSize),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable pack size testing completed!");
    }

    @Test
    public void ZigzagVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_Zigzag_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8, 1024*8};

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Pack Size",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 1; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定pack size为8，QMBD长度也为8
            int packSize = 8;
            int qmbdLen = 64;

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        long[] scaledInts = zigzag(scaledInt);
                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }
                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        long decodeDuration = System.nanoTime() - decodeStartTime;

                        // 验证无损性（只比较原始长度部分）
                        boolean lossless = true;
                        for (int k = 0; k < scaledInts.length; k++) {
                            if (scaledInts[k] != decoded[k]) {
                                lossless = false;
                                break;
                            }
                        }
                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(8000), 10, RoundingMode.HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, RoundingMode.HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(packSize),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable chunk size testing completed!");
    }

    @Test
    public void SprintzBDC() throws IOException {
        System.out.println("\nPerformance Testing (Dynamic pack over 8-values groups)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_Sprintz";
        File outputDir = new File(outputDirstr);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        if (!dir.exists()) {
            System.err.println("Directory not found: " + directory);
            return;
        }

        final int groupSize = 1; // 每8个值一个 group
        final int qmbdLen = 8;  // QMBD 队列上限（可调）

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
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
            csvReader.close();

            int time_of_repeat = 10;
            long modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0;

            for (int j = 0; j < time_of_repeat; j++) {
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long startTime = System.nanoTime();
                    long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);
                    SprintzEncodedResult ser = sprintz(scaledInt);
                    long[] scaledInts = ser.getEncodedData();


                    // pad to multiple of groupSize
                    int remainder = scaledInts.length % groupSize;
                    int paddingLength = (remainder == 0) ? 0 : groupSize - remainder;
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
//                    int groups = paddedArray.length / groupSize;
//                    int[] bitWidths = new int[groups];
//                    int gidx = 0;
//                    for (int si = 0; si < paddedArray.length; si += groupSize) {
//                        long maxInGroup = 0;
//                        for (int sj = si; sj < si + groupSize; ++sj) {
//                            long v = paddedArray[sj];
//                            if (v > maxInGroup) maxInGroup = v;
//                        }
//                        int bitWidth = 0;
//                        if (maxInGroup > 0) {
//                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
//                        } else {
//                            bitWidth = 0;
//                        }
//                        bitWidths[gidx++] = bitWidth;
//                    }
                    // encode using dynamic packing over groups of size 8
                    byte[] compressed = encodeDynamicPacking(paddedArray, groupSize, qmbdLen);
                    long duration = System.nanoTime() - startTime;
                    modelTime += duration;
                    modelCost += (long) compressed.length * 8L;

                    // decode time
                    long startDec = System.nanoTime();
                    long[] decoded = decodeDynamicPacking(compressed, groupSize,paddedArray.length);
                    long[] result = sprintzDecode(decoded, ser.getFirstValue());
                    long decDur = System.nanoTime() - startDec;
                    modelDecodeTime += decDur;

                }
            }

            modelCost = modelCost / time_of_repeat;
            modelTime = modelTime / time_of_repeat;
            modelDecodeTime = modelDecodeTime / time_of_repeat;

            double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
            double modelTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelTime == 0 ? 1 : modelTime);
            double modelDecodeTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelDecodeTime == 0 ? 1 : modelDecodeTime);

            String[] record = {
                    file.toString(),
                    "BP_dynamic",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelDecodeTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();

            System.out.println("Encoding throughput: " + modelTime_throughput + " MB/s");
            System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " MB/s");
            System.out.println("Compression ratio: " + model_ratio);
        }
    }

    @Test
    public void SprintzVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_Sprintz_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable pack sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Pack Size",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 1;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定chunk size为1024
            int chunkSize = 1024;
            int qmbdLen = 64;

            // 测试不同的pack size: 2^0 到 2^9 (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
            for (int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int packSize = (int) Math.pow(2, pack_size_exp);

                System.out.println("Testing pack size: " + packSize + " (2^" + pack_size_exp + ")");

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        SprintzEncodedResult ser = sprintz(scaledInt);
                        long[] scaledInts = ser.getEncodedData();

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }

                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        long[] result = sprintzDecode(decoded, ser.getFirstValue());
                        long decodeDuration = System.nanoTime() - decodeStartTime;

                        // 验证无损性（只比较原始长度部分）
                        boolean lossless = true;
                        for (int k = 0; k < scaledInts.length; k++) {
                            if (scaledInts[k] != decoded[k]) {
                                lossless = false;
                                break;
                            }
                        }
                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(packSize),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable pack size testing completed!");
    }

    @Test
    public void SprintzVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_Sprintz_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8, 1024*8};

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Pack Size",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 1; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定pack size为8，QMBD长度也为8
            int packSize = 8;
            int qmbdLen = 64;

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        SprintzEncodedResult ser = sprintz(scaledInt);
                        long[] scaledInts = ser.getEncodedData();

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }
                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        long[] result = sprintzDecode(decoded,ser.getFirstValue());
                        long decodeDuration = System.nanoTime() - decodeStartTime;

//                        // 验证无损性（只比较原始长度部分）
//                        boolean lossless = true;
//                        for (int k = 0; k < scaledInts.length; k++) {
//                            if (scaledInts[k] != decoded[k]) {
//                                lossless = false;
//                                break;
//                            }
//                        }
//                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, RoundingMode.HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(packSize),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable chunk size testing completed!");
    }

    @Test
    public void TS2DIFFBDC() throws IOException {
        System.out.println("\nPerformance Testing (Dynamic pack over 8-values groups)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_TS2DIFF";
        File outputDir = new File(outputDirstr);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        if (!dir.exists()) {
            System.err.println("Directory not found: " + directory);
            return;
        }

        final int groupSize = 1; // 每8个值一个 group
        final int qmbdLen = 8;  // QMBD 队列上限（可调）

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
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
            csvReader.close();

            int time_of_repeat = 10;
            long modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0;

            for (int j = 0; j < time_of_repeat; j++) {
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long startTime = System.nanoTime();
                    long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);
                    TSDIFFEncodedResult ter = ts2diff(scaledInt);
                    long[] scaledInts = ter.getEncodedData();


                    // pad to multiple of groupSize
                    int remainder = scaledInts.length % groupSize;
                    int paddingLength = (remainder == 0) ? 0 : groupSize - remainder;
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
//                    int groups = paddedArray.length / groupSize;
//                    int[] bitWidths = new int[groups];
//                    int gidx = 0;
//                    for (int si = 0; si < paddedArray.length; si += groupSize) {
//                        long maxInGroup = 0;
//                        for (int sj = si; sj < si + groupSize; ++sj) {
//                            long v = paddedArray[sj];
//                            if (v > maxInGroup) maxInGroup = v;
//                        }
//                        int bitWidth = 0;
//                        if (maxInGroup > 0) {
//                            bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
//                        } else {
//                            bitWidth = 0;
//                        }
//                        bitWidths[gidx++] = bitWidth;
//                    }
                    // encode using dynamic packing over groups of size 8
                    byte[] compressed = encodeDynamicPacking(paddedArray, groupSize, qmbdLen);
                    long duration = System.nanoTime() - startTime;
                    modelTime += duration;
                    modelCost += (long) compressed.length * 8L;

                    // decode time
                    long startDec = System.nanoTime();
                    long[] decoded = decodeDynamicPacking(compressed, groupSize,paddedArray.length);
                    long[] result = ts2diffDecode(decoded, ter.getFirstValue(),ter.getMinDiff());
                    long decDur = System.nanoTime() - startDec;
                    modelDecodeTime += decDur;

                }
            }

            modelCost = modelCost / time_of_repeat;
            modelTime = modelTime / time_of_repeat;
            modelDecodeTime = modelDecodeTime / time_of_repeat;

            double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
            double modelTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelTime == 0 ? 1 : modelTime);
            double modelDecodeTime_throughput = (double) (numbers.size() * 8000L) / (double) (modelDecodeTime == 0 ? 1 : modelDecodeTime);

            String[] record = {
                    file.toString(),
                    "BP_dynamic",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelDecodeTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();

            System.out.println("Encoding throughput: " + modelTime_throughput + " MB/s");
            System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " MB/s");
            System.out.println("Compression ratio: " + model_ratio);
        }
    }

    @Test
    public void TS2DIFFVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_TS2DIFF_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable pack sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Pack Size",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 1;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定chunk size为1024
            int chunkSize = 1025;
            int qmbdLen = 64;

            // 测试不同的pack size: 2^0 到 2^9 (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
            for (int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int packSize = (int) Math.pow(2, pack_size_exp);

                System.out.println("Testing pack size: " + packSize + " (2^" + pack_size_exp + ")");

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        TSDIFFEncodedResult ter = ts2diff(scaledInt);
                        long[] scaledInts = ter.getEncodedData();

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }

                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        long[] result = ts2diffDecode(decoded, ter.getFirstValue(),ter.getMinDiff());
                        long decodeDuration = System.nanoTime() - decodeStartTime;

                        // 验证无损性（只比较原始长度部分）
                        boolean lossless = true;
                        for (int k = 0; k < scaledInts.length; k++) {
                            if (scaledInts[k] != decoded[k]) {
                                lossless = false;
                                break;
                            }
                        }
                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(packSize),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable pack size testing completed!");
    }

    @Test
    public void TS2DIFFVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BDC_TS2DIFF_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8, 1024*8};

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Pack Size",
                    "QMBD Length",
                    "Lossless Verified"
            };
            writer.writeRecord(head);

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
            csvReader.close();

            if (numbers.isEmpty()) {
                System.out.println("Warning: No data in file " + file.getName());
                writer.close();
                continue;
            }

            int time_of_repeat = 1; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定pack size为8，QMBD长度也为8
            int packSize = 1;
            int qmbdLen = 64;

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                chunkSize += 1;
                System.out.println("Testing chunk size: " + chunkSize);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                boolean allLossless = true;

                for (int j = 0; j < time_of_repeat; j++) {
                    BigDecimal totalCost = BigDecimal.ZERO;

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        List<String> chunkNumbers = numbers.subList(i, end);

                        if (chunkNumbers.size() < 2) continue;

                        // 缩放数据
                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        TSDIFFEncodedResult ter = ts2diff(scaledInt);
                        long[] scaledInts = ter.getEncodedData();


                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / packSize]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += packSize) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + packSize; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / packSize] = bitWidth;
                        }
                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        long[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        long[] originalData = ts2diffDecode(decoded,ter.getFirstValue(),ter.getMinDiff());
                        long decodeDuration = System.nanoTime() - decodeStartTime;

//                        // 验证无损性（只比较原始长度部分）
//                        boolean lossless = true;
//                        for (int k = 0; k < scaledInts.length; k++) {
//                            if (scaledInts[k] != decoded[k]) {
//                                lossless = false;
//                                break;
//                            }
//                        }
//                        if (!lossless) allLossless = false;

                        // 累加统计
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(BigDecimal.valueOf(compressed.length * 8L));
                    }
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                // 计算压缩比和吞吐量
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal totalBits = numbersSizeBD.multiply(BigDecimal.valueOf(64)); // 原始数据每个值64位
                BigDecimal modelRatio = modelCost.divide(totalBits, 10, BigDecimal.ROUND_HALF_UP);

                // 编码吞吐量（points/ms）
                BigDecimal modelTimeThroughput = BigDecimal.ZERO;
                if (modelTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(8000), 10, BigDecimal.ROUND_HALF_UP);
                    decodeThroughput = numbersSizeBD.divide(modelDecodeTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize-1),
                        file.toString(),
                        "BDC",
                        modelTimeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        modelRatio.toPlainString(),
                        String.valueOf(packSize),
                        String.valueOf(qmbdLen),
                        String.valueOf(allLossless)
                };
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Completed testing for file: " + file.getName());
        }

        System.out.println("Variable chunk size testing completed!");
    }

}
