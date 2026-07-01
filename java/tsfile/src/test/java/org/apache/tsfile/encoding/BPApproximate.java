package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BPApproximate {
    private static final int CHUNK_SIZE = 1024;
    private static final int BASELINE_PACK_SIZE = 8;

    // 压缩方案标记
    static final byte SCHEME_BASELINE = 0;  // 使用baseline bitpacking
    static final byte SCHEME_RLE = 1;       // 使用RLE分段压缩

    // 段数据结构
    static class Segment {
        int bitWidth;  // 段的位宽（0-63）
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
            // 段的成本包括段头成本(16bits)和数据存储成本
            return 16 + bitWidth * length * packSize;
        }
    }

    // 压缩结果
    static class CompressionResult {
        byte[] data;
        byte scheme;  // 0: baseline, 1: RLE
        int compressedSize;
        double compressionRatio;
        int packSize; // 使用的packsize

        CompressionResult(byte[] data, byte scheme, int packSize) {
            this.data = data;
            this.scheme = scheme;
            this.compressedSize = data.length * 8;
            this.packSize = packSize;
        }
    }

    // 分段评估结果
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


    // 智能选择编码方案
    private static CompressionResult encodeWithSmartSelection(long[] data, int packSize) {
        // 计算baseline方案的压缩成本
        BaselineCostResult baselineResult = calculateBaselineCost(data);

        // 计算RLE方案的压缩成本
        int rleCost = calculateRLECost(data, packSize);

        // 选择成本更小的方案
        if (baselineResult.cost <= rleCost) {
            // 使用baseline方案
            byte[] compressedData = encodeWithBaseline(data, baselineResult.packSize);
            return new CompressionResult(compressedData, SCHEME_BASELINE, baselineResult.packSize);
        } else {
            // 使用RLE方案
            byte[] compressedData = encodeWithRLE(data, packSize);
            return new CompressionResult(compressedData, SCHEME_RLE, packSize);
        }
    }

    // Baseline成本结果类
    static class BaselineCostResult {
        int cost;
        int packSize;

        BaselineCostResult(int cost, int packSize) {
            this.cost = cost;
            this.packSize = packSize;
        }
    }

    // 计算baseline方案的压缩成本，尝试packsize 8和16
    private static BaselineCostResult calculateBaselineCost(long[] data) {
        int bestCost = Integer.MAX_VALUE;
        int bestPackSize = 8;

        // 尝试packsize 8和16
        for (int packSize : new int[]{8, 16}) {
            int groupCount = (data.length + packSize - 1) / packSize;

            // baseline成本包括：方案标记(8bits) + packsize标记(8bits) + 每个组的位宽(每个组8bits) + 数据位
            int headerCost = 16; // 2字节 = 16bits (方案标记和packsize标记)
            int groupHeadersCost = groupCount * 8; // 每个组一个字节存储位宽

            // 计算每个组的位宽和
            int totalBitWidth = 0;
            for (int i = 0; i < groupCount; i++) {
                int startIdx = i * packSize;
                int endIdx = Math.min(startIdx + packSize, data.length);
                long maxVal = 0;

                for (int j = startIdx; j < endIdx; j++) {
                    if (data[j] > maxVal) {
                        maxVal = data[j];
                    }
                }
                int bitWidth = 64 - Long.numberOfLeadingZeros(maxVal);
                totalBitWidth += bitWidth * (endIdx - startIdx);
            }

            int currentCost = headerCost + groupHeadersCost + totalBitWidth;

            if (currentCost < bestCost) {
                bestCost = currentCost;
                bestPackSize = packSize;
            }
        }

        return new BaselineCostResult(bestCost, bestPackSize);
    }

    // 计算RLE方案的压缩成本
    private static int calculateRLECost(long[] data, int packSize) {
        // 获取分段信息
        SegmentationResult segResult = getSegmentationInfo(data, packSize);

        // RLE成本：1字节（方案标记）+ 分段信息 + 数据位
        int headerCost = 8; // 1字节方案标记
        int segmentHeaderCost = 16 + segResult.numSegments * 16; // 2字节段数 + 每段2字节

        return headerCost + segmentHeaderCost + (int)segResult.totalCost;
    }

    // baseline编码方案，支持动态packsize
    private static byte[] encodeWithBaseline(long[] data, int packSize) {
        List<Byte> result = new ArrayList<>();

        // 方案标记
        result.add(SCHEME_BASELINE);

        // 存储packsize标记 (0表示8, 1表示16)
        byte packSizeFlag = (byte) (packSize == 8 ? 0 : 1);
        result.add(packSizeFlag);

        int groupCount = (data.length + packSize - 1) / packSize;
        List<Integer> bitWidths = new ArrayList<>();

        // 计算每个组的位宽并存储
        for (int i = 0; i < groupCount; i++) {
            int startIdx = i * packSize;
            int endIdx = Math.min(startIdx + packSize, data.length);
            long maxVal = 0;

            for (int j = startIdx; j < endIdx; j++) {
                if (data[j] > maxVal) {
                    maxVal = data[j];
                }
            }
            int bitWidth = 64 - Long.numberOfLeadingZeros(maxVal);
            bitWidths.add(bitWidth);
            result.add((byte) bitWidth);
        }

        // 进行bit-packing
        int totalBits = 0;
        for (int i = 0; i < bitWidths.size(); i++) {
            int bitWidth = bitWidths.get(i);
            int valuesInGroup = Math.min(packSize, data.length - i * packSize);
            totalBits += bitWidth * valuesInGroup;
        }

        int totalBytes = (totalBits + 7) / 8;
        byte[] packedData = new byte[totalBytes];

        int currentBytePos = 0;
        int currentBitPos = 0;

        for (int i = 0; i < groupCount; i++) {
            int bitWidth = bitWidths.get(i);
            int startIdx = i * packSize;
            int endIdx = Math.min(startIdx + packSize, data.length);

            for (int j = startIdx; j < endIdx; j++) {
                long value = data[j];
                for (int bit = bitWidth - 1; bit >= 0; bit--) {
                    int currentBit = (int)((value >> bit) & 1);
                    packedData[currentBytePos] |= (currentBit << (7 - currentBitPos));
                    currentBitPos++;
                    if (currentBitPos == 8) {
                        currentBytePos++;
                        currentBitPos = 0;
                    }
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

    // 智能解码方案
    private static long[] decodeWithSmartSelection(byte[] encodedData, int originalLength, int packSize) {
        if (encodedData.length == 0) {
            return new long[originalLength];
        }

        // 读取方案标记
        byte scheme = encodedData[0];

        if (scheme == SCHEME_BASELINE) {
            return decodeBaseline(encodedData, originalLength);
        } else {
            return decodeRLE(encodedData, originalLength, packSize);
        }
    }

    // baseline解码方案，支持动态packsize
    private static long[] decodeBaseline(byte[] encodedData, int originalLength) {
        try {
            if (encodedData.length < 2) {
                return new long[originalLength];
            }

            int pos = 1; // 跳过方案标记

            // 读取packsize标记
            byte packSizeFlag = encodedData[pos++];
            int packSize = packSizeFlag == 0 ? 8 : 16;

            int groupCount = (originalLength + packSize - 1) / packSize;

            // 读取每个组的位宽
            int[] bitWidths = new int[groupCount];
            for (int i = 0; i < groupCount; i++) {
                if (pos >= encodedData.length) break;
                bitWidths[i] = encodedData[pos++] & 0xFF;
            }

            long[] result = new long[originalLength];
            int resultIndex = 0;
            int currentBitPos = 0;

            for (int groupIdx = 0; groupIdx < groupCount && resultIndex < originalLength; groupIdx++) {
                int bitWidth = bitWidths[groupIdx];
                int valuesInGroup = Math.min(packSize, originalLength - resultIndex);

                for (int i = 0; i < valuesInGroup && resultIndex < originalLength; i++) {
                    long value = 0;

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

                    result[resultIndex++] = value;
                }
            }

            return result;
        } catch (Exception e) {
            System.err.println("baseline解码异常: " + e.getMessage());
            e.printStackTrace();
            return new long[originalLength];
        }
    }

    // 获取分段信息
    private static SegmentationResult getSegmentationInfo(long[] data, int packSize) {
        int groupCount = data.length / packSize;
        int[] bitWidths = new int[groupCount];

        for (int i = 0; i < groupCount; i++) {
            long maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                if (data[startIdx + j] > maxInGroup) {
                    maxInGroup = data[startIdx + j];
                }
            }
            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        List<Segment> segments = computeRLESegmentation(bitWidths, packSize);
        return new SegmentationResult(segments, data.length * 64); // 原始大小为每个值64位
    }

    // RLE编码方案
    public static byte[] encodeWithRLE(long[] data, int packSize) {
        List<Byte> result = new ArrayList<>();

        // 添加方案标记
        result.add(SCHEME_RLE);

        // 处理数据，确保长度是packSize的倍数
        int actual_length = data.length;
        int remainder = actual_length % packSize;
        if (remainder != 0) {
            int paddingLength = packSize - remainder;
            long[] paddedArray = new long[actual_length + paddingLength];
            System.arraycopy(data, 0, paddedArray, 0, actual_length);
            data = paddedArray;
            actual_length = paddedArray.length;
        }

        int groupCount = actual_length / packSize;
        int[] bitWidths = new int[groupCount];

        // 计算每个组的bitwidth
        for (int i = 0; i < groupCount; i++) {
            long maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                if (data[startIdx + j] > maxInGroup) {
                    maxInGroup = data[startIdx + j];
                }
            }
            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        // 使用RLE分段算法
        List<Segment> segments = computeRLESegmentation(bitWidths, packSize);

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
                ArrayList<Long> groupData = new ArrayList<>();
                for (int j = 0; j < packSize; j++) {
                    if (startIndex + j < data.length) {
                        groupData.add(data[startIndex + j]);
                    } else {
                        groupData.add(0L);
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

    // RLE分段算法
    private static List<Segment> computeRLESegmentation(int[] bitWidths, int packSize) {
        List<Segment> segments = new ArrayList<>();
        if (bitWidths.length == 0) return segments;

        int currentBitWidth = bitWidths[0];
        int currentLength = 1;

        for (int i = 1; i < bitWidths.length; i++) {
            if (bitWidths[i] == currentBitWidth) {
                currentLength++;
                // 限制最大长度（因为length-1只有10bits，最大1023，所以length最大1024）
                if (currentLength > 1024) {
                    segments.add(new Segment(currentBitWidth, currentLength - 1, packSize));
                    currentBitWidth = bitWidths[i];
                    currentLength = 1;
                }
            } else {
                segments.add(new Segment(currentBitWidth, currentLength, packSize));
                currentBitWidth = bitWidths[i];
                currentLength = 1;
            }
        }
        segments.add(new Segment(currentBitWidth, currentLength, packSize));

        // 尝试合并相邻的段
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
                    // 检查合并后的长度是否超过限制
                    if (mergedLength > 1024) {
                        continue;
                    }
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

    // RLE解码方案
    public static long[] decodeRLE(byte[] encodedData, int originalLength, int packSize) {
        try {
            // 跳过方案标记（第一个字节）
            int startPos = 1;

            // 解码段列表
            List<Segment> segments = decodeSegments(encodedData, startPos);
            if (segments.isEmpty()) {
                return new long[originalLength];
            }

            int pos = startPos + 2 + segments.size() * 2; // 每个段2字节
            long[] result = new long[originalLength];
            int resultIndex = 0;
            int currentBitPos = 0;

            for (Segment segment : segments) {
                if (resultIndex >= originalLength) break;

                int totalValues = segment.length * packSize;
                int valuesToRead = Math.min(totalValues, originalLength - resultIndex);

                for (int i = 0; i < valuesToRead && resultIndex < originalLength; i++) {
                    long value = 0;

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
            return new long[originalLength];
        }
    }

    // 编码段信息：bitWidth用6bits存储，length-1用10bits存储
    private static List<Byte> encodeSegments(List<Segment> segments) {
        List<Byte> result = new ArrayList<>();
        if (segments.isEmpty()) return result;

        int segmentCount = segments.size();
        // 段数用2字节存储
        result.add((byte) (segmentCount >> 8));
        result.add((byte) segmentCount);

        for (Segment segment : segments) {
            // 验证bitWidth范围（0-63）
            if (segment.bitWidth < 0 || segment.bitWidth > 63) {
                throw new IllegalArgumentException("bitWidth超出范围(0-63): " + segment.bitWidth);
            }

            // 验证length范围（1-1024）
            if (segment.length < 1 || segment.length > 1024) {
                throw new IllegalArgumentException("length超出范围(1-1024): " + segment.length);
            }

            // 存储length-1（0-1023）
            int lengthMinusOne = segment.length - 1;

            // 将bitWidth（高6位）和length-1（低10位）组合成一个16位整数
            int combined = (segment.bitWidth << 10) | lengthMinusOne;

            // 存储为2个字节
            result.add((byte) (combined >> 8));
            result.add((byte) combined);
        }

        return result;
    }

    // 解码段信息：从2字节中解码bitWidth和length
    private static List<Segment> decodeSegments(byte[] data, int startPos) {
        List<Segment> segments = new ArrayList<>();
        if (startPos + 2 > data.length) return segments;

        int segmentCount = ((data[startPos] & 0xFF) << 8) | (data[startPos + 1] & 0xFF);
        if (segmentCount <= 0) return segments;

        int pos = startPos + 2;
        for (int i = 0; i < segmentCount && pos + 1 < data.length; i++) {
            // 读取2字节
            int combined = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
            pos += 2;

            // 解码bitWidth（高6位）
            int bitWidth = (combined >> 10) & 0x3F; // 0x3F = 6位掩码

            // 解码length-1（低10位）并加1得到实际length
            int lengthMinusOne = combined & 0x3FF; // 0x3FF = 10位掩码
            int length = lengthMinusOne + 1;

            segments.add(new Segment(bitWidth, length, 0)); // packSize在解码时不重要
        }

        return segments;
    }

    // Bit-packing方法 - 处理long值
    public static int bitPacking(ArrayList<Long> numbers, int start, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int totalCount = numbers.size() - start;
        int currentBytePos = encode_pos;
        int currentBitPos = 0;

        for (int i = 0; i < totalCount; i++) {
            long value = numbers.get(start + i);
            for (int bit = bit_width - 1; bit >= 0; bit--) {
                int currentBit = (int)((value >> bit) & 1);
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

    // 缩放方法 - 返回long数组
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


    // 注意：以下这些方法原本就处理long，所以不需要修改
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

        long[] result = new long[numbers.length];

        // 第一个值保持不变
        long firstValue = numbers[0];
        result[0] = numbers[0];

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
                result[i] = (normalizedDiff << 1) ^ (normalizedDiff >> 31);
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


    @Test
    public void BPApproximate() throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 表头
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Average Segment Length",
                    "Pack Size Used"
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

            int time_of_repeat = 10;

            long modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0;
            int baselineCount = 0;
            int rleCount = 0;
            double avgSegmentLength = 0;
            int avgPackSizeUsed = 0;

            for(int j = 0; j < time_of_repeat; j++){
                int totalCost = 0;
                int totalSegments = 0;
                int totalPackSizeUsed = 0;

                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size() < 8) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledLongs = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();

                    // 使用智能选择方案
                    CompressionResult compResult = encodeWithSmartSelection(scaledLongs, 8);
                    int curCost = compResult.compressedSize;
                    totalPackSizeUsed += compResult.packSize;

                    // 统计方案选择
                    if (compResult.scheme == SCHEME_BASELINE) {
                        baselineCount++;
                    } else {
                        rleCount++;

                        // 获取分段信息
                        SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                        totalSegments += segResult.numSegments;
                    }

                    long duration = System.nanoTime() - startTime;
                    modelTime += duration;
                    modelCost += curCost;

                    // 解码测试
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    modelDecodeTime += decodeDuration;
                }

                avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * 8) : 0;
                avgPackSizeUsed = numbers.size() > 0 ? totalPackSizeUsed / (numbers.size() / CHUNK_SIZE + 1) : 0;
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
                    "RLE_Only",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(modelDecodeTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio),
                    selectedScheme,
                    String.valueOf(avgSegmentLength),
                    String.valueOf(avgPackSizeUsed)
            };
            writer.writeRecord(record);
            writer.close();
        }
    }

    @Test
    public void BPVarPackSize() throws IOException {
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

            // 表头
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
                    "Average Segment Length"
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

                        long[] scaledLongs = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(scaledLongs, pack_size);
                        int cur_cost = compResult.compressedSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        modelTime += duration;
                        modelCost += cur_cost;

                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
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
                        "RLE_Only",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(modelDecodeTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength)
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    @Test
    public void BPVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (RLE)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRLE_vary_m";
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
                    "Average Segment Length",
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

            int time_of_repeat = 10; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

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
            long[] scaledLongs_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledLongs_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                // 固定pack size为8
                int pack_size = 8;

                long modelCost = 0;
                long modelTime = 0;
                long modelDecodeTime = 0;
                int baselineCount = 0;
                int rleCount = 0;
                double avgSegmentLength = 0;
                int avgPackSizeUsed = 0;

                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    int totalSegments = 0;
                    int totalPackSizeUsed = 0;

                    for (int i = 0; i < scaledLongs_all.length; i += chunkSize) {
                        int end = Math.min(i + chunkSize, scaledLongs_all.length);
                        long[] chunkData = new long[end - i];
                        System.arraycopy(scaledLongs_all, i, chunkData, 0, end - i);

                        if (chunkData.length < 8) continue;

                        long startTime = System.nanoTime();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(chunkData, pack_size);
                        int curCost = compResult.compressedSize;
                        totalPackSizeUsed += compResult.packSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(chunkData, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        modelTime += duration;
                        modelCost += curCost;

                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, chunkData.length, compResult.packSize);
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        modelDecodeTime += decodeDuration;
                    }

                    avgSegmentLength = totalSegments > 0 ? (double) scaledLongs_all.length / (totalSegments * pack_size) : 0;
                    avgPackSizeUsed = scaledLongs_all.length > 0 ? totalPackSizeUsed / ((scaledLongs_all.length / chunkSize) + 1) : 0;
                }

                // 计算平均值
                modelCost /= time_of_repeat;
                modelTime = modelTime / time_of_repeat;
                modelDecodeTime = modelDecodeTime / time_of_repeat;

                // 计算压缩比和吞吐量
                double model_ratio = (double) modelCost / (double) (scaledLongs_all.length * 64); // 每个原始值64位
                double modelTime_throughput = modelTime > 0 ? (double) (scaledLongs_all.length * 8000L) / (double) modelTime : 0; // points/ms
                double modelDecodeTime_throughput = modelDecodeTime > 0 ? (double) (scaledLongs_all.length * 8000L) / (double) modelDecodeTime : 0;

                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize),
                        file.toString(),
                        "RLE_Only",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(modelDecodeTime_throughput),
                        String.valueOf(scaledLongs_all.length),
                        String.valueOf(modelCost),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength),
                        String.valueOf(avgPackSizeUsed)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }

    @Test
    public void ZigzagApproximate() throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_Zigzag";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 表头
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Average Segment Length",
                    "Pack Size Used"
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

            int time_of_repeat = 10;

            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal encodeTime = BigDecimal.ZERO;
            BigDecimal decodeTime = BigDecimal.ZERO;
            int baselineCount = 0;
            int rleCount = 0;
            double avgSegmentLength = 0;
            int avgPackSizeUsed = 0;

            for(int j = 0; j < time_of_repeat; j++){
                int totalCost = 0;
                int totalSegments = 0;
                int totalPackSizeUsed = 0;

                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size() < 8) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledLong = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    long[] scaledLongs = zigzag(scaledLong);

                    // 使用智能选择方案
                    CompressionResult compResult = encodeWithSmartSelection(scaledLongs, 8);
                    long curCost = compResult.compressedSize;
                    totalPackSizeUsed += compResult.packSize;

                    // 统计方案选择
                    if (compResult.scheme == SCHEME_BASELINE) {
                        baselineCount++;
                    } else {
                        rleCount++;

                        // 获取分段信息
                        SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                        totalSegments += segResult.numSegments;
                    }

                    long duration = System.nanoTime() - startTime;
//                    modelTime += duration;
//                    modelCost += curCost;
                    encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                    modelCost = modelCost.add(BigDecimal.valueOf(curCost));
                    // 解码测试
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
                    long[] result = zigzagDecode(decodedData);
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
//                    modelDecodeTime += decodeDuration;
                }

                avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * 8) : 0;
                avgPackSizeUsed = !numbers.isEmpty() ? totalPackSizeUsed / (numbers.size() / CHUNK_SIZE + 1) : 0;
            }

            // 计算统计结果
            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

            // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
            // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
            BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
            BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                    .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

            // 计算解码吞吐量
            BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                    .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

            // 确定选择的方案
            String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

            // 输出结果
            String[] record = {
                    file.toString(),
                    "RLE_Only",
                    String.valueOf(encodeThroughput.doubleValue()),
                    String.valueOf(decodeThroughput.doubleValue()),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio.doubleValue()),
                    selectedScheme,
                    String.valueOf(avgSegmentLength),
                    String.valueOf(avgPackSizeUsed)
            };
            writer.writeRecord(record);
            writer.close();
        }
    }


    @Test
    public void ZigzagVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_Zigzag_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + "...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 表头
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
                    "Average Segment Length"
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

            for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                System.out.println("Testing pack size: " + pack_size);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
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

                        long[] scaledLong = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        long[] scaledLongs = zigzag(scaledLong);

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(scaledLongs, pack_size);
                        int cur_cost = compResult.compressedSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(BigDecimal.valueOf(cur_cost));

                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
                        long[] result = zigzagDecode(decodedData);
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                    avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * pack_size) : 0;
                }

                // 计算统计结果
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s


                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                String[] record = {
                        file.toString(),
                        "RLE_Only",
                        String.valueOf(encodeThroughput.doubleValue()),
                        String.valueOf(decodeThroughput.doubleValue()),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength)
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    @Test
    public void ZigzagVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (RLE)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_Zigzag_vary_m";
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
                    "Average Segment Length",
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

            int time_of_repeat = 10; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

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
            long[] scaledLongs_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledLongs_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                // 固定pack size为8
                int pack_size = 8;

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                int baselineCount = 0;
                int rleCount = 0;
                double avgSegmentLength = 0;
                int avgPackSizeUsed = 0;

                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    int totalSegments = 0;
                    int totalPackSizeUsed = 0;

                    for (int i = 0; i < scaledLongs_all.length; i += chunkSize) {
                        int end = Math.min(i + chunkSize, scaledLongs_all.length);
                        long[] scaleInt = new long[end - i];
                        System.arraycopy(scaledLongs_all, i, scaleInt, 0, end - i);

                        if (scaleInt.length < 8) continue;

                        long startTime = System.nanoTime();
                        long[] chunkData = zigzag(scaleInt);

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(chunkData, pack_size);
                        int curCost = compResult.compressedSize;
                        totalPackSizeUsed += compResult.packSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(chunkData, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(BigDecimal.valueOf(curCost));


                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, chunkData.length, compResult.packSize);
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                    avgSegmentLength = totalSegments > 0 ? (double) scaledLongs_all.length / (totalSegments * pack_size) : 0;
                    avgPackSizeUsed = scaledLongs_all.length > 0 ? totalPackSizeUsed / ((scaledLongs_all.length / chunkSize) + 1) : 0;
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s
                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize),
                        file.toString(),
                        "RLE_Zigzag",
                        String.valueOf(encodeThroughput.doubleValue()),
                        String.valueOf(decodeThroughput.doubleValue()),
                        String.valueOf(scaledLongs_all.length),
                        String.valueOf(modelCost),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength),
                        String.valueOf(avgPackSizeUsed)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }

    @Test
    public void SprintzApproximate() throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_Sprintz";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 表头
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Average Segment Length",
                    "Pack Size Used"
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

            int time_of_repeat = 10;

            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal encodeTime = BigDecimal.ZERO;
            BigDecimal decodeTime = BigDecimal.ZERO;
            int baselineCount = 0;
            int rleCount = 0;
            double avgSegmentLength = 0;
            int avgPackSizeUsed = 0;

            for(int j = 0; j < time_of_repeat; j++){
                int totalCost = 0;
                int totalSegments = 0;
                int totalPackSizeUsed = 0;

                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size() < 8) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledLong = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    SprintzEncodedResult ser = sprintz(scaledLong);
                    long[] scaledLongs = ser.getEncodedData();

                    // 使用智能选择方案
                    CompressionResult compResult = encodeWithSmartSelection(scaledLongs, 8);
                    long curCost = compResult.compressedSize;
                    totalPackSizeUsed += compResult.packSize;

                    // 统计方案选择
                    if (compResult.scheme == SCHEME_BASELINE) {
                        baselineCount++;
                    } else {
                        rleCount++;

                        // 获取分段信息
                        SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                        totalSegments += segResult.numSegments;
                    }

                    long duration = System.nanoTime() - startTime;
//                    modelTime += duration;
//                    modelCost += curCost;
                    encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                    modelCost = modelCost.add(BigDecimal.valueOf(curCost));
                    // 解码测试
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
                    long[] result = sprintzDecode(decodedData,ser.getFirstValue());
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
//                    modelDecodeTime += decodeDuration;
                }

                avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * 8) : 0;
                avgPackSizeUsed = !numbers.isEmpty() ? totalPackSizeUsed / (numbers.size() / CHUNK_SIZE + 1) : 0;
            }

            // 计算统计结果
            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

            // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
            // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
            BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
            BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                    .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

            // 计算解码吞吐量
            BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                    .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

            // 确定选择的方案
            String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

            // 输出结果
            String[] record = {
                    file.toString(),
                    "RLE_Sprintz",
                    String.valueOf(encodeThroughput.doubleValue()),
                    String.valueOf(decodeThroughput.doubleValue()),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio.doubleValue()),
                    selectedScheme,
                    String.valueOf(avgSegmentLength),
                    String.valueOf(avgPackSizeUsed)
            };
            writer.writeRecord(record);
            writer.close();
        }
    }


    @Test
    public void SprintzVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_Sprintz_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + "...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 表头
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
                    "Average Segment Length"
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

            for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                System.out.println("Testing pack size: " + pack_size);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
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

                        long[] scaledLong = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        SprintzEncodedResult ser = sprintz(scaledLong);
                        long[] scaledLongs = ser.getEncodedData();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(scaledLongs, pack_size);
                        int cur_cost = compResult.compressedSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(BigDecimal.valueOf(cur_cost));

                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
                        long[] result = sprintzDecode(decodedData,ser.getFirstValue());
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                    avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * pack_size) : 0;
                }

                // 计算统计结果
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s


                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                String[] record = {
                        file.toString(),
                        "RLE_Only",
                        String.valueOf(encodeThroughput.doubleValue()),
                        String.valueOf(decodeThroughput.doubleValue()),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength)
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    @Test
    public void SprintzVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (RLE)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_Sprintz_vary_m";
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
                    "Average Segment Length",
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

            int time_of_repeat = 10; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

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
            long[] scaledLongs_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledLongs_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                // 固定pack size为8
                int pack_size = 8;

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                int baselineCount = 0;
                int rleCount = 0;
                double avgSegmentLength = 0;
                int avgPackSizeUsed = 0;

                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    int totalSegments = 0;
                    int totalPackSizeUsed = 0;

                    for (int i = 0; i < scaledLongs_all.length; i += chunkSize) {
                        int end = Math.min(i + chunkSize, scaledLongs_all.length);
                        long[] scaleInt = new long[end - i];
                        System.arraycopy(scaledLongs_all, i, scaleInt, 0, end - i);

                        if (scaleInt.length < 8) continue;

                        long startTime = System.nanoTime();
                        SprintzEncodedResult ser = sprintz(scaleInt);
                        long[] chunkData = ser.getEncodedData();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(chunkData, pack_size);
                        int curCost = compResult.compressedSize;
                        totalPackSizeUsed += compResult.packSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(chunkData, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(BigDecimal.valueOf(curCost));


                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, chunkData.length, compResult.packSize);
                        long[] result = sprintzDecode(decodedData,ser.getFirstValue());
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                    avgSegmentLength = totalSegments > 0 ? (double) scaledLongs_all.length / (totalSegments * pack_size) : 0;
                    avgPackSizeUsed = scaledLongs_all.length > 0 ? totalPackSizeUsed / ((scaledLongs_all.length / chunkSize) + 1) : 0;
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s
                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize),
                        file.toString(),
                        "RLE_Sprintz",
                        String.valueOf(encodeThroughput.doubleValue()),
                        String.valueOf(decodeThroughput.doubleValue()),
                        String.valueOf(scaledLongs_all.length),
                        String.valueOf(modelCost),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength),
                        String.valueOf(avgPackSizeUsed)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }

    @Test
    public void TS2DIFFApproximate() throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_TS2DIFF";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 表头
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
                    "Selected Scheme",
                    "Average Segment Length",
                    "Pack Size Used"
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

            int time_of_repeat = 10;

            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal encodeTime = BigDecimal.ZERO;
            BigDecimal decodeTime = BigDecimal.ZERO;
            int baselineCount = 0;
            int rleCount = 0;
            double avgSegmentLength = 0;
            int avgPackSizeUsed = 0;

            for(int j = 0; j < time_of_repeat; j++){
                int totalCost = 0;
                int totalSegments = 0;
                int totalPackSizeUsed = 0;

                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size() < 8) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledLong = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    TSDIFFEncodedResult ter = ts2diff(scaledLong);
                    long[] scaledLongs = ter.getEncodedData();

                    // 使用智能选择方案
                    CompressionResult compResult = encodeWithSmartSelection(scaledLongs, 8);
                    long curCost = compResult.compressedSize;
                    totalPackSizeUsed += compResult.packSize;

                    // 统计方案选择
                    if (compResult.scheme == SCHEME_BASELINE) {
                        baselineCount++;
                    } else {
                        rleCount++;

                        // 获取分段信息
                        SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                        totalSegments += segResult.numSegments;
                    }

                    long duration = System.nanoTime() - startTime;
//                    modelTime += duration;
//                    modelCost += curCost;
                    encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                    modelCost = modelCost.add(BigDecimal.valueOf(curCost));
                    // 解码测试
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
                    long[] result = ts2diffDecode(decodedData,ter.getFirstValue(),ter.getMinDiff());
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
//                    modelDecodeTime += decodeDuration;
                }

                avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * 8) : 0;
                avgPackSizeUsed = !numbers.isEmpty() ? totalPackSizeUsed / (numbers.size() / CHUNK_SIZE + 1) : 0;
            }

            // 计算统计结果
            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

            // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
            // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
            BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
            BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                    .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

            // 计算解码吞吐量
            BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                    .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

            // 确定选择的方案
            String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

            // 输出结果
            String[] record = {
                    file.toString(),
                    "RLE_Sprintz",
                    String.valueOf(encodeThroughput.doubleValue()),
                    String.valueOf(decodeThroughput.doubleValue()),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio.doubleValue()),
                    selectedScheme,
                    String.valueOf(avgSegmentLength),
                    String.valueOf(avgPackSizeUsed)
            };
            writer.writeRecord(record);
            writer.close();
        }
    }

    @Test
    public void TS2DIFFVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Pack Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_TS2DIFF_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + "...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 表头
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
                    "Average Segment Length"
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

            for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                System.out.println("Testing pack size: " + pack_size);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                int baselineCount = 0;
                int rleCount = 0;
                double avgSegmentLength = 0;

                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    int totalSegments = 0;
                    int chunksize = CHUNK_SIZE+1;

                    for (int i = 0; i < numbers.size(); i += chunksize) {
                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + chunksize, numbers.size()));
                        if (chunkNumbers.size() < 8) continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + chunksize, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledLong = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        TSDIFFEncodedResult ter = ts2diff(scaledLong);
                        long[] scaledLongs = ter.getEncodedData();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(scaledLongs, pack_size);
                        int cur_cost = compResult.compressedSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(scaledLongs, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(BigDecimal.valueOf(cur_cost));

                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, scaledLongs.length, compResult.packSize);
                        long[] result = ts2diffDecode(decodedData,ter.getFirstValue(),ter.getMinDiff());
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                    avgSegmentLength = totalSegments > 0 ? (double)numbers.size() / (totalSegments * pack_size) : 0;
                }

                // 计算统计结果
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s


                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                String[] record = {
                        file.toString(),
                        "RLE_Only",
                        String.valueOf(encodeThroughput.doubleValue()),
                        String.valueOf(decodeThroughput.doubleValue()),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength)
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    @Test
    public void TS2DIFFVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (RLE)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_RLE_TS2DIFF_vary_m";
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
                    "Average Segment Length",
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

            int time_of_repeat = 10; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

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
            long[] scaledLongs_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledLongs_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                // 固定pack size为8
                int pack_size = 8;

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                int baselineCount = 0;
                int rleCount = 0;
                double avgSegmentLength = 0;
                int avgPackSizeUsed = 0;
                chunkSize += 1;
                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    int totalSegments = 0;
                    int totalPackSizeUsed = 0;


                    for (int i = 0; i < scaledLongs_all.length; i += chunkSize) {
                        int end = Math.min(i + chunkSize, scaledLongs_all.length);
                        long[] scaleInt = new long[end - i];
                        System.arraycopy(scaledLongs_all, i, scaleInt, 0, end - i);

                        if (scaleInt.length < 8) continue;

                        long startTime = System.nanoTime();
                        TSDIFFEncodedResult ter = ts2diff(scaleInt);
                        long[] chunkData = ter.getEncodedData();

                        // 使用智能选择方案
                        CompressionResult compResult = encodeWithSmartSelection(chunkData, pack_size);
                        int curCost = compResult.compressedSize;
                        totalPackSizeUsed += compResult.packSize;

                        // 统计方案选择
                        if (compResult.scheme == SCHEME_BASELINE) {
                            baselineCount++;
                        } else {
                            rleCount++;

                            // 获取分段信息
                            SegmentationResult segResult = getSegmentationInfo(chunkData, compResult.packSize);
                            totalSegments += segResult.numSegments;
                        }

                        long duration = System.nanoTime() - startTime;
                        encodeTime = encodeTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(BigDecimal.valueOf(curCost));


                        // 解码测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decodeWithSmartSelection(compResult.data, chunkData.length, compResult.packSize);
                        long[] result = ts2diffDecode(decodedData,ter.getFirstValue(),ter.getMinDiff());
                        long decodeDuration = System.nanoTime() - startDecodeTime;
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                    avgSegmentLength = totalSegments > 0 ? (double) scaledLongs_all.length / (totalSegments * pack_size) : 0;
                    avgPackSizeUsed = scaledLongs_all.length > 0 ? totalPackSizeUsed / ((scaledLongs_all.length / chunkSize) + 1) : 0;
                }

                // 计算平均值
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s
                // 确定选择的方案
                String selectedScheme = baselineCount > rleCount ? "BASELINE" : "RLE";

                // 写入结果
                String[] record = {
                        String.valueOf(chunkSize-1),
                        file.toString(),
                        "RLE_Sprintz",
                        String.valueOf(encodeThroughput.doubleValue()),
                        String.valueOf(decodeThroughput.doubleValue()),
                        String.valueOf(scaledLongs_all.length),
                        String.valueOf(modelCost),
                        String.valueOf(model_ratio),
                        selectedScheme,
                        String.valueOf(avgSegmentLength),
                        String.valueOf(avgPackSizeUsed)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }

}