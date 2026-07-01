package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SprintzDP {

    private static final int CHUNK_SIZE = 1024;

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
            s = s.replace(",", ""); // remove thousands sep

            // If scientific notation present, BigDecimal can parse it
            try {
                BigDecimal bd = new BigDecimal(s);
                BigDecimal scaled = bd.multiply(scale);
                // rounding to nearest whole
                BigDecimal rounded = scaled.setScale(0, RoundingMode.HALF_UP);
                vals[i] = rounded;
            } catch (Exception ex) {
                // fallback: parse double
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

        // find min
        BigDecimal minv = vals[0];
        for (int i = 1; i < n; ++i) if (vals[i].compareTo(minv) < 0) minv = vals[i];

        for (int i = 0; i < n; ++i) {
            BigDecimal shifted = vals[i].subtract(minv);
            // clamp to long range
            try {
                BigInteger bi = shifted.toBigIntegerExact();
                if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) result[i] = Long.MAX_VALUE;
                else if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) result[i] = Long.MIN_VALUE;
                else result[i] = bi.longValue();
            } catch (ArithmeticException ae) {
                // not an integer exactly: fallback by converting to long with rounding
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

//    public static long[] sprintz(long[] numbers) {
//        int size = numbers.length;
//        long[] result = new long[size];
//
//        if (size == 0) return result; // 空数组直接返回
//
//        long first = numbers[0];
//        result[0] = first;
//
//        long prev = first;
//        for (int i = 1; i < size; i++) {
//            long current = numbers[i];
//            long diff = current - prev;
//            // ZigZag 编码: 正数 -> 偶数, 负数 -> 奇数
//            result[i] = (diff << 1) ^ (diff >> 63);
//            prev = current;
//        }
//
//        return result;
//    }

    // 动态规划结果类
    static class PackingPlan {
        int optimalC;
        List<Integer> groupSizes;    // 每个分组包含的块数
        List<Integer> groupBitWidths; // 每个分组的位宽
        int totalCost;

        PackingPlan(int optimalC, List<Integer> groupSizes, List<Integer> groupBitWidths, int totalCost) {
            this.optimalC = optimalC;
            this.groupSizes = groupSizes;
            this.groupBitWidths = groupBitWidths;
            this.totalCost = totalCost;
        }
    }

    // 计算最优分组方案的完整实现
    public static PackingPlan computeOptimalPackingPlan(int[] bitWidths, int pack_size) {
        int N = bitWidths.length;
        if (N == 0) {
            return new PackingPlan(0, new ArrayList<>(), new ArrayList<>(), 0);
        }

        // 预计算所有区间的最大位宽
        int[][] maxB = new int[N][N];
        for (int l = 0; l < N; l++) {
            maxB[l][l] = bitWidths[l];
            for (int r = l + 1; r < N; r++) {
                maxB[l][r] = Math.max(maxB[l][r - 1], bitWidths[r]);
            }
        }

        int minTotalCost = Integer.MAX_VALUE;
        int bestC = 1;
        List<Integer> bestGroupSizes = new ArrayList<>();
        List<Integer> bestGroupBitWidths = new ArrayList<>();

        int maxPossibleC = 64 - Long.numberOfLeadingZeros(N);

        for (int C = 1; C <= maxPossibleC; C++) {
            int low_C = (C == 1) ? 1 : (1 << (C - 1));
            int high_C = Math.min((1 << C) - 1, N);

            // DP表和相关记录数组
            int[][] dp = new int[N + 1][2];
            int[][] prevState = new int[N + 1][2];
            int[][] prevFlag = new int[N + 1][2];
            int[][] packSize = new int[N + 1][2];

            // 初始化
            for (int i = 0; i <= N; i++) {
                dp[i][0] = Integer.MAX_VALUE / 2;
                dp[i][1] = Integer.MAX_VALUE / 2;
            }
            dp[0][0] = 0;

            // 动态规划计算
            for (int i = 1; i <= N; i++) {
                for (int k = Math.max(1, i - high_C + 1); k <= i; k++) {
                    int packLength = i - k + 1;
                    int currentMaxB = maxB[k - 1][i - 1];
                    int packCost = pack_size * packLength * currentMaxB + 6 + C;

                    if (packLength < low_C) {
                        // 小分组，不能改变状态
                        if (dp[k - 1][0] + packCost < dp[i][0]) {
                            dp[i][0] = dp[k - 1][0] + packCost;
                            prevState[i][0] = k - 1;
                            prevFlag[i][0] = 0;
                            packSize[i][0] = packLength;
                        }
                        if (dp[k - 1][1] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][1] + packCost;
                            prevState[i][1] = k - 1;
                            prevFlag[i][1] = 1;
                            packSize[i][1] = packLength;
                        }
                    } else {
                        // 大分组，可以改变状态到1
                        if (dp[k - 1][0] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][0] + packCost;
                            prevState[i][1] = k - 1;
                            prevFlag[i][1] = 0;
                            packSize[i][1] = packLength;
                        }
                        if (dp[k - 1][1] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][1] + packCost;
                            prevState[i][1] = k - 1;
                            prevFlag[i][1] = 1;
                            packSize[i][1] = packLength;
                        }
                    }
                }
            }

            // 回溯构建分组方案
            if (dp[N][1] < minTotalCost) {
                minTotalCost = dp[N][1];
                bestC = C;
                bestGroupSizes = new ArrayList<>();
                bestGroupBitWidths = new ArrayList<>();

                // 回溯路径
                int i = N;
                int flag = 1;
                while (i > 0) {
                    int size = packSize[i][flag];
                    int start = i - size;
                    int bitWidth = maxB[start][i - 1];

                    bestGroupSizes.add(0, size);
                    bestGroupBitWidths.add(0, bitWidth);

                    i = prevState[i][flag];
                    flag = prevFlag[i + size][flag]; // 注意这里要调整索引
                }
            }
        }

        return new PackingPlan(bestC, bestGroupSizes, bestGroupBitWidths, minTotalCost);
    }

    // 计算压缩后数据总大小
    private static int calculateTotalCompressedSize(PackingPlan plan, int pack_size) {
        int totalSize = 0;

        // 元数据头大小: C(6bits) + 分组数量(8bits)
        totalSize += 6; // C占用6bits
        totalSize += 8; // 分组数量占用8bits

        // 每个分组的元数据: packsize(plan.optimalC bits) + 位宽(6bits)
        int groupMetadataBits = plan.groupSizes.size() * (plan.optimalC + 6);
        totalSize += groupMetadataBits;

        // 压缩数据大小
        for (int i = 0; i < plan.groupSizes.size(); i++) {
            int groupBlocks = plan.groupSizes.get(i);
            int bitWidth = plan.groupBitWidths.get(i);
            int dataBits = groupBlocks * pack_size * bitWidth;
            totalSize += dataBits;
        }

        // 转换为字节数（向上取整）
        return (totalSize + 7) / 8;
    }

    // 辅助：位写入器（逐位写入）
    private static class BitWriter {
        final byte[] data;
        int bytePos = 0;
        int bitPos = 0; // 0..7, next bit to write in current byte (7 - bitPos is mask shift)

        BitWriter(byte[] data, int startBytePos) {
            this.data = data;
            this.bytePos = startBytePos;
        }

        // 写 numBits 位（numBits <= 64），value 的低 numBits 位被写出（big-endian bit order）
        void writeBits(long value, int numBits) {
            for (int i = numBits - 1; i >= 0; --i) {
                int bit = (int)((value >> i) & 1L);
                data[bytePos] |= (byte)(bit << (7 - bitPos));
                bitPos++;
                if (bitPos == 8) {
                    bitPos = 0;
                    bytePos++;
                }
            }
        }

        int getBytePos() { return bytePos; }
        int getBitPos() { return bitPos; }
        // 返回写入结束后占用的字节数（包括部分字节）
        int bytesWritten() {
            return bytePos + (bitPos > 0 ? 1 : 0);
        }

        // 批量写入多个值（逐位打包）
        void writeValues(long[] values, int start, int count, int bitWidth) {
            for (int i = 0; i < count; i++) {
                long value = values[start + i];
                writeBits(value, bitWidth);
            }
        }
    }

    static class BitReader {
        final byte[] data;
        int bytePos = 0;
        int bitPos = 0;

        BitReader(byte[] data, int startBytePos, int startBitPos) {
            this.data = data;
            this.bytePos = startBytePos;
            this.bitPos = startBitPos;
        }

        // 读 numBits 位并作为 long 返回（numBits <= 64）
        long readBits(int numBits) {
            long res = 0L;
            for (int i = 0; i < numBits; ++i) {
                int bit = (data[bytePos] >> (7 - bitPos)) & 1;
                res = (res << 1) | bit;
                bitPos++;
                if (bitPos == 8) {
                    bitPos = 0;
                    bytePos++;
                }
            }
            return res;
        }

        int getBytePos() { return bytePos; }
        int getBitPos() { return bitPos; }

        // 批量读取多个值（逐位解包）
        void readValues(long[] result, int start, int count, int bitWidth) {
            for (int i = 0; i < count; i++) {
                long value = readBits(bitWidth);
                result[start + i] = value;
            }
        }
    }

    // 使用 BitWriter/BitReader 来实现压缩/解压（逐位打包）
    public static byte[] compressWithOptimalPacking(long[] paddedArray, int[] bitWidths, int pack_size, int[] encodePos) {
        PackingPlan optimalPlan = computeOptimalPackingPlan(bitWidths, pack_size);
        int totalSize = calculateTotalCompressedSize(optimalPlan, pack_size); // 以字节计
        byte[] compressedData = new byte[totalSize + 32]; // 预留一些额外空间
        Arrays.fill(compressedData, (byte)0); // 初始化为0
        BitWriter writer = new BitWriter(compressedData, 0);

        // 写 C (6 bits) 和 groupCount(8 bits)
        writer.writeBits(optimalPlan.optimalC, 6);
        writer.writeBits(optimalPlan.groupSizes.size(), 8);

        int dataIndex = 0;
        for (int gi = 0; gi < optimalPlan.groupSizes.size(); ++gi) {
            int groupBlocks = optimalPlan.groupSizes.get(gi);
            int bitWidth = optimalPlan.groupBitWidths.get(gi);
            writer.writeBits(groupBlocks, optimalPlan.optimalC);
            writer.writeBits(bitWidth, 6);

            int groupDataCount = groupBlocks * pack_size;
            // 批量写入该组的所有值（逐位）
            writer.writeValues(paddedArray, dataIndex, groupDataCount, bitWidth);
            dataIndex += groupDataCount;
        }

        int finalBytes = writer.bytesWritten();
        if (finalBytes > totalSize) {
            // 警告：计算函数可能低估了实际大小。这里扩容并返回实际大小。
            System.err.println(String.format("Warning: calculateTotalCompressedSize underestimated bytes: estimated=%d actual=%d. Returning actual length.",
                    totalSize, finalBytes));
            byte[] out = new byte[finalBytes];
            System.arraycopy(compressedData, 0, out, 0, finalBytes);
            encodePos[0] = finalBytes;
            return out;
        } else {
            // 返回精确长度拷贝
            byte[] out = new byte[finalBytes];
            System.arraycopy(compressedData, 0, out, 0, finalBytes);
            encodePos[0] = finalBytes;
            return out;
        }
    }

    // 解压缩方法（逐位解包）
    public static long[] decompressWithOptimalPacking(byte[] compressedData, int originalLength, int pack_size) {
        BitReader reader = new BitReader(compressedData, 0, 0);
        int C = (int) reader.readBits(6);
        int groupCount = (int) reader.readBits(8);

        // sanity checks
        if (C <= 0 || C > 64) throw new IllegalArgumentException("Invalid C read from header: " + C);
        if (groupCount < 0 || groupCount > (1 << 20)) throw new IllegalArgumentException("Suspicious groupCount: " + groupCount);

        long[] result = new long[originalLength];
        int resultIndex = 0;

        for (int gi = 0; gi < groupCount; ++gi) {
            // 先确保还能读 group header
            long remainingBitsBeforeGroup = ((long)(compressedData.length - reader.getBytePos())) * 8L - reader.getBitPos();
            if (remainingBitsBeforeGroup < C + 6) {
                throw new IllegalArgumentException(String.format(
                        "Not enough bits for group header #%d: need %d but only %d remain (bytePos=%d bitPos=%d totalBytes=%d).",
                        gi, C + 6, remainingBitsBeforeGroup, reader.getBytePos(), reader.getBitPos(), compressedData.length));
            }

            int groupBlocks = (int) reader.readBits(C);
            int bitWidth = (int) reader.readBits(6);
            if (bitWidth < 0 || bitWidth > 64) throw new IllegalArgumentException("Invalid bitWidth: " + bitWidth);

            long groupDataCount = (long) groupBlocks * (long) pack_size;
            if (groupDataCount < 0 || groupDataCount > (1L << 31)) {
                throw new IllegalArgumentException("Suspicious groupDataCount: " + groupDataCount);
            }

            long neededDataBits = groupDataCount * (long) bitWidth;
            long remainingBitsAfterHeader = ((long)(compressedData.length - reader.getBytePos())) * 8L - reader.getBitPos();
            if (neededDataBits > remainingBitsAfterHeader) {
                throw new IllegalArgumentException(String.format(
                        "Not enough bits for group #%d data: need %d bits but only %d available (bytePos=%d bitPos=%d).",
                        gi, neededDataBits, remainingBitsAfterHeader, reader.getBytePos(), reader.getBitPos()));
            }

            // 批量读取该组的所有值（逐位）
            int countToRead = (int) Math.min(groupDataCount, result.length - resultIndex);
            reader.readValues(result, resultIndex, countToRead, bitWidth);
            resultIndex += countToRead;

            // 如果还有剩余的数据（groupDataCount > countToRead），跳过它们
            int remainingValues = (int) (groupDataCount - countToRead);
            if (remainingValues > 0) {
                int bitsToSkip = remainingValues * bitWidth;
                int bitsSkipped = 0;
                while (bitsSkipped < bitsToSkip) {
                    reader.readBits(1); // 逐位跳过
                    bitsSkipped++;
                }
            }
        }

        // 确保结果数组被正确填充
        if (resultIndex < result.length) {
            long[] trimmedResult = new long[resultIndex];
            System.arraycopy(result, 0, trimmedResult, 0, resultIndex);
            return trimmedResult;
        }

        return result;
    }

    // 8. 修复：sprintz编码解码
    public static long[] sprintz(long[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new long[0];
        }

        long[] result = new long[numbers.length];
        result[0] = numbers[0];

        for (int i = 1; i < numbers.length; i++) {
            long diff = numbers[i] - numbers[i-1];
            // ZigZag编码
            result[i] = (diff << 1) ^ (diff >> 31);
        }

        return result;
    }

    public static long[] sprintzDecode(long[] encodedData) {
        if (encodedData == null || encodedData.length == 0) {
            return new long[0];
        }

        long[] result = new long[encodedData.length];
        result[0] = encodedData[0];

        for (int i = 1; i < encodedData.length; i++) {
            // ZigZag解码
            long zigzag = encodedData[i];
            long diff = (zigzag >>> 1) ^ -(zigzag & 1);
            result[i] = result[i-1] + diff;
        }

        return result;
    }

    public static void main(String[] args) throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_DP";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
//            if (!file.getName().equals("EPM-Education.csv") && !file.getName().equals("TH-Climate.csv")) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
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
            int time_of_repeat = 10;
            for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                System.out.println(pack_size);
                int modelCost = 0;
                long modelTime = 0;
                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        long[] scaledInts = sprintz(scaledInt);
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + pack_size; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / pack_size] = bitWidth;
                        }
                        int[] encodePos = new int[1];
                        byte[] res = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        int cur_cost = encodePos[0]*8;
                        long duration = System.nanoTime() - startTime;
                        modelTime += (duration);
                        modelCost += cur_cost;
                    }
                }
                modelCost /= time_of_repeat;
                modelTime = (modelTime) / time_of_repeat;
                double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);
                String[] record = {
                        file.toString(),
                        "SPRINTZ-DP",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }

    @Test
    public void TestVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_DP_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
//            if (!file.getName().equals("EPM-Education.csv") && !file.getName().equals("TH-Climate.csv")) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
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
            int time_of_repeat = 10;
            for(int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);
                System.out.println(pack_size);
                int modelCost = 0;
                long modelTime = 0;
                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        long[] scaledInts = sprintz(scaledInt);
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                            // 1. 找出当前pack_size个元素中的最大值
                            long maxInGroup = 0;
                            for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + pack_size; scaledInts_j++) {
                                if (paddedArray[scaledInts_j] > maxInGroup) {
                                    maxInGroup = paddedArray[scaledInts_j];
                                }
                            }

                            // 2. 计算该最大值的去头零位宽
                            int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                            // 3. 存储结果
                            bitWidths[scaledInts_i / pack_size] = bitWidth;
                        }
                        int[] encodePos = new int[1];
                        byte[] res = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        int cur_cost = encodePos[0]*8;
                        long duration = System.nanoTime() - startTime;
                        modelTime += (duration);
                        modelCost += cur_cost;
                    }
                }
                modelCost /= time_of_repeat;
                modelTime = (modelTime) / time_of_repeat;
                double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);
                String[] record = {
                        file.toString(),
                        "SPRINTZ-DP",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio)
                };
                writer.writeRecord(record);
            }
            writer.close();
        }
    }

    // 新增方法：测试不同chunk size的表现
    @Test
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_DP_vary_m";
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
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
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
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

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
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    int modelCost = 0;
                    long modelTime = 0;

                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {
                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInt = new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);

                            long startTime = System.nanoTime();
                            long[] scaledInts = sprintz(scaledInt);
                            int remainder = scaledInts.length % pack_size;
                            int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                            // 创建新数组，长度补齐为pack_size的倍数
                            long[] paddedArray = new long[scaledInts.length + paddingLength];
                            System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                            int actual_length = paddedArray.length;
                            int[] bitWidths = new int[actual_length / pack_size];

                            for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                long maxInGroup = 0;
                                for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + pack_size; scaledInts_j++) {
                                    if (paddedArray[scaledInts_j] > maxInGroup) {
                                        maxInGroup = paddedArray[scaledInts_j];
                                    }
                                }

                                int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);
                                bitWidths[scaledInts_i / pack_size] = bitWidth;
                            }

                            int[] encodePos = new int[1];
                            byte[] res = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                            int cur_cost = encodePos[0] * 8;
                            long duration = System.nanoTime() - startTime;
                            modelTime += (duration);
                            modelCost += cur_cost;
                        }
                    }

                    modelCost /= time_of_repeat;
                    modelTime = (modelTime) / time_of_repeat;
                    double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                    double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "Sprintz-DP",
                            String.valueOf(modelTime_throughput),
                            String.valueOf(numbers.size()),
                            String.valueOf(modelCost),
                            String.valueOf(pack_size),
                            String.valueOf(model_ratio)
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }
}