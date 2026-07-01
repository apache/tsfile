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


public class BP {

    private static final int CHUNK_SIZE = 1024;
    static int all_time_of_repeat = 200;


    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    // 修改：使用 int[] 替代 ArrayList<Integer>
    public static int bitPacking(long[] numbers, int start, int count, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int currentBytePos = encode_pos;
        int currentBitPos = 0;

        for (int i = 0; i < count; i++) {
            long value = numbers[start + i];

            for (int bit = bit_width - 1; bit >= 0; bit--) {
                long currentBit = (value >> bit) & 1;

                // 确保不越界
                if (currentBytePos >= encoded_result.length) {
                    throw new ArrayIndexOutOfBoundsException(
                            "currentBytePos " + currentBytePos + " >= length " + encoded_result.length
                    );
                }

                encoded_result[currentBytePos] |= (currentBit << (7 - currentBitPos));
                currentBitPos++;

                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }
        }

        return currentBytePos + (currentBitPos > 0 ? 1 : 0);
    }

    // 修复后的 decodeBitPacking 方法
    public static long[] decodeBitPacking(byte[] compressedData, int[] bitWidths, int pack_size, int originalLength) {
        long[] result = new long[originalLength];
        int resultIndex = 0;
        int decodePos = 0;
        int currentBitPos = 0;

        for (int group = 0; group < bitWidths.length && resultIndex < originalLength; group++) {
            // 读取位宽（1字节）
            if (decodePos >= compressedData.length) {
                break; // 没有更多数据可读
            }
            int bitWidth = compressedData[decodePos] & 0xFF;
            decodePos++;

            int startOfGroupData = decodePos;
            currentBitPos = 0;

            // 读取 pack_size 个值，逐bit读取
            int valuesToRead = Math.min(pack_size, originalLength - resultIndex);
            for (int i = 0; i < valuesToRead; i++) {
                long value = 0;

                // 按位读取
                for (int bit = 0; bit < bitWidth; bit++) {
                    // 检查边界
                    if (decodePos >= compressedData.length) {
                        throw new ArrayIndexOutOfBoundsException(
                                "decodePos " + decodePos + " >= length " + compressedData.length +
                                        " (group=" + group + ", value=" + i + ", bit=" + bit + ")"
                        );
                    }

                    // 确保从字节中读取正确的位
                    int currentBit = 0;
                    if (decodePos < compressedData.length) {
                        currentBit = (compressedData[decodePos] >> (7 - currentBitPos)) & 1;
                    }
                    value = (value << 1) | currentBit;
                    currentBitPos++;

                    if (currentBitPos == 8) {
                        decodePos++;
                        currentBitPos = 0;
                    }
                }

                if (resultIndex < originalLength) {
                    result[resultIndex++] = value;
                }
            }

            // 跳过本组剩余位，对齐到下一组
            if (currentBitPos > 0) {
                decodePos++; // 跳过部分使用的字节
                currentBitPos = 0;
            }

            // 计算本组数据占用的字节数并跳转到下一组
            int groupDataBits = pack_size * bitWidth;
            int groupDataBytes = (groupDataBits + 7) / 8;
            decodePos = startOfGroupData + groupDataBytes;

            // 确保 decodePos 不会超过数组长度
            if (decodePos > compressedData.length) {
                decodePos = compressedData.length;
            }
        }

        return result;
    }

    // 新增的完整解压函数（包含头部信息解析）
//    public static int[] decodeBitPackingWithHeader(byte[] encodedWithHeader, int pack_size) {
//        // 解析头部信息 - 假设前4个字节存储原始数据长度
//        int originalLength = bytes2Integer(encodedWithHeader, 0, 4);
//
//        // 解析分组数
//        int groupCount = bytes2Integer(encodedWithHeader, 4, 4);
//
//        // 解析位宽数组
//        int[] bitWidths = new int[groupCount];
//        int headerSize = 8; // 4字节原始长度 + 4字节分组数
//        for (int i = 0; i < groupCount; i++) {
//            bitWidths[i] = encodedWithHeader[headerSize + i] & 0xFF;
//        }
//
//        // 压缩数据起始位置
//        int dataStart = headerSize + groupCount;
//        byte[] compressedData = new byte[encodedWithHeader.length - dataStart];
//        System.arraycopy(encodedWithHeader, dataStart, compressedData, 0, compressedData.length);
//
//        // 调用解压函数
//        return decodeBitPacking(compressedData, bitWidths, pack_size, originalLength);
//    }

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

    @Test
    public void BP0() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头，增加解压吞吐率列
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = 100;


            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal modelTime = BigDecimal.ZERO;
            BigDecimal modelDecodeTime = BigDecimal.ZERO; // 新增：解压时间

            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                    // 创建新数组，长度补齐为8的倍数
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        // 1. 找出当前8个元素中的最大值
                        long maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }

                        // 2. 计算该最大值的去头零位宽
                        int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                        // 3. 存储结果
                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }

//                        int fixed_block = CHUNK_SIZE/40;
                    byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, 8);
                    BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8L); // 转换为bit数
                    long duration = System.nanoTime() - startTime;
                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                    modelCost = modelCost.add(cur_cost);

                    // 新增：测试解压性能
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeBitPacking(compressedData, bitWidths, 8, scaledInts.length);
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                }

            }
            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP); // 平均解压时间

            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelDecodeTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);

            // 更新输出记录，包含解压吞吐率
            String[] record = {
                    file.toString(),
                    "BP",
                    modelTime_throughput.toPlainString(),
                    modelDecodeTime_throughput.toPlainString(),
                    String.valueOf(numbers.size()),
                    modelCost.toPlainString(),
                    model_ratio.toPlainString()
            };
            writer.writeRecord(record);
            writer.close();

            System.out.println("Encoding throughput: " + modelTime_throughput + " MB/s");
            System.out.println("Decoding throughput: " + modelDecodeTime_throughput + " MB/s");
            System.out.println("Compression ratio: " + model_ratio);
        }

    }

    // 修复后的encodeBitPacking方法，大幅增加缓冲区大小 - 使用 int[] 替代 ArrayList<Integer>
    public static byte[] encodeBitPacking(long[] paddedArray, int[] bitWidths, int pack_size) {
        int totalGroups = bitWidths.length;

        // 计算总位数：每个分组需要 pack_size * bitWidths[group] 位，加上每个分组的位宽元数据（1字节）
        int totalBits = 0;
        for (int i = 0; i < totalGroups; i++) {
            totalBits += 8; // 位宽元数据（1字节 = 8位）
            totalBits += pack_size * bitWidths[i]; // 数据位
        }

        // 转换为字节数（向上取整），大幅增加额外空间
        int totalBytes = (totalBits + 7) / 8 * 2; // 加倍
        if (totalBytes < 1024) totalBytes = 1024; // 至少1KB

        byte[] bitPackedData = new byte[totalBytes];

        // 初始化数组为0，确保未使用的位为0
        for (int i = 0; i < bitPackedData.length; i++) {
            bitPackedData[i] = 0;
        }

        int encodePos = 0;

        for (int group = 0; group < totalGroups; group++) {
            int startIndex = group * pack_size;

            // 写入位宽（1字节）
            bitPackedData[encodePos++] = (byte) bitWidths[group];

            // 使用逐bit拼接方式写入数据 - 使用 int[] 替代 ArrayList<Integer>
            encodePos = bitPacking(paddedArray, startIndex, pack_size, bitWidths[group], encodePos, bitPackedData);
        }

        // 返回实际使用的字节数
        byte[] finalResult = new byte[encodePos];
        System.arraycopy(bitPackedData, 0, finalResult, 0, encodePos);

        return finalResult;
    }

    public static int computeMinPackingCost(int[] bitWidths, int fixed_pack, int pack_size) {
        int blocksize= bitWidths.length;
        int totalCost = 0;
        int numPacks = (int) Math.ceil((double) blocksize / fixed_pack);

        for (int pack = 0; pack < numPacks; pack++) {
            int start = pack * fixed_pack;
            int end = Math.min(start + fixed_pack, blocksize);

            int maxBitWidth = 0;
            for (int i = start; i < end; i++) {
                if (bitWidths[i] > maxBitWidth) {
                    maxBitWidth = bitWidths[i];
                }
            }

            totalCost += pack_size * (end-start) * maxBitWidth;
        }

        totalCost += 5 * blocksize / fixed_pack;
        return totalCost;
    }

    /** 验证压缩与解压往返一致性：任意 pack_size 下 encode 再 decode 应得到原数据 */
    @Test
    public void testEncodeDecodeRoundTrip() {
        int[] packSizes = {1, 2, 3, 5, 8, 10, 16};
        for (int packSize : packSizes) {
            int len = 64;
            int[] original = new int[len];
            for (int i = 0; i < len; i++) {
                original[i] = i * 7 + 3;
            }

            int paddedLen = ((len + packSize - 1) / packSize) * packSize;
            long[] padded = new long[paddedLen];
            System.arraycopy(original, 0, padded, 0, len);

            int groupCount = paddedLen / packSize;
            int[] bitWidths = new int[groupCount];
            for (int g = 0; g < groupCount; g++) {
                long maxVal = 0;
                for (int j = g * packSize; j < (g + 1) * packSize; j++) {
                    if (padded[j] > maxVal) maxVal = padded[j];
                }
                bitWidths[g] = maxVal == 0 ? 1 : (64 - Long.numberOfLeadingZeros(maxVal));
            }

            try {
                byte[] compressed = encodeBitPacking(padded, bitWidths, packSize);
                long[] decoded = decodeBitPacking(compressed, bitWidths, packSize, len);

                // 验证
                for (int k = 0; k < len; k++) {
                    if (original[k] != decoded[k]) {
                        throw new AssertionError("pack_size=" + packSize + " index=" + k
                                + " expected=" + original[k] + " got=" + decoded[k]);
                    }
                }
                System.out.println("Successfully tested pack_size=" + packSize);
            } catch (Exception e) {
                System.err.println("Error with pack_size=" + packSize + ": " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }
    }

    @Test
    public void TestVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = all_time_of_repeat;

            for(int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
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

                        byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                        BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8); // 转换为bit数
                        long duration = System.nanoTime() - startTime;

                        long decodeStart = System.nanoTime();
                        decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                        long decodeDuration = System.nanoTime() - decodeStart;

                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    decodeThroughput =
                            numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                }
                String[] record = {
                        file.toString(),
                        "BP",
                        modelTime_throughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
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
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP_vary_m";
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
                    "Compression Throughput",
                    "Decompression Throughput",
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

            int time_of_repeat = 100; // 减少重复次数以加快测试速度
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
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 3; pack_size_exp < 4; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;

                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {

                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInts = new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInts, 0, end - i);


                            long startTime = System.nanoTime();
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

                            byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                            BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8);
                            long duration = System.nanoTime() - startTime;

                            long decodeStart = System.nanoTime();
                            decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                            long decodeDuration = System.nanoTime() - decodeStart;

                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                            modelCost = modelCost.add(cur_cost);
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "BP",
                            modelTime_throughput.toPlainString(),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            modelCost.toPlainString(),
                            String.valueOf(pack_size),
                            model_ratio.toPlainString()
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }

    @Test
    public void Zigzag() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP_Zigzag";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头，增加解压吞吐率列
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = 100;


            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal modelTime = BigDecimal.ZERO;
            BigDecimal modelDecodeTime = BigDecimal.ZERO; // 新增：解压时间

            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();

                    long[] scaledInts = zigzag(scaledInt);

                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                    // 创建新数组，长度补齐为8的倍数
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        // 1. 找出当前8个元素中的最大值
                        long maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }

                        // 2. 计算该最大值的去头零位宽
                        int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                        // 3. 存储结果
                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }

//                        int fixed_block = CHUNK_SIZE/40;
                    byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, 8);
                    BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8L); // 转换为bit数
                    long duration = System.nanoTime() - startTime;
                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                    modelCost = modelCost.add(cur_cost);

                    // 新增：测试解压性能
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeBitPacking(compressedData, bitWidths, 8, scaledInts.length);
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                }

            }
            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP); // 平均解压时间

            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelDecodeTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);

            // 更新输出记录，包含解压吞吐率
            String[] record = {
                    file.toString(),
                    "BP",
                    modelTime_throughput.toPlainString(),
                    modelDecodeTime_throughput.toPlainString(),
                    String.valueOf(numbers.size()),
                    modelCost.toPlainString(),
                    model_ratio.toPlainString()
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
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_Zigzag_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = 100;

            for(int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
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

                        long[] scaledInts = zigzag(scaledInt);
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

                        byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                        BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8); // 转换为bit数
                        long duration = System.nanoTime() - startTime;
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(cur_cost);

                        long decodeStart = System.nanoTime();
                        long[] decodedData = decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                        zigzagDecode(decodedData);
                        long decodeDuration = System.nanoTime() - decodeStart;
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    decodeThroughput =
                            numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                }
                String[] record = {
                        file.toString(),
                        "BP",
                        modelTime_throughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    // 新增方法：测试不同chunk size的表现
    @Test
    public void ZigzagVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_Zigzag_vary_m";
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
                    "Compression Throughput",
                    "Decompression Throughput",
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

            int time_of_repeat = 100; // 减少重复次数以加快测试速度
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
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 3; pack_size_exp < 4; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;


                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {

                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInt = new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);


                            long startTime = System.nanoTime();
                            long[] scaledInts = zigzag(scaledInt);
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

                            byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                            BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8);
                            long duration = System.nanoTime() - startTime;

                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);

                            long decodeStart = System.nanoTime();
                            long[] decodedData = decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                            zigzagDecode(decodedData);
                            long decodeDuration = System.nanoTime() - decodeStart;
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "BP",
                            modelTime_throughput.toPlainString(),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            modelCost.toPlainString(),
                            String.valueOf(pack_size),
                            model_ratio.toPlainString()
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }

    @Test
    public void Sprintz() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP_Sprintz";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头，增加解压吞吐率列
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = 100;


            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal modelTime = BigDecimal.ZERO;
            BigDecimal modelDecodeTime = BigDecimal.ZERO; // 新增：解压时间

            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();

                    SprintzEncodedResult ser = sprintz(scaledInt);
                    long[] scaledInts = ser.getEncodedData();

                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                    // 创建新数组，长度补齐为8的倍数
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        // 1. 找出当前8个元素中的最大值
                        long maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }

                        // 2. 计算该最大值的去头零位宽
                        int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                        // 3. 存储结果
                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }

//                        int fixed_block = CHUNK_SIZE/40;
                    byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, 8);
                    BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8L); // 转换为bit数
                    long duration = System.nanoTime() - startTime;
                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                    modelCost = modelCost.add(cur_cost);

                    // 新增：测试解压性能
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeBitPacking(compressedData, bitWidths, 8, scaledInts.length);
                    long[] originalData = sprintzDecode(decodedData,ser.getFirstValue());
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                }

            }
            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP); // 平均解压时间

            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelDecodeTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);

            // 更新输出记录，包含解压吞吐率
            String[] record = {
                    file.toString(),
                    "BP",
                    modelTime_throughput.toPlainString(),
                    modelDecodeTime_throughput.toPlainString(),
                    String.valueOf(numbers.size()),
                    modelCost.toPlainString(),
                    model_ratio.toPlainString()
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
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_Sprintz_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = 1;

            for(int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
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

                        SprintzEncodedResult ser = sprintz(scaledInt);
                        long[] scaledInts = ser.getEncodedData();
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
                        byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                        BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8); // 转换为bit数
                        long duration = System.nanoTime() - startTime;
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(cur_cost);

                        long decodeStart = System.nanoTime();
                        long[] decodedData = decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                        sprintzDecode(decodedData, ser.getFirstValue());
                        long decodeDuration = System.nanoTime() - decodeStart;
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    decodeThroughput =
                            numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                }
                String[] record = {
                        file.toString(),
                        "BP",
                        modelTime_throughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    // 新增方法：测试不同chunk size的表现
    @Test
    public void SprintzVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_Sprintz_vary_m";
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
                    "Compression Throughput",
                    "Decompression Throughput",
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

            int time_of_repeat = 100; // 减少重复次数以加快测试速度
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
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 3; pack_size_exp < 4; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;


                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {

                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInt = new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);


                            long startTime = System.nanoTime();

                            SprintzEncodedResult ser = sprintz(scaledInt);
                            long[] scaledInts = ser.getEncodedData();
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

                            byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                            BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8);
                            long duration = System.nanoTime() - startTime;

                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);

                            long decodeStart = System.nanoTime();
                            long[] decodedData = decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                            sprintzDecode(decodedData, ser.getFirstValue());
                            long decodeDuration = System.nanoTime() - decodeStart;
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "BP",
                            modelTime_throughput.toPlainString(),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            modelCost.toPlainString(),
                            String.valueOf(pack_size),
                            model_ratio.toPlainString()
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }

    @Test
    public void TS2DIFF() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BP_TS2DIFF";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头，增加解压吞吐率列
            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = 100;


            BigDecimal modelCost = BigDecimal.ZERO;
            BigDecimal modelTime = BigDecimal.ZERO;
            BigDecimal modelDecodeTime = BigDecimal.ZERO; // 新增：解压时间

            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();

                    TSDIFFEncodedResult ter = ts2diff(scaledInt);
                    long[] scaledInts = ter.getEncodedData();

                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                    // 创建新数组，长度补齐为8的倍数
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        // 1. 找出当前8个元素中的最大值
                        long maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }

                        // 2. 计算该最大值的去头零位宽
                        int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                        // 3. 存储结果
                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }

//                        int fixed_block = CHUNK_SIZE/40;
                    byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, 8);
                    BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8L); // 转换为bit数
                    long duration = System.nanoTime() - startTime;
                    modelTime = modelTime.add(BigDecimal.valueOf(duration));
                    modelCost = modelCost.add(cur_cost);

                    // 新增：测试解压性能
                    long startDecodeTime = System.nanoTime();
                    long[] decodedData = decodeBitPacking(compressedData, bitWidths, 8, scaledInts.length);
                    long[] originalData = ts2diffDecode(decodedData,ter.getFirstValue(),ter.getMinDiff());
                    long decodeDuration = System.nanoTime() - startDecodeTime;
                    modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                }

            }
            BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
            modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
            modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP); // 平均解压时间

            BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
            BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
            BigDecimal modelDecodeTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000L)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);

            // 更新输出记录，包含解压吞吐率
            String[] record = {
                    file.toString(),
                    "BP",
                    modelTime_throughput.toPlainString(),
                    modelDecodeTime_throughput.toPlainString(),
                    String.valueOf(numbers.size()),
                    modelCost.toPlainString(),
                    model_ratio.toPlainString()
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
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_TS2DIFF_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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
            int time_of_repeat = all_time_of_repeat;

            for(int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal modelTime = BigDecimal.ZERO;
                BigDecimal modelDecodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    int chunksize = CHUNK_SIZE+1;
                    for (int i = 0; i < numbers.size(); i += chunksize) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + chunksize, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + chunksize, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();

                        TSDIFFEncodedResult ter = ts2diff(scaledInt);
                        long[] scaledInts = ter.getEncodedData();
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
                        byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                        BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8); // 转换为bit数
                        long duration = System.nanoTime() - startTime;
                        modelTime = modelTime.add(BigDecimal.valueOf(duration));
                        modelCost = modelCost.add(cur_cost);

                        long decodeStart = System.nanoTime();
                        long[] decodedData = decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                        ts2diffDecode(decodedData, ter.getFirstValue(), ter.getMinDiff());
                        long decodeDuration = System.nanoTime() - decodeStart;
                        modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    decodeThroughput =
                            numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                }
                String[] record = {
                        file.toString(),
                        "BP",
                        modelTime_throughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    // 新增方法：测试不同chunk size的表现
    @Test
    public void TS2DIFFVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_TS2DIFF_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8}; //, 1024*8

        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
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

            int time_of_repeat = 100; // 减少重复次数以加快测试速度
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
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                chunkSize += 1;
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 3; pack_size_exp < 4; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;


                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {

                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInt = new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);


                            long startTime = System.nanoTime();

                            TSDIFFEncodedResult ter = ts2diff(scaledInt);
                            long[] scaledInts = ter.getEncodedData();
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

                            byte[] compressedData = encodeBitPacking(paddedArray, bitWidths, pack_size);
                            BigDecimal cur_cost = BigDecimal.valueOf(compressedData.length * 8);
                            long duration = System.nanoTime() - startTime;

                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);

                            long decodeStart = System.nanoTime();
                            long[] decodedData = decodeBitPacking(compressedData, bitWidths, pack_size, scaledInts.length);
                            ts2diffDecode(decodedData, ter.getFirstValue(), ter.getMinDiff());
                            long decodeDuration = System.nanoTime() - decodeStart;
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize-1),
                            file.toString(),
                            "BP",
                            modelTime_throughput.toPlainString(),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            modelCost.toPlainString(),
                            String.valueOf(pack_size),
                            model_ratio.toPlainString()
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }

}