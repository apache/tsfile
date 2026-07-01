package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
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
public class SprintzBDCTest {

    private static final int CHUNK_SIZE = 1024;

    // -------------------- 工具 --------------------
    public static int getBitWidth(int num) {
        if (num == 0) return 1;
        return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static void intToBytesBE(int value, byte[] dst, int pos) {
        dst[pos] = (byte) (value >> 24);
        dst[pos+1] = (byte) (value >> 16);
        dst[pos+2] = (byte) (value >> 8);
        dst[pos+3] = (byte) (value);
    }
    public static void packCountToBytesBE(int value, byte[] dst, int pos) {
        dst[pos] = (byte) (value);
    }

    public static int bytesToIntBE(byte[] src, int pos) {
        int v = 0;
        v |= (src[pos] & 0xFF) << 24;
        v |= (src[pos+1] & 0xFF) << 16;
        v |= (src[pos+2] & 0xFF) << 8;
        v |= (src[pos+3] & 0xFF);
        return v;
    }
    public static int bytesToPackCount(byte[] src, int pos) {
        int v = 0;
        v |= (src[pos] & 0xFF);
        return v;
    }

    // -------------------- 缩放 (无 Delta/ZigZag) --------------------
    private static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
        int size = numbers.size();
        int[] result = new int[size];
        if (size == 0) return result;

        BigDecimal min = null;
        BigDecimal[] scaledValues = new BigDecimal[size];

        for (int i = 0; i < size; i++) {
            BigDecimal val = new BigDecimal(numbers.get(i)).multiply(scale);
            scaledValues[i] = val;
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
        }

        for (int i = 0; i < size; i++) {
            BigDecimal current = scaledValues[i].subtract(min);
            result[i] = current.toBigInteger().intValue();
        }
        return result;
    }

    // -------------------- bit-packing (基于位缓冲，行为与上次 pack8/unpack8 思路一致) --------------------
    /**
     * 将 values（任意长度，但通常为 packSize 的整数倍）以 width 位连续写成字节流（高位先出）。
     * 返回字节数组（末尾会按位补齐到整字节）。
     */
    private static byte[] bitPackList(ArrayList<Integer> values, int width) {
        if (values == null || values.isEmpty()) return new byte[0];

        // 计算需要的字节数
        int totalBits = values.size() * width;
        int totalBytes = (totalBits + 7) / 8;
        byte[] encoded_result = new byte[totalBytes];

        // 处理8的倍数个数值
        int encode_pos = bitPacking(values, 0, width, 0, encoded_result);

        // 处理剩余的值（如果不是8的倍数）
        int processedCount = (values.size() / 8) * 8;
        int remaining = values.size() - processedCount;

        if (remaining > 0) {
            // 使用原来的位打包逻辑处理剩余的值
            long bitBuffer = 0L;
            int bitCount = 0;
            long mask = (width >= 63) ? -1L : ((1L << width) - 1L);

            for (int i = processedCount; i < values.size(); i++) {
                int v = values.get(i);
                long vv = ((long) v) & mask;
                bitBuffer = (bitBuffer << width) | vv;
                bitCount += width;

                while (bitCount >= 8) {
                    int shift = bitCount - 8;
                    int b = (int) ((bitBuffer >>> shift) & 0xFFL);
                    encoded_result[encode_pos++] = (byte) b;
                    bitCount -= 8;
                    if (bitCount == 0) {
                        bitBuffer = 0L;
                    } else {
                        bitBuffer = bitBuffer & ((1L << bitCount) - 1L);
                    }
                }
            }

            // 写剩余的不足一字节的位
            if (bitCount > 0) {
                int b = (int) ((bitBuffer << (8 - bitCount)) & 0xFFL);
                encoded_result[encode_pos++] = (byte) b;
            }
        }

        // 创建正确大小的结果数组
        if (encode_pos < totalBytes) {
            byte[] result = new byte[encode_pos];
            System.arraycopy(encoded_result, 0, result, 0, encode_pos);
            return result;
        }

        return encoded_result;
    }

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,
                                   byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values.get(valueIdx) << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
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


    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos,
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
    public static byte[] encodeDynamicPacking(int[] paddedArray, int packSize, int qmbdLen) {
        if (paddedArray == null) return new byte[0];
        int totalGroups = paddedArray.length / packSize;

        ArrayList<Integer> queueValues = new ArrayList<>();
        int maxBD = 0;
        final int SUBHEADER_BITS_ESTIMATE = 40; // 5 bytes header ~= 40 bits

        List<byte[]> packPayloads = new ArrayList<>();
        List<Integer> packNumGroups = new ArrayList<>();
        List<Integer> packBitWidths = new ArrayList<>();

        for (int g = 0; g < totalGroups; g++) {
            int start = g * packSize;
            int groupMax = 0;
            for (int k = 0; k < packSize; k++) {
                int val = paddedArray[start + k];
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

    // decode 对应的 encodeDynamicPacking 格式
    public static int[] decodeDynamicPacking(byte[] encoded, int packSize, int originalLength) {
        if (encoded == null || encoded.length < 1) return new int[0];
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
        int[] out = new int[originalLength];
        for (int i = 0; i < originalLength && i < all.size(); i++) out[i] = all.get(i);
        if (all.size() < originalLength) {
            for (int i = all.size(); i < originalLength; i++) out[i] = 0;
        }
        return out;
    }
    // Sprintz编码（带ZigZag）
    public static int[] sprintz(int[] numbers) {
        int size = numbers.length;
        int[] result = new int[size];

        int first = numbers[0];
        result[0] = first;

        int prev = first;
        for (int i = 1; i < size; i++) {
            int current = numbers[i];
            int diff = current - prev;
            // ZigZag编码
            result[i] = (diff << 1) ^ (diff >> 31);
            prev = current;
        }

        return result;
    }

    // Sprintz解码（带ZigZag解码）
    public static int[] sprintzDecode(int[] encodedData) {
        int size = encodedData.length;
        int[] result = new int[size];

        if (size == 0) return result;

        result[0] = encodedData[0];
        int prev = result[0];

        for (int i = 1; i < size; i++) {
            int zigzagEncoded = encodedData[i];
            // ZigZag解码
            int diff = (zigzagEncoded >>> 1) ^ (-(zigzagEncoded & 1));
            result[i] = prev + diff;
            prev = result[i];
        }

        return result;
    }


    // -------------------- 主测试（参考你原 main） --------------------
    public static void main(String[] args) throws IOException {
        System.out.println("\nPerformance Testing (Dynamic pack over 8-values groups)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_BDC";
        File outputDir = new File(outputDirstr);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        if (!dir.exists()) {
            System.err.println("Directory not found: " + directory);
            return;
        }

        final int groupSize = 8; // 每8个值一个 group
        final int qmbdLen = 8;  // QMBD 队列上限（可调）

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
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

            int time_of_repeat = 50;
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

                    int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                    int[] scaledInts = sprintz(scaledInt);
                    // pad to multiple of groupSize
                    int remainder = scaledInts.length % groupSize;
                    int paddingLength = (remainder == 0) ? 0 : groupSize - remainder;
                    int[] paddedArray = new int[scaledInts.length + paddingLength];
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
                    int[] decoded = decodeDynamicPacking(compressed, groupSize,paddedArray.length);
                    int[] decodedInts = sprintzDecode(decoded);
                    long decDur = System.nanoTime() - startDec;
                    modelDecodeTime += decDur;

//                    // 验证（只在第一次迭代验证原始 scaledInts 部分）
//                    if (j == 0) {
//                        boolean ok = true;
//                        for (int k = 0; k < scaledInts.length; k++) {
//                            if (scaledInts[k] != decoded[k]) {
//                                ok = false;
//                                System.err.println("Mismatch at idx " + k + ": expect " + scaledInts[k] + " got " + decoded[k]);
//                                break;
//                            }
//                        }
//                        if (ok) System.out.println("Chunk decoded OK.");
//                    }
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
        int[] paddedArray = new int[testData.length + paddingLength];
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
        int[] decoded = decodeDynamicPacking(compressed, groupSize, paddedArray.length);
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
        int[] smallData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        int smallRemainder = smallData.length % groupSize;
        int smallPaddingLength = (smallRemainder == 0) ? 0 : groupSize - smallRemainder;
        int[] smallPadded = new int[smallData.length + smallPaddingLength];
        System.arraycopy(smallData, 0, smallPadded, 0, smallData.length);

        byte[] smallCompressed = encodeDynamicPacking(smallPadded, groupSize, qmbdLen);
        int[] smallDecoded = decodeDynamicPacking(smallCompressed, groupSize, smallPadded.length);

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
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_BDC_vary_pack_size";
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
                    "Encoding Time",
                    "Decoding Time",
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

            int time_of_repeat = 10;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定chunk size为1024
            int chunkSize = 1024;
            int qmbdLen = 8;

            // 测试不同的pack size: 2^0 到 2^9 (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
            for (int pack_size_exp = 0; pack_size_exp < 10; pack_size_exp++) {
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
                        int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        int[] scaledInts = sprintz(scaledInt);

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        int[] paddedArray = new int[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);

                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        int[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        int[] decodedsprintz = sprintzDecode(decoded);
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
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes (BDC)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_BDC_vary_m";
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
                    "Chunk Size",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time (points/ms)",
                    "Decoding Time (points/ms)",
                    "Points",
                    "Compressed Size (bits)",
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

            int time_of_repeat = 5; // 减少重复次数以加快测试速度
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 固定pack size为8，QMBD长度也为8
            int packSize = 8;
            int qmbdLen = 8;

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
                        int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();

                        int[] scaledInts = sprintz(scaledInt);

                        // 填充数据，使其长度为packSize的倍数
                        int remainder = scaledInts.length % packSize;
                        int paddingLength = (remainder == 0) ? 0 : packSize - remainder;
                        int[] paddedArray = new int[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);

                        // 编码
                        byte[] compressed = encodeDynamicPacking(paddedArray, packSize, qmbdLen);
                        long duration = System.nanoTime() - startTime;

                        // 解码
                        long decodeStartTime = System.nanoTime();
                        int[] decoded = decodeDynamicPacking(compressed, packSize, paddedArray.length);
                        int[] sprintzdecoded = sprintzDecode(decoded);
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
                    BigDecimal modelTimeMs = modelTime.divide(BigDecimal.valueOf(1000000), 10, BigDecimal.ROUND_HALF_UP);
                    modelTimeThroughput = numbersSizeBD.divide(modelTimeMs, 10, BigDecimal.ROUND_HALF_UP);
                }

                // 解码吞吐量（points/ms）
                BigDecimal decodeThroughput = BigDecimal.ZERO;
                if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal modelDecodeTimeMs = modelDecodeTime.divide(BigDecimal.valueOf(1000000), 10, BigDecimal.ROUND_HALF_UP);
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
}
