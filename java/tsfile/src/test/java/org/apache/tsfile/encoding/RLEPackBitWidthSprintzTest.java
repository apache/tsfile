package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RLEPackBitWidthSprintzTest {

    private static final int CHUNK_SIZE = 1024;

    // 固定packsize选项
    private static final int[] FIXED_PACK_SIZES = { 16};
    private static final int BITWIDTH_BITS = 6; // 固定packsize中存储bitwidth的位数

    public static void main(String[] args) throws IOException {
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_AdaptiveSprintz";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
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
                    "Compression Ratio",
                    "Selected Scheme"
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
            int time_of_repeat = 50;

            int modelCost = 0;
            long modelTime = 0;
            long modelDecodeTime = 0;
            String selectedScheme = "";

            for (int j = 0; j < time_of_repeat; j++) {
                int totalCost = 0;
                String currentRunScheme = "";

                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if (chunkNumbers.size() < 8) continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);

                    int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    int[] scaledInts = sprintz(scaledInt);

                    // 自适应编码
                    byte[] compressedData;
                    int cur_cost;

                    // 1. 计算RLE方案cost（packsize=8）
                    byte[] rleEncoded = encodeSprintzWithRLE(scaledInts, 8);
                    int rleCost = rleEncoded.length * 8;

                    // 2. 计算固定packsize方案的cost，选择最小的
                    int minFixedCost = Integer.MAX_VALUE;
                    byte[] bestFixedEncoded = null;
                    int bestPackSize = 8;

                    for (int packSize : FIXED_PACK_SIZES) {
                        byte[] fixedEncoded = encodeSprintzWithFixedPackSize(scaledInts, packSize);
                        int fixedCost = fixedEncoded.length * 8;

                        if (fixedCost < minFixedCost) {
                            minFixedCost = fixedCost;
                            bestFixedEncoded = fixedEncoded;
                            bestPackSize = packSize;
                        }
                    }

                    // 3. 比较两种方案，选择更小的
                    if (rleCost <= minFixedCost) {
                        compressedData = rleEncoded;
                        cur_cost = rleCost;
                        currentRunScheme = "Sprintz+RLE";
                    } else {
                        compressedData = bestFixedEncoded;
                        cur_cost = minFixedCost;
                        currentRunScheme = "Sprintz+FixedPack" + bestPackSize;
                    }

                    long duration = System.nanoTime() - startTime;

                    long decodeStartTime = System.nanoTime();
                    int[] decompressedPacked = decodeAdaptiveSprintz(compressedData, scaledInts.length);
                    int[] decompressedScaled = sprintzDecode(decompressedPacked);
                    long decodeDuration = System.nanoTime() - decodeStartTime;

                    modelTime += duration;
                    modelDecodeTime += decodeDuration;
                    modelCost += cur_cost;
                }

                if (j == 0) {
                    selectedScheme = currentRunScheme;
                }
            }

            modelCost /= time_of_repeat;
            modelTime = modelTime / time_of_repeat;
            modelDecodeTime = modelDecodeTime / time_of_repeat;

            double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
            double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);
            double decodeThroughput = (double) (numbers.size() * 8000) / (double) modelDecodeTime;

            String[] record = {
                    file.toString(),
                    "AdaptiveSprintz",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(decodeThroughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio),
                    selectedScheme
            };
            writer.writeRecord(record);
            writer.close();
        }
    }

    // 使用RLE编码bitwidths的Sprintz方案
    public static byte[] encodeSprintzWithRLE(int[] data, int packSize) {
        List<Byte> result = new ArrayList<>();

        // 添加sign标识：0表示RLE方案
        result.add((byte) 0);
        result.add((byte) 0);

        // 处理数据，确保长度是packSize的倍数
        int actual_length = data.length;
        int remainder = actual_length % packSize;

        if (remainder != 0) {
            int paddingLength = packSize - remainder;
            int[] paddedArray = new int[actual_length + paddingLength];
            System.arraycopy(data, 0, paddedArray, 0, actual_length);
            data = paddedArray;
            actual_length = paddedArray.length;
        }

        int groupCount = actual_length / packSize;
        int[] bitWidths = new int[groupCount];

        // 计算每个组的bitwidth
        for (int i = 0; i < groupCount; i++) {
            int maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                if (data[startIdx + j] > maxInGroup) {
                    maxInGroup = data[startIdx + j];
                }
            }
            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        // 对bitwidths进行RLE编码
        List<Byte> rleEncoded = encodeRLE(bitWidths);
        result.addAll(rleEncoded);

        // 进行bit-packing
        int totalBitPackedBytes = 0;
        for (int bitWidth : bitWidths) {
            totalBitPackedBytes += (bitWidth * packSize + 7) / 8;
        }

        byte[] bitPackedData = new byte[totalBitPackedBytes];
        int encodePos = 0;

        for (int i = 0; i < groupCount; i++) {
            int startIndex = i * packSize;
            ArrayList<Integer> groupData = new ArrayList<>();
            for (int j = 0; j < packSize; j++) {
                if (startIndex + j < data.length) {
                    groupData.add(data[startIndex + j]);
                } else {
                    groupData.add(0);
                }
            }
            encodePos = bitPacking(groupData, 0, bitWidths[i], encodePos, bitPackedData);
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

    // 使用固定packsize的Sprintz方案
    public static byte[] encodeSprintzWithFixedPackSize(int[] data, int packSize) {
        List<Byte> result = new ArrayList<>();

        // 添加sign标识：1表示固定packsize方案
        result.add((byte) 1);
        result.add((byte) 1);

        // 添加packsize标识：0->8, 1->16, 2->32
        byte packSizeFlag;
        if (packSize == 8) packSizeFlag = 0;
        else if (packSize == 16) packSizeFlag = 1;
        else packSizeFlag = 2;
        result.add(packSizeFlag);

        // 处理数据，确保长度是packSize的倍数
        int actual_length = data.length;
        int remainder = actual_length % packSize;

        if (remainder != 0) {
            int paddingLength = packSize - remainder;
            int[] paddedArray = new int[actual_length + paddingLength];
            System.arraycopy(data, 0, paddedArray, 0, actual_length);
            data = paddedArray;
            actual_length = paddedArray.length;
        }

        int groupCount = actual_length / packSize;
        int[] bitWidths = new int[groupCount];

        // 计算每个组的bitwidth
        for (int i = 0; i < groupCount; i++) {
            int maxInGroup = 0;
            int startIdx = i * packSize;
            for (int j = 0; j < packSize; j++) {
                if (data[startIdx + j] > maxInGroup) {
                    maxInGroup = data[startIdx + j];
                }
            }
            int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);
            bitWidths[i] = bitWidth;
        }

        // 编码bitwidths（每个用6bits）
        int totalBitWidthBits = groupCount * BITWIDTH_BITS;
        int bitWidthBytes = (totalBitWidthBits + 7) / 8;
        byte[] bitWidthEncoded = new byte[bitWidthBytes];

        int currentByte = 0;
        int currentBitPos = 0;

        for (int i = 0; i < groupCount; i++) {
            int bitWidth = bitWidths[i];

            // 写入6位bitwidth
            for (int bit = 5; bit >= 0; bit--) {
                int currentBit = (bitWidth >> bit) & 1;
                bitWidthEncoded[currentByte] |= (currentBit << (7 - currentBitPos));
                currentBitPos++;

                if (currentBitPos == 8) {
                    currentByte++;
                    currentBitPos = 0;
                }
            }
        }

        // 添加编码后的bitwidths
        for (byte b : bitWidthEncoded) {
            result.add(b);
        }

        // 进行bit-packing
        int totalBitPackedBytes = 0;
        for (int bitWidth : bitWidths) {
            totalBitPackedBytes += (bitWidth * packSize + 7) / 8;
        }

        byte[] bitPackedData = new byte[totalBitPackedBytes];
        int encodePos = 0;

        for (int i = 0; i < groupCount; i++) {
            int startIndex = i * packSize;
            ArrayList<Integer> groupData = new ArrayList<>();
            for (int j = 0; j < packSize; j++) {
                if (startIndex + j < data.length) {
                    groupData.add(data[startIndex + j]);
                } else {
                    groupData.add(0);
                }
            }
            encodePos = bitPacking(groupData, 0, bitWidths[i], encodePos, bitPackedData);
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

    // 自适应解码Sprintz数据
    public static int[] decodeAdaptiveSprintz(byte[] encodedData, int originalLength) {
        // 读取sign标识
        int sign = encodedData[0] & 0xFF;
        int pos = 2;

        if (sign == 0) {
            // RLE方案
            return decodeSprintzRLEBased(encodedData, pos, originalLength, 8);
        } else {
            // 固定packsize方案
            int packSizeFlag = encodedData[pos] & 0xFF;
            pos++;

            int packSize;
            if (packSizeFlag == 0) packSize = 8;
            else if (packSizeFlag == 1) packSize = 16;
            else packSize = 32;

            return decodeSprintzFixedPackSize(encodedData, pos, originalLength, packSize);
        }
    }

    // 解码RLE方案的Sprintz数据
    private static int[] decodeSprintzRLEBased(byte[] encodedData, int startPos, int originalLength, int packSize) {
        int pos = startPos;

        // 解析RLE编码的bitwidths
        int runCount = bytes2Integer(encodedData, pos, 4);
        pos += 4;

        int[] bitWidths = decodeRLE(encodedData, pos, runCount);
        pos += runCount * 2; // 每个游程占2字节

        // 解压bit-packed数据
        int[] result = new int[originalLength];
        int resultIndex = 0;
        int totalGroups = bitWidths.length;
        int currentBitPos = 0;

        for (int group = 0; group < totalGroups && resultIndex < originalLength; group++) {
            int bitWidth = bitWidths[group];
            int copyLength = Math.min(packSize, originalLength - resultIndex);

            for (int i = 0; i < copyLength; i++) {
                int value = 0;

                // 按位读取
                for (int bit = 0; bit < bitWidth; bit++) {
                    int currentBit = (encodedData[pos] >> (7 - currentBitPos)) & 1;
                    value = (value << 1) | currentBit;
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

            // 跳过剩余的数据位
            int remainingValues = packSize - copyLength;
            if (remainingValues > 0) {
                int bitsToSkip = remainingValues * bitWidth;
                currentBitPos += bitsToSkip;
                pos += currentBitPos / 8;
                currentBitPos = currentBitPos % 8;
            }
        }

        return result;
    }

    // 解码固定packsize方案的Sprintz数据
    private static int[] decodeSprintzFixedPackSize(byte[] encodedData, int startPos, int originalLength, int packSize) {
        int pos = startPos;

        // 计算group数量
        int groupCount = (int) Math.ceil((double) originalLength / packSize);
        int[] bitWidths = new int[groupCount];

        // 解码bitwidths（每个6bits）
        int currentByte = pos;
        int currentBitPos = 0;

        for (int i = 0; i < groupCount; i++) {
            int bitWidth = 0;

            // 读取6位bitwidth
            for (int bit = 0; bit < 6; bit++) {
                int currentBit = (encodedData[currentByte] >> (7 - currentBitPos)) & 1;
                bitWidth = (bitWidth << 1) | currentBit;
                currentBitPos++;

                if (currentBitPos == 8) {
                    currentByte++;
                    currentBitPos = 0;
                }
            }

            bitWidths[i] = bitWidth;
        }

        pos = currentByte + (currentBitPos > 0 ? 1 : 0);

        // 解压bit-packed数据
        int[] result = new int[originalLength];
        int resultIndex = 0;
        currentBitPos = 0;

        for (int group = 0; group < groupCount && resultIndex < originalLength; group++) {
            int bitWidth = bitWidths[group];
            int copyLength = Math.min(packSize, originalLength - resultIndex);

            for (int i = 0; i < copyLength; i++) {
                int value = 0;

                // 按位读取
                for (int bit = 0; bit < bitWidth; bit++) {
                    int currentBit = (encodedData[pos] >> (7 - currentBitPos)) & 1;
                    value = (value << 1) | currentBit;
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

            // 跳过剩余的数据位
            int remainingValues = packSize - copyLength;
            if (remainingValues > 0) {
                int bitsToSkip = remainingValues * bitWidth;
                currentBitPos += bitsToSkip;
                pos += currentBitPos / 8;
                currentBitPos = currentBitPos % 8;
            }
        }

        return result;
    }

    @Test
    public void TestVarPackSize() throws IOException {
        System.out.println("\nPerformance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_sprintz_RLE_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
//            if (!file.getName().equals("EPM-Education.csv") && !file.getName().equals("TH-Climate.csv")) continue;
            System.out.println("Processing " + file.getName() + "...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头
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

                int modelCost = 0;
                long modelTime = 0;
                String selectedScheme = "";

                for (int j = 0; j < time_of_repeat; j++) {
                    int totalCost = 0;
                    String currentRunScheme = "";

                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() < 8) continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        int[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startTime = System.nanoTime();
                        int[] scaledInts = sprintz(scaledInt);

                        // 自适应编码
                        byte[] compressedData;
                        int cur_cost;

                        // 1. 计算RLE方案cost
                        byte[] rleEncoded = encodeSprintzWithRLE(scaledInts, pack_size);
                        int rleCost = rleEncoded.length * 8;

                        // 2. 计算固定packsize方案cost（如果pack_size在可选范围内）
                        int fixedCost = Integer.MAX_VALUE;
                        if (pack_size == 8 || pack_size == 16 || pack_size == 32) {
                            byte[] fixedEncoded = encodeSprintzWithFixedPackSize(scaledInts, pack_size);
                            fixedCost = fixedEncoded.length * 8;
                        }

                        // 3. 选择较小者
                        if (rleCost <= fixedCost) {
                            compressedData = rleEncoded;
                            cur_cost = rleCost;
                            currentRunScheme = "Sprintz+RLE";
                        } else {
                            compressedData = encodeSprintzWithFixedPackSize(scaledInts, pack_size);
                            cur_cost = fixedCost;
                            currentRunScheme = "Sprintz+FixedPack" + pack_size;
                        }

                        long duration = System.nanoTime() - startTime;
                        modelTime += duration;
                        modelCost += cur_cost;
                    }

                    if (j == 0) {
                        selectedScheme = currentRunScheme;
                    }
                }

                modelCost /= time_of_repeat;
                modelTime = modelTime / time_of_repeat;
                double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);

                String[] record = {
                        file.toString(),
                        "AdaptiveSprintz",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    @Test
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_AdaptiveSprintz_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes
        int[] chunkSizes = {16 * 8, 32 * 8, 64 * 8, 128 * 8, 256 * 8, 512 * 8, 1024 * 8};

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            // 更新表头
            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio",
                    "Selected Scheme"
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

            int time_of_repeat = 50;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            // 预处理所有数据
            int batchSize = 1024;
            List<int[]> batches = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += batchSize) {
                int end = Math.min(numbers.size(), i + batchSize);
                List<String> batch = numbers.subList(i, end);
                int[] scaledBatch = scaleNumbers(batch, decimalMax);
                batches.add(scaledBatch);
            }

            int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
            int[] scaledInts_all = new int[totalLength];
            int currentIndex = 0;

            for (int[] batch : batches) {
                System.arraycopy(batch, 0, scaledInts_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }

            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                // 固定pack_size为8进行测试
                int pack_size = 8;
                int modelCost = 0;
                long modelTime = 0;
                String selectedScheme = "";

                for (int j = 0; j < time_of_repeat; j++) {
                    String currentRunScheme = "";

                    for (int i = 0; i < numbers.size(); i += chunkSize) {
                        int end = Math.min(i + chunkSize, numbers.size());
                        int[] scaledInt = new int[end - i];

                        if (end - i >= 0) {
                            System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);
                        }

                        long startTime = System.nanoTime();
                        int[] scaledInts = sprintz(scaledInt);

                        // 自适应编码
                        byte[] compressedData;
                        int cur_cost;

                        // 1. 计算RLE方案cost
                        byte[] rleEncoded = encodeSprintzWithRLE(scaledInts, pack_size);
                        int rleCost = rleEncoded.length * 8;

                        // 2. 计算固定packsize方案cost
                        int fixedCost = Integer.MAX_VALUE;
                        if (pack_size == 8 || pack_size == 16 || pack_size == 32) {
                            byte[] fixedEncoded = encodeSprintzWithFixedPackSize(scaledInts, pack_size);
                            fixedCost = fixedEncoded.length * 8;
                        }

                        // 3. 选择较小者
                        if (rleCost <= fixedCost) {
                            compressedData = rleEncoded;
                            cur_cost = rleCost;
                            currentRunScheme = "Sprintz+RLE";
                        } else {
                            compressedData = encodeSprintzWithFixedPackSize(scaledInts, pack_size);
                            cur_cost = fixedCost;
                            currentRunScheme = "Sprintz+FixedPack" + pack_size;
                        }

                        long duration = System.nanoTime() - startTime;
                        modelTime += duration;
                        modelCost += cur_cost;
                    }

                    if (j == 0) {
                        selectedScheme = currentRunScheme;
                    }
                }

                modelCost /= time_of_repeat;
                modelTime = modelTime / time_of_repeat;
                double model_ratio = (double) modelCost / (double) (numbers.size() * 64);
                double modelTime_throughput = (double) (numbers.size() * 8000) / (double) (modelTime);

                String[] record = {
                        String.valueOf(chunkSize / 8),
                        file.toString(),
                        "AdaptiveSprintz",
                        String.valueOf(modelTime_throughput),
                        String.valueOf(numbers.size()),
                        String.valueOf(modelCost),
                        String.valueOf(pack_size),
                        String.valueOf(model_ratio),
                        selectedScheme
                };
                writer.writeRecord(record);
            }

            writer.close();
        }
    }

    // 以下是原有的辅助方法，保持不变
    public static int getCount(long long1, int mask) {
        return ((int) (long1 & mask));
    }

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

    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos,
                                 byte[] encoded_result) {
        int totalCount = numbers.size() - start;
        int currentBytePos = encode_pos;
        int currentBitPos = 0;

        for (int i = 0; i < totalCount; i++) {
            int value = numbers.get(start + i);
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

    public static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        int scale = (int) Math.pow(10, decimalMax);
        int size = numbers.size();
        int[] result = new int[size];

        if (size == 0) {
            return result;
        }

        // 1. Parse all numbers and scale them up
        int[] scaledValues = new int[size];
        for (int i = 0; i < size; i++) {
            String numStr = numbers.get(i);
            String[] parts = numStr.split("\\.");
            int whole = Integer.parseInt(parts[0]);

            int fraction = 0;
            if (parts.length > 1) {
                String fractionStr = parts[1];
                if (fractionStr.length() < decimalMax) {
                    while (fractionStr.length() < decimalMax) {
                        fractionStr += "0";
                    }
                } else if (fractionStr.length() > decimalMax) {
                    fractionStr = fractionStr.substring(0, decimalMax);
                }
                fraction = Integer.parseInt(fractionStr);
            }

            scaledValues[i] = whole * scale + fraction;
        }
        return scaledValues;
    }

    public static int[] sprintz(int[] numbers) {
        int size = numbers.length;
        int[] result = new int[size];

        int first = numbers[0];
        result[0] = first;

        int prev = first;
        for (int i = 1; i < size; i++) {
            int current = numbers[i];
            int diff = current - prev;
            result[i] = (diff << 1) ^ (diff >> 31); // ZigZag encoding
            prev = current;
        }

        return result;
    }

    public static int[] sprintzDecode(int[] encodedData) {
        int size = encodedData.length;
        int[] result = new int[size];

        if (size == 0) return result;

        result[0] = encodedData[0];
        int prev = result[0];

        for (int i = 1; i < size; i++) {
            int zigzagEncoded = encodedData[i];
            int diff = (zigzagEncoded >>> 1) ^ -(zigzagEncoded & 1); // ZigZag解码
            result[i] = prev + diff;
            prev = result[i];
        }

        return result;
    }

    public static int[] decodeRLE(byte[] data, int startPos, int runCount) {
        List<Integer> bitWidths = new ArrayList<>();

        for (int i = 0; i < runCount; i++) {
            int runLength = data[startPos + i * 2] & 0xFF;
            int value = data[startPos + i * 2 + 1] & 0xFF;

            for (int j = 0; j < runLength; j++) {
                bitWidths.add(value);
            }
        }

        int[] result = new int[bitWidths.size()];
        for (int i = 0; i < bitWidths.size(); i++) {
            result[i] = bitWidths.get(i);
        }
        return result;
    }

    public static List<Byte> encodeRLE(int[] bitWidths) {
        List<Byte> result = new ArrayList<>();

        if (bitWidths.length == 0) {
            return result;
        }

        int length_bitWidths_list = bitWidths.length;
        int run_count = 0;
        int[] run_lengths = new int[length_bitWidths_list];
        int[] run_values = new int[length_bitWidths_list];
        int pre_bit_width = bitWidths[0];
        int pre_run_length = 1;

        for (int i = 1; i < length_bitWidths_list; i++) {
            if (bitWidths[i] == pre_bit_width) {
                pre_run_length++;
            } else {
                run_lengths[run_count] = pre_run_length;
                run_values[run_count++] = pre_bit_width;
                pre_bit_width = bitWidths[i];
                pre_run_length = 1;
            }
        }
        run_lengths[run_count] = pre_run_length;
        run_values[run_count++] = pre_bit_width;

        // 写入run_count（4字节）
        result.add((byte) (run_count >> 24));
        result.add((byte) (run_count >> 16));
        result.add((byte) (run_count >> 8));
        result.add((byte) run_count);

        // 写入每个游程
        for (int i = 0; i < run_count; i++) {
            encodeRLERun(result, run_lengths[i], run_values[i]);
        }

        return result;
    }

    private static void encodeRLERun(List<Byte> result, int runLength, int value) {
        result.add((byte) runLength);
        result.add((byte) value);
    }
}