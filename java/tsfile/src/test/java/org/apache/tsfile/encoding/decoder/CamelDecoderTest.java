package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.CamelEncoder;
import org.apache.tsfile.encoding.encoder.DoublePrecisionEncoderV2;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CamelDecoderTest {
    @Test
    public void testSimpleCaseForDebug() throws Exception {
        System.out.println("Double.MIN=" + Double.MIN_VALUE);
        double[] original = new double[] { 0.1, 0.12345 };
        CamelEncoder compressor = new CamelEncoder();
        // 压缩所有数据点
        for (double v : original) {
            compressor.addValue(v);
        }
        long totalWrittenBits = compressor.close();
        ByteArrayOutputStream compressed = compressor.getByteArrayOutputStream();

        // 解压并比对
        InputStream inputStream = new ByteArrayInputStream(compressed.toByteArray());
        CamelDecoder decompressor = new CamelDecoder(inputStream, totalWrittenBits);
        List<Double> result = decompressor.getValues();
        assertEquals(original.length, result.size());
        for (int i = 0; i < original.length; i++) {
            // 允许很小的浮点误差，例如 1e-9
            assertEquals(original[i], result.get(i), 0);
        }
    }

    @Test
    public void testCityTempCompression() throws Exception {
        // 1. 读取CSV文件
        String filePath = "D:/workspace/camel/src/test/resources/ElfTestData/City-temp.csv";
        List<Double> originalData = Files.lines(Paths.get(filePath))
                .map(Double::parseDouble)
                .collect(Collectors.toList());
        System.out.println("Read " + originalData.size() + " data points");

        // 2. 压缩
        long startTime = System.nanoTime();
        CamelEncoder compressor = new CamelEncoder();
        for (Double value : originalData) {  // 改用传统for循环处理异常
            compressor.addValue(value);
        }
        long totalBits = compressor.close();
        ByteArrayOutputStream compressedStream = compressor.getByteArrayOutputStream();
        long compressTime = System.nanoTime() - startTime;

        // 3. 解压缩
        startTime = System.nanoTime();
        InputStream inputStream = new ByteArrayInputStream(compressedStream.toByteArray());
        List<Double> decompressedData = new CamelDecoder(inputStream, totalBits).getValues();
        long decompressTime = System.nanoTime() - startTime;

        // 4. 计算统计信息
        double originalSize = originalData.size() * 8.0;
        double compressedSize = compressedStream.size();
        double compressionRatio = originalSize / compressedSize;

        System.out.println("\nCompression Results:");
        System.out.printf("Original size: %.2f bytes\n", originalSize);
        System.out.printf("Compressed size: %.2f bytes\n", compressedSize);
        System.out.printf("Compression ratio: %.2f:1\n", compressionRatio);
        System.out.printf("Compression time: %.3f ms\n", compressTime / 1e6);
        System.out.printf("Decompression time: %.3f ms\n", decompressTime / 1e6);

        assertEquals(originalData.size(), decompressedData.size());
    }

    @Test
    public void testGorillaCompression() throws Exception {
        // 1. 读取CSV文件
        String filePath = "D:/workspace/camel/src/test/resources/ElfTestData/City-temp.csv";
        List<Double> originalData = Files.lines(Paths.get(filePath))
                .map(Double::parseDouble)
                .collect(Collectors.toList());
        System.out.println("Read " + originalData.size() + " data points");

        // 2. Gorilla压缩
        long startTime = System.nanoTime();
        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
        DoublePrecisionEncoderV2 encoder = new DoublePrecisionEncoderV2();
        for (Double value : originalData) {
            encoder.encode(value, compressedStream);
        }
        encoder.flush(compressedStream); // 结束编码
        long compressTime = System.nanoTime() - startTime;

        // 3. Gorilla解压缩
        startTime = System.nanoTime();
        ByteBuffer buffer = ByteBuffer.wrap(compressedStream.toByteArray());
        DoublePrecisionDecoderV2 decoder = new DoublePrecisionDecoderV2();
        List<Double> decompressedData = new ArrayList<>();
        while (decoder.hasNext(buffer)) { // 假设Decoder有hasNext()方法
            decompressedData.add(decoder.readDouble(buffer));
        }
        long decompressTime = System.nanoTime() - startTime;

        // 4. 计算统计信息
        double originalSize = originalData.size() * 8.0; // 每个double 8字节
        double compressedSize = compressedStream.size();
        double compressionRatio = originalSize / compressedSize;

        System.out.println("\nGorilla Compression Results:");
        System.out.printf("Original size: %.2f bytes\n", originalSize);
        System.out.printf("Compressed size: %.2f bytes\n", compressedSize);
        System.out.printf("Compression ratio: %.2f:1\n", compressionRatio);
        System.out.printf("Compression time: %.3f ms\n", compressTime / 1e6);
        System.out.printf("Decompression time: %.3f ms\n", decompressTime / 1e6);

        assertEquals(originalData.size(), decompressedData.size());
    }

    // 要处理的目录路径
    private static final String INPUT_DIR = "D:/workspace/camel/src/test/resources/ElfTestData/";

    @Test
    public void testAllCsvFilesCompression() throws Exception {
        // 1. 获取目录下所有CSV文件
        List<Path> csvFiles = Files.list(Paths.get(INPUT_DIR))
                .filter(path -> path.toString().endsWith(".csv"))
                .collect(Collectors.toList());

        if (csvFiles.isEmpty()) {
            System.out.println("No CSV files found in directory: " + INPUT_DIR);
            return;
        }

        System.out.println("Found " + csvFiles.size() + " CSV files to camel process:");
        csvFiles.forEach(path -> System.out.println("  - " + path.getFileName()));

        // 2. 处理每个文件
        for (Path csvFile : csvFiles) {
            System.out.println("\n=== Processing: " + csvFile.getFileName() + " ===");
            processSingleFile(csvFile);
        }
    }

    private void processSingleFile(Path csvFile) throws Exception {
        // 1. 读取CSV文件
        List<Double> originalData;
        try {
            originalData = Files.lines(csvFile)
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .map(Double::parseDouble)
                    .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            System.err.println("Skipping file due to parsing error: " + csvFile);
            return;
        }

        System.out.println("Read " + originalData.size() + " data points");

        // 2. 压缩
        long startTime = System.nanoTime();
        CamelEncoder compressor = new CamelEncoder();
        for (Double value : originalData) {
            try {
                compressor.addValue(value);
            } catch (IOException e) {
                throw new RuntimeException("Compression failed", e);
            }
        }
        long totalBits = compressor.close();
        ByteArrayOutputStream compressedStream = compressor.getByteArrayOutputStream();
        long compressTime = System.nanoTime() - startTime;

        // 3. 解压缩
        startTime = System.nanoTime();
        InputStream inputStream = new ByteArrayInputStream(compressedStream.toByteArray());
        List<Double> decompressedData = new CamelDecoder(inputStream, totalBits).getValues();
        long decompressTime = System.nanoTime() - startTime;

        // 4. 计算统计信息
        double originalSize = originalData.size() * 8.0;
        double compressedSize = compressedStream.size();
        double compressionRatio = originalSize / compressedSize;

        System.out.println("\nCompression Results:");
        System.out.printf("Original size: %.2f bytes\n", originalSize);
        System.out.printf("Compressed size: %.2f bytes\n", compressedSize);
        System.out.printf("Compression ratio: %.2f:1\n", compressionRatio);
        System.out.printf("Compression time: %.3f ms\n", compressTime / 1e6);
        System.out.printf("Decompression time: %.3f ms\n", decompressTime / 1e6);

        // 5. 验证数据完整性
        assertEquals(originalData.size(), decompressedData.size());
    }

    @Test
    public void testAllCsvFilesWithGorilla() throws Exception {
        // 1. 获取目录下所有CSV文件
        List<Path> csvFiles = Files.list(Paths.get(INPUT_DIR))
                .filter(path -> path.toString().endsWith(".csv"))
                .collect(Collectors.toList());

        if (csvFiles.isEmpty()) {
            System.out.println("No CSV files found in directory: " + INPUT_DIR);
            return;
        }

        System.out.println("Found " + csvFiles.size() + " CSV files for Gorilla compression:");
        csvFiles.forEach(path -> System.out.println("  - " + path.getFileName()));

        // 2. 处理每个文件
        for (Path csvFile : csvFiles) {
            System.out.println("\n=== Processing with Gorilla: " + csvFile.getFileName() + " ===");
            processSingleFileWithGorilla(csvFile);
        }
    }

    private void processSingleFileWithGorilla(Path csvFile) throws Exception {
        // 1. 读取CSV文件
        List<Double> originalData;
        try {
            originalData = Files.lines(csvFile)
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .map(Double::parseDouble)
                    .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            System.err.println("Skipping file due to parsing error: " + csvFile);
            return;
        }

        System.out.println("Read " + originalData.size() + " data points");

        // 2. Gorilla压缩
        long startTime = System.nanoTime();
        ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
        DoublePrecisionEncoderV2 encoder = new DoublePrecisionEncoderV2();
        for (Double value : originalData) {
            encoder.encode(value, compressedStream);
        }
        encoder.flush(compressedStream); // 结束编码
        long compressTime = System.nanoTime() - startTime;

        // 3. Gorilla解压缩
        startTime = System.nanoTime();
        ByteBuffer buffer = ByteBuffer.wrap(compressedStream.toByteArray());
        DoublePrecisionDecoderV2 decoder = new DoublePrecisionDecoderV2();
        List<Double> decompressedData = new ArrayList<>();
        while (decoder.hasNext(buffer)) { // 假设Decoder有hasNext(ByteBuffer)方法
            decompressedData.add(decoder.readDouble(buffer));
        }
        long decompressTime = System.nanoTime() - startTime;

        // 4. 计算统计信息
        double originalSize = originalData.size() * 8.0;
        double compressedSize = compressedStream.size();
        double compressionRatio = originalSize / compressedSize;

        System.out.println("\nGorilla Compression Results:");
        System.out.printf("Original size: %.2f bytes\n", originalSize);
        System.out.printf("Compressed size: %.2f bytes\n", compressedSize);
        System.out.printf("Compression ratio: %.2f:1\n", compressionRatio);
        System.out.printf("Compression time: %.3f ms\n", compressTime / 1e6);
        System.out.printf("Decompression time: %.3f ms\n", decompressTime / 1e6);

        // 5. 验证数据完整性
        assertEquals(originalData.size(), decompressedData.size());
    }

    @Test
    public void testBasicCompressDecompress() throws Exception {
        // 原始测试数据（保留原有测试用例）
        double[] original = new double[] {
                100.0, -100.52, 100.75, 100.23, 101.25, 100.25,
                // 新增测试数据：覆盖 .01 ~ .99 的小数部分
                100.01, 100.02, 100.03, 100.04, 100.05, 100.06, 100.07, 100.08, 100.09,
                100.10, 100.11, 100.12, 100.13, 100.14, 100.15, 100.16, 100.17, 100.18, 100.19,
                100.20, 100.21, 100.22, 100.23, 100.24, 100.25, 100.26, 100.27, 100.28, 100.29,
                100.30, 100.31, 100.32, 100.33, 100.34, 100.35, 100.36, 100.37, 100.38, 100.39,
                100.40, 100.41, 100.42, 100.43, 100.44, 100.45, 100.46, 100.47, 100.48, 100.49,
                100.50, 100.51, 100.52, 100.53, 100.54, 100.55, 100.56, 100.57, 100.58, 100.59,
                100.60, 100.61, 100.62, 100.63, 100.64, 100.65, 100.66, 100.67, 100.68, 100.69,
                100.70, 100.71, 100.72, 100.73, 100.74, 100.75, 100.76, 100.77, 100.78, 100.79,
                100.80, 100.81, 100.82, 100.83, 100.84, 100.85, 100.86, 100.87, 100.88, 100.89,
                100.90, 100.91, 100.92, 100.93, 100.94, 100.95, 100.96, 100.97, 100.98, 100.99,
                // 额外边界测试
                -100.01, -100.99, 0.01, 0.99, 999.99, -999.99
        };

        CamelEncoder compressor = new CamelEncoder();
        // 压缩所有数据点
        for (double v : original) {
            compressor.addValue(v);
        }
        long totalWrittenBits = compressor.close();
        ByteArrayOutputStream compressed = compressor.getByteArrayOutputStream();

        // 解压并比对
        InputStream inputStream = new ByteArrayInputStream(compressed.toByteArray());
        CamelDecoder decompressor = new CamelDecoder(inputStream, totalWrittenBits);
        List<Double> result = decompressor.getValues();
        assertEquals(original.length, result.size());
        for (int i = 0; i < original.length; i++) {
            // 允许很小的浮点误差，例如 1e-4
            assertEquals(original[i], result.get(i), 1e-4);
        }
    }

    @Test
    public void testRandomizedCompressDecompress() throws Exception {
        // 初始化随机数生成器
        Random random = new Random();
        int sampleSize = 100000;  // 恢复为100,000组测试数据
        double[] original = new double[sampleSize];

        // 生成随机测试数据
        for (int i = 0; i < sampleSize; i++) {
            // 随机整数部分：INT32_MIN >> 1 ~ INT32_MAX >> 1
            int intPart = random.nextInt(Integer.MAX_VALUE) - (Integer.MAX_VALUE >> 1);
            // 随机小数部分：0.0001 ~ 0.9999（保留四位小数）
            double decimalPart = 0.0001 + random.nextInt(9999) * 0.0001;
            // 组合为完整浮点数
            original[i] = intPart + decimalPart;
        }

        CamelEncoder compressor = new CamelEncoder();
        // 压缩所有数据点
        for (double v : original) {
            compressor.addValue(v);
        }
        long totalWrittenBits = compressor.close();
        ByteArrayOutputStream compressed = compressor.getByteArrayOutputStream();

        // 解压并比对
        InputStream inputStream = new ByteArrayInputStream(compressed.toByteArray());
        CamelDecoder decompressor = new CamelDecoder(inputStream, totalWrittenBits);
        List<Double> result = decompressor.getValues();

        assertEquals(original.length, result.size());
        for (int i = 0; i < original.length; i++) {
            // 允许浮点误差（1e-4）
            assertEquals(original[i], result.get(i), 1e-4);
        }
    }
}