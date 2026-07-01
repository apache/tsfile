package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.xerial.snappy.Snappy;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import com.github.luben.zstd.Zstd;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;
import org.tukaani.xz.XZInputStream;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompressionTestBenchmark {

    // LZ4 压缩器工厂
    private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor lz4Compressor = lz4Factory.fastCompressor();
    private static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

    // double[] -> byte[]
    public static byte[] doubleArrayToByteArray(double[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(data.length * Double.BYTES);
        for (double v : data) {
            buffer.putDouble(v);
        }
        return buffer.array();
    }

    // byte[] -> double[] （调试/需要时可用）
    public static double[] byteArrayToDoubleArray(byte[] data, int originalLength) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        double[] result = new double[originalLength];
        for (int i = 0; i < originalLength; i++) {
            result[i] = buffer.getDouble();
        }
        return result;
    }

    // LZ4：在输出中写入原始字节长度和压缩字节长度
    public static byte[] compressWithLZ4(byte[] data) throws IOException {
        int maxCompressedLength = lz4Compressor.maxCompressedLength(data.length);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // 写入原始数据长度（bytes）
        dos.writeInt(data.length);

        // 压缩数据到临时数组
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = lz4Compressor.compress(data, 0, data.length, compressed, 0, maxCompressedLength);

        // 写入压缩数据长度和压缩数据
        dos.writeInt(compressedLength);
        dos.write(compressed, 0, compressedLength);

        dos.close();
        return baos.toByteArray();
    }

    // LZ4 解压：从头读取原始字节长度并解压
    public static byte[] decompressWithLZ4(byte[] compressedData) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
        DataInputStream dis = new DataInputStream(bais);

        int originalLength = dis.readInt();      // bytes
        int compressedLength = dis.readInt();    // bytes

        byte[] compressed = new byte[compressedLength];
        dis.readFully(compressed);

        byte[] decompressed = new byte[originalLength];
        try {
            lz4Decompressor.decompress(compressed, 0, decompressed, 0, originalLength);
        } catch (Exception e) {
            throw new IOException("LZ4 decompression failed: " + e.getMessage(), e);
        }
        return decompressed;
    }

    // 测试 Snappy（接受 double[]）
    public static CompressionResult testSnappy(double[] data) throws IOException {
        byte[] originalBytes = doubleArrayToByteArray(data);

        long startTime = System.nanoTime();
        byte[] compressed = Snappy.compress(originalBytes);
        long compressTime = System.nanoTime() - startTime;

        startTime = System.nanoTime();
        byte[] decompressed = Snappy.uncompress(compressed);
        long decompressTime = System.nanoTime() - startTime;

        if (!Arrays.equals(originalBytes, decompressed)) {
            throw new RuntimeException("Snappy decompression verification failed");
        }

        return new CompressionResult(
                "Snappy",
                compressTime,
                decompressTime,
                compressed.length,
                originalBytes.length,
                data.length
        );
    }

    // 测试 LZ4（接受 double[]）
    public static CompressionResult testLZ4(double[] data) throws IOException {
        byte[] originalBytes = doubleArrayToByteArray(data);

        long startTime = System.nanoTime();
        byte[] compressed = compressWithLZ4(originalBytes);
        long compressTime = System.nanoTime() - startTime;

        startTime = System.nanoTime();
        byte[] decompressed = decompressWithLZ4(compressed);
        long decompressTime = System.nanoTime() - startTime;

        if (!Arrays.equals(originalBytes, decompressed)) {
            throw new RuntimeException("LZ4 decompression verification failed");
        }

        return new CompressionResult(
                "LZ4",
                compressTime,
                decompressTime,
                compressed.length,
                originalBytes.length,
                data.length
        );
    }

    // 测试 Zstd（接受 double[]）
    public static CompressionResult testZstd(double[] data) throws IOException {
        byte[] originalBytes = doubleArrayToByteArray(data);

        long startTime = System.nanoTime();
        byte[] compressed = Zstd.compress(originalBytes);
        long compressTime = System.nanoTime() - startTime;

        startTime = System.nanoTime();
        byte[] decompressed = Zstd.decompress(compressed, originalBytes.length);
        long decompressTime = System.nanoTime() - startTime;

        if (!Arrays.equals(originalBytes, decompressed)) {
            throw new RuntimeException("Zstd decompression verification failed");
        }

        return new CompressionResult(
                "Zstd",
                compressTime,
                decompressTime,
                compressed.length,
                originalBytes.length,
                data.length
        );
    }

    // 测试 GZIP（接受 double[]）
    public static CompressionResult testGzip(double[] data) throws IOException {
        byte[] originalBytes = doubleArrayToByteArray(data);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        java.util.zip.GZIPOutputStream gzipOutputStream = new java.util.zip.GZIPOutputStream(baos);

        long startTime = System.nanoTime();
        gzipOutputStream.write(originalBytes);
        gzipOutputStream.close();
        long compressTime = System.nanoTime() - startTime;

        byte[] compressed = baos.toByteArray();

        // 解压缩
        ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        java.util.zip.GZIPInputStream gzipInputStream = new java.util.zip.GZIPInputStream(bais);
        ByteArrayOutputStream decompressedBaos = new ByteArrayOutputStream();

        startTime = System.nanoTime();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = gzipInputStream.read(buffer)) != -1) {
            decompressedBaos.write(buffer, 0, len);
        }
        gzipInputStream.close();
        long decompressTime = System.nanoTime() - startTime;

        byte[] decompressed = decompressedBaos.toByteArray();

        if (!Arrays.equals(originalBytes, decompressed)) {
            throw new RuntimeException("GZIP decompression verification failed");
        }

        return new CompressionResult(
                "GZIP",
                compressTime,
                decompressTime,
                compressed.length,
                originalBytes.length,
                data.length
        );
    }

    // 测试 XZ（接受 double[]）
    public static CompressionResult testXZ(double[] data) throws IOException {
        byte[] originalBytes = doubleArrayToByteArray(data);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LZMA2Options options = new LZMA2Options();
        XZOutputStream xzOutputStream = new XZOutputStream(baos, options);

        long startTime = System.nanoTime();
        xzOutputStream.write(originalBytes);
        xzOutputStream.close();
        long compressTime = System.nanoTime() - startTime;

        byte[] compressed = baos.toByteArray();

        // 解压缩
        ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        XZInputStream xzInputStream = new XZInputStream(bais);
        ByteArrayOutputStream decompressedBaos = new ByteArrayOutputStream();

        startTime = System.nanoTime();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = xzInputStream.read(buffer)) != -1) {
            decompressedBaos.write(buffer, 0, len);
        }
        xzInputStream.close();
        long decompressTime = System.nanoTime() - startTime;

        byte[] decompressed = decompressedBaos.toByteArray();

        if (!Arrays.equals(originalBytes, decompressed)) {
            throw new RuntimeException("XZ decompression verification failed");
        }

        return new CompressionResult(
                "XZ",
                compressTime,
                decompressTime,
                compressed.length,
                originalBytes.length,
                data.length
        );
    }

    // 压缩结果数据类
    static class CompressionResult {
        String algorithm;
        long compressTimeNanos;
        long decompressTimeNanos;
        int compressedSize; // bytes
        int originalSize;   // bytes
        int numPoints;      // 元素个数

        CompressionResult(String algorithm, long compressTime, long decompressTime,
                          int compressedSize, int originalSize, int numPoints) {
            this.algorithm = algorithm;
            this.compressTimeNanos = compressTime;
            this.decompressTimeNanos = decompressTime;
            this.compressedSize = compressedSize;
            this.originalSize = originalSize;
            this.numPoints = numPoints;
        }

        BigDecimal getCompressionRatio() {
            if (originalSize == 0) return BigDecimal.ZERO;
            return BigDecimal.valueOf(compressedSize)
                    .divide(BigDecimal.valueOf(originalSize), 10, RoundingMode.HALF_UP);
        }

        // 吞吐使用 MiB/s = 1024*1024 bytes
        BigDecimal getCompressThroughputMBps() {
            double seconds = compressTimeNanos / 1_000_000_000.0;
            if (seconds == 0) return BigDecimal.ZERO;
            double mib = ((double) originalSize) / (1024.0 * 1024.0);
            return BigDecimal.valueOf(mib / seconds);
        }

        BigDecimal getDecompressThroughputMBps() {
            double seconds = decompressTimeNanos / 1_000_000_000.0;
            if (seconds == 0) return BigDecimal.ZERO;
            double mib = ((double) originalSize) / (1024.0 * 1024.0);
            return BigDecimal.valueOf(mib / seconds);
        }
    }

    // 主函数：对目录中每个文件做 benchmark
    public static void main(String[] args) {
        System.out.println("\nCompression Algorithms Benchmarking...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_Compression";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) {
            boolean created = outputDir.mkdirs();
            if (!created) {
                System.err.println("Failed to create output directory: " + outputDirstr);
                return;
            }
        }

        File dir = new File(directory);
        File[] files = dir.listFiles();
        if (files == null) {
            System.err.println("No files found in directory: " + directory);
            return;
        }

        for (File file : files) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("\nProcessing file: " + file.getName());
            String outputPath = outputDirstr + "/" + file.getName() ;

            CsvWriter writer = null;
            try {
                writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);

                // CSV 表头（按你的要求，列名不变）
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

                // 读取原始数值字符串并解析为 double
                List<Double> values = new ArrayList<>();

                CsvReader csvReader = null;
                try {
                    csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
                    while (csvReader.readRecord()) {
                        for (String value : csvReader.getValues()) {
                            String numStr = value.trim();
                            if (!numStr.isEmpty()) {
                                try {
                                    double d = Double.parseDouble(numStr);
                                    values.add(d);
                                } catch (NumberFormatException nfe) {
                                    System.err.println("Warning: cannot parse to double: '" + numStr + "' in file " + file.getName());
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error reading CSV file " + file.getName() + ": " + e.getMessage());
                    continue;
                } finally {
                    if (csvReader != null) csvReader.close();
                }

                if (values.isEmpty()) {
                    System.out.println("No numeric data found in file: " + file.getName());
                    continue;
                }

                // 转为 double[]
                double[] allData = new double[values.size()];
                for (int i = 0; i < values.size(); i++) allData[i] = values.get(i);

                System.out.println("Total data points: " + allData.length);
                long originalBytesLen = (long) allData.length * Double.BYTES;
                System.out.println("Original data size: " + originalBytesLen + " bytes");

                // 预热与多次测试
                int warmupIterations = 0;
                int testIterations = 1;
                List<CompressionResult> allResults = new ArrayList<>();

                List<String> algorithms = Arrays.asList("Snappy", "LZ4", "Zstd", "GZIP", "XZ");

                for (String algorithm : algorithms) {
                    System.out.println("Testing " + algorithm + "...");

                    // 预热
                    for (int i = 0; i < warmupIterations; i++) {
                        try {
                            switch (algorithm) {
                                case "Snappy": testSnappy(allData); break;
                                case "LZ4": testLZ4(allData); break;
                                case "Zstd": testZstd(allData); break;
                                case "GZIP": testGzip(allData); break;
                                case "XZ": testXZ(allData); break;
                            }
                        } catch (Exception e) {
                            System.err.println("Warning: " + algorithm + " warmup failed: " + e.getMessage());
                        }
                    }

                    // 实际测试多次取平均
                    List<CompressionResult> iterationResults = new ArrayList<>();
                    for (int i = 0; i < testIterations; i++) {
                        try {
                            CompressionResult result = null;
                            switch (algorithm) {
                                case "Snappy": result = testSnappy(allData); break;
                                case "LZ4": result = testLZ4(allData); break;
                                case "Zstd": result = testZstd(allData); break;
                                case "GZIP": result = testGzip(allData); break;
                                case "XZ": result = testXZ(allData); break;
                            }
                            if (result != null) iterationResults.add(result);
                        } catch (Exception e) {
                            System.err.println("Error testing " + algorithm + " iteration " + i + ": " + e.getMessage());
                        }
                    }

                    if (!iterationResults.isEmpty()) {
                        long totalCompressTime = 0;
                        long totalDecompressTime = 0;
                        long totalCompressedSize = 0;

                        for (CompressionResult r : iterationResults) {
                            totalCompressTime += r.compressTimeNanos;
                            totalDecompressTime += r.decompressTimeNanos;
                            totalCompressedSize += r.compressedSize;
                        }

                        long avgCompressTime = totalCompressTime / iterationResults.size();
                        long avgDecompressTime = totalDecompressTime / iterationResults.size();
                        int avgCompressedSize = (int) (totalCompressedSize / iterationResults.size());

                        CompressionResult avgResult = new CompressionResult(
                                iterationResults.get(0).algorithm,
                                avgCompressTime,
                                avgDecompressTime,
                                avgCompressedSize,
                                iterationResults.get(0).originalSize,
                                iterationResults.get(0).numPoints
                        );
                        allResults.add(avgResult);

                        // 写 CSV：Encoding Time / Decoding Time 改为吞吐 MB/s（MiB/s），但列名保持不变
                        BigDecimal encMBps = avgResult.getCompressThroughputMBps().setScale(2, RoundingMode.HALF_UP);
                        BigDecimal decMBps = avgResult.getDecompressThroughputMBps().setScale(2, RoundingMode.HALF_UP);

                        String[] record = {
                                file.getName(),
                                avgResult.algorithm,
                                encMBps.toString(),
                                decMBps.toString(),
                                String.valueOf(avgResult.numPoints),
                                String.valueOf(avgResult.compressedSize),
                                avgResult.getCompressionRatio().setScale(3, RoundingMode.HALF_UP).toString()
                        };
                        writer.writeRecord(record);
                        writer.flush();

                        System.out.printf("  %s: Encode(MiB/s)=%s, Decode(MiB/s)=%s, Ratio=%.3f, Compressed=%d bytes\n",
                                avgResult.algorithm,
                                encMBps.toString(),
                                decMBps.toString(),
                                avgResult.getCompressionRatio().doubleValue(),
                                avgResult.compressedSize);
                    } else {
                        System.err.println("No valid results for algorithm: " + algorithm);
                        String[] record = {
                                file.getName(),
                                algorithm,
                                "0.00",
                                "0.00",
                                "0",
                                "0",
                                "0.000"
                        };
                        writer.writeRecord(record);
                    }
                }

                // 控制台输出汇总
                System.out.println("\nSummary for " + file.getName() + ":");
                System.out.println("Algorithm\tEncode(MiB/s)\tDecode(MiB/s)\tPoints\tRatio\tCompressedSize");
                for (CompressionResult r : allResults) {
                    System.out.printf("%-10s\t%-14.2f\t%-14.2f\t%-6d\t%-6.3f\t%d bytes\n",
                            r.algorithm,
                            r.getCompressThroughputMBps().doubleValue(),
                            r.getDecompressThroughputMBps().doubleValue(),
                            r.numPoints,
                            r.getCompressionRatio().doubleValue(),
                            r.compressedSize);
                }
                System.out.println("========");

            } catch (Exception e) {
                System.err.println("Error processing file " + file.getName() + ": " + e.getMessage());
                e.printStackTrace();
            } finally {
                if (writer != null) writer.close();
            }
        }

        System.out.println("\nBenchmark completed!");
    }

}
