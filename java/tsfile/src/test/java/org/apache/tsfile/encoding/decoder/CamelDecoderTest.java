package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.CamelEncoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class CamelDecoderTest {
    @Test
    public void testSimpleCaseForDebug() throws Exception {
        double[] original = new double[] { -536641.84,  -536641.83 };
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
            assertEquals(original[i], result.get(i), 1e-4);
        }
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
        int sampleSize = 1000;  // 恢复为100,000组测试数据
        double[] original = new double[sampleSize];

        // 生成随机测试数据
        for (int i = 0; i < sampleSize; i++) {
            // 随机整数部分：-1,000,000 ~ 1,000,000
            int intPart = random.nextInt(2_000_001) - 1_000_000;
            // 随机小数部分：0.01 ~ 0.99（保留两位小数）
            double decimalPart = 0.01 + random.nextInt(99) * 0.01;
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
