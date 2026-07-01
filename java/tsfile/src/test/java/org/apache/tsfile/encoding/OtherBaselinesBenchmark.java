/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionChimpDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionDecoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionChimpEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV2;

/**
 * Benchmark Gorilla ({@link DoublePrecisionEncoderV2}), Chimp ({@link DoublePrecisionChimpEncoder}),
 * and Zstd (same as {@link CompressionTestBenchmark#testZstd}) on selected Camel CSV datasets.
 *
 * <p>CSV columns match {@link CompressionTestBenchmark}: Encoding/Decoding Time columns hold
 * compress/decompress throughput in MiB/s; Compression Ratio is compressedSize/originalSize.
 */
public class OtherBaselinesBenchmark {

  /** Fig10 dataset list (must match fig10_vary_all_pack_size.py dataset_mapping keys). */
  private static final Set<String> TARGET_DATASETS = new LinkedHashSet<>(
      Arrays.asList(
          "PM10-dust.csv",
          "Stocks-UK.csv",
          "Food-price.csv",
          "Blockchain-tr.csv",
          "CS-Sensors.csv",
          "TY-Transport.csv",
          "USGS-Earthquakes.csv",
          "TH-Climate.csv",
          "Cyber-Vehicle.csv",
          "EPM-Education.csv"));

  private static final int WARMUP = 2;
  private static final int RUNS = 3;

  private static void assertDoublesEqual(double[] expected, double[] actual) {
    if (expected.length != actual.length) {
      throw new RuntimeException("length mismatch");
    }
    for (int i = 0; i < expected.length; i++) {
      if (Double.doubleToLongBits(expected[i]) != Double.doubleToLongBits(actual[i])) {
        if (Math.abs(expected[i] - actual[i]) > 1e-9 * (1 + Math.abs(expected[i]))) {
          throw new RuntimeException("mismatch at " + i + ": " + expected[i] + " vs " + actual[i]);
        }
      }
    }
  }

  public static CompressionTestBenchmark.CompressionResult testGorilla(double[] data)
      throws IOException {
    int originalSize = data.length * Double.BYTES;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DoublePrecisionEncoderV2 encoder = new DoublePrecisionEncoderV2();
    long t0 = System.nanoTime();
    for (double v : data) {
      encoder.encode(v, baos);
    }
    encoder.flush(baos);
    long compressNs = System.nanoTime() - t0;
    byte[] compressed = baos.toByteArray();

    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    DoublePrecisionDecoderV2 decoder = new DoublePrecisionDecoderV2();
    double[] decoded = new double[data.length];
    t0 = System.nanoTime();
    for (int i = 0; i < data.length; i++) {
      if (!decoder.hasNext(buffer)) {
        throw new RuntimeException("Gorilla decoder exhausted early at " + i);
      }
      decoded[i] = decoder.readDouble(buffer);
    }
    long decompressNs = System.nanoTime() - t0;
    assertDoublesEqual(data, decoded);

    return new CompressionTestBenchmark.CompressionResult(
        "Gorilla",
        compressNs,
        decompressNs,
        compressed.length,
        originalSize,
        data.length);
  }

  public static CompressionTestBenchmark.CompressionResult testChimp(double[] data)
      throws IOException {
    int originalSize = data.length * Double.BYTES;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DoublePrecisionChimpEncoder encoder = new DoublePrecisionChimpEncoder();
    long t0 = System.nanoTime();
    for (double v : data) {
      encoder.encode(v, baos);
    }
    encoder.flush(baos);
    long compressNs = System.nanoTime() - t0;
    byte[] compressed = baos.toByteArray();

    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    DoublePrecisionChimpDecoder decoder = new DoublePrecisionChimpDecoder();
    double[] decoded = new double[data.length];
    t0 = System.nanoTime();
    for (int i = 0; i < data.length; i++) {
      if (!decoder.hasNext(buffer)) {
        throw new RuntimeException("Chimp decoder exhausted early at " + i);
      }
      decoded[i] = decoder.readDouble(buffer);
    }
    long decompressNs = System.nanoTime() - t0;
    assertDoublesEqual(data, decoded);

    return new CompressionTestBenchmark.CompressionResult(
        "Chimp",
        compressNs,
        decompressNs,
        compressed.length,
        originalSize,
        data.length);
  }

  private static CompressionTestBenchmark.CompressionResult averageResults(
      List<CompressionTestBenchmark.CompressionResult> runs, String algorithm) {
    long sumEnc = 0;
    long sumDec = 0;
    long sumSize = 0;
    for (CompressionTestBenchmark.CompressionResult r : runs) {
      sumEnc += r.compressTimeNanos;
      sumDec += r.decompressTimeNanos;
      sumSize += r.compressedSize;
    }
    int n = runs.size();
    return new CompressionTestBenchmark.CompressionResult(
        algorithm,
        sumEnc / n,
        sumDec / n,
        (int) (sumSize / n),
        runs.get(0).originalSize,
        runs.get(0).numPoints);
  }

  private static void benchmarkAlgorithm(
      String name,
      ThrowingFunction<double[], CompressionTestBenchmark.CompressionResult> fn,
      double[] data,
      CsvWriter writer,
      String inputPath,
      File datasetFile)
      throws Exception {
    for (int i = 0; i < WARMUP; i++) {
      fn.apply(data);
    }
    List<CompressionTestBenchmark.CompressionResult> results = new ArrayList<>();
    for (int i = 0; i < RUNS; i++) {
      results.add(fn.apply(data));
    }
    CompressionTestBenchmark.CompressionResult avg = averageResults(results, name);

    BigDecimal encMBps =
        avg.getCompressThroughputMBps().setScale(15, RoundingMode.HALF_UP);
    BigDecimal decMBps =
        avg.getDecompressThroughputMBps().setScale(15, RoundingMode.HALF_UP);

    String[] record = {
      inputPath,
      avg.algorithm,
      encMBps.toPlainString(),
      decMBps.toPlainString(),
      String.valueOf(avg.numPoints),
      String.valueOf(avg.compressedSize),
      avg.getCompressionRatio().setScale(15, RoundingMode.HALF_UP).toPlainString()
    };
    writer.writeRecord(record);
    writer.flush();
    System.out.printf(
        "  %s %s: enc=%s MiB/s dec=%s MiB/s ratio=%s%n",
        datasetFile.getName(),
        name,
        encMBps.toPlainString(),
        decMBps.toPlainString(),
        avg.getCompressionRatio().toPlainString());
  }

  @FunctionalInterface
  private interface ThrowingFunction<T, R> {
    R apply(T t) throws Exception;
  }

  public static void main(String[] args) throws Exception {
    String dataDir =
        args.length > 0
            ? args[0]
            : "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
    String outDirStr =
        args.length > 1
            ? args[1]
            : "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_other_baseline";

    File outDir = new File(outDirStr);
    if (!outDir.exists() && !outDir.mkdirs()) {
      System.err.println("Failed to create " + outDirStr);
      return;
    }

    File dir = new File(dataDir);
    File[] files = dir.listFiles();
    if (files == null) {
      System.err.println("No files in " + dataDir);
      return;
    }

    for (File file : files) {
      if (file.isDirectory() || !TARGET_DATASETS.contains(file.getName())) {
        continue;
      }
      String inputPath = file.getAbsolutePath();
      System.out.println("Dataset: " + file.getName());

      List<Double> values = new ArrayList<>();
      CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
      try {
        while (csvReader.readRecord()) {
          for (String value : csvReader.getValues()) {
            String numStr = value.trim();
            if (!numStr.isEmpty()) {
              try {
                values.add(Double.parseDouble(numStr));
              } catch (NumberFormatException nfe) {
                System.err.println("skip: " + numStr);
              }
            }
          }
        }
      } finally {
        csvReader.close();
      }

      if (values.isEmpty()) {
        System.err.println("No data: " + file.getName());
        continue;
      }

      double[] allData = new double[values.size()];
      for (int i = 0; i < values.size(); i++) {
        allData[i] = values.get(i);
      }

      String outPath = outDirStr + "/" + file.getName();
      CsvWriter writer = new CsvWriter(outPath, ',', StandardCharsets.UTF_8);
      try {
        writer.writeRecord(
            new String[] {
              "Input Direction",
              "Encoding Algorithm",
              "Encoding Time",
              "Decoding Time",
              "Points",
              "Compressed Size",
              "Compression Ratio"
            });

        benchmarkAlgorithm(
            "Gorilla", OtherBaselinesBenchmark::testGorilla, allData, writer, inputPath, file);
        benchmarkAlgorithm(
            "Chimp", OtherBaselinesBenchmark::testChimp, allData, writer, inputPath, file);
        benchmarkAlgorithm(
            "Zstd",
            CompressionTestBenchmark::testZstd,
            allData,
            writer,
            inputPath,
            file);

      } finally {
        writer.close();
      }
    }

    System.out.println("Done. Output under " + outDirStr);
  }
}
