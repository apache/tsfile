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

package org.apache.tsfile.interop;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Generates TsFile test files for C# interoperability testing. */
public class TsFileInteropGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileInteropGenerator.class);
  private static final String OUTPUT_DIR = "/tmp/interop-test-files";
  private static final String DEVICE = "root.test.d0";
  private static final String SENSOR = "s0";
  private static final int VALUE_COUNT = 100;

  public static void main(String[] args) {
    try {
      File outputDir = new File(OUTPUT_DIR);
      if (outputDir.exists()) {
        deleteDirectory(outputDir);
      }
      outputDir.mkdirs();
      LOGGER.info("Created output directory: {}", OUTPUT_DIR);

      List<TestFileMetadata> allMetadata = new ArrayList<>();

      // Generate test files for different combinations
      generateTestFiles(allMetadata);

      // Write metadata to JSON
      writeMetadataJson(allMetadata);

      LOGGER.info("Successfully generated {} test files", allMetadata.size());
    } catch (Exception e) {
      LOGGER.error("Error generating test files", e);
      System.exit(1);
    }
  }

  private static void generateTestFiles(List<TestFileMetadata> allMetadata) throws Exception {
    // Define test configurations
    TSDataType[] dataTypes = {
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.BOOLEAN,
      TSDataType.TEXT
    };

    String[] patterns = {"sequential", "repeated", "alternating"};

    // Test each data type with compatible encodings
    for (TSDataType dataType : dataTypes) {
      for (TSEncoding encoding : getCompatibleEncodings(dataType)) {
        for (CompressionType compression : getTestCompressions()) {
          for (String pattern : patterns) {
            try {
              generateTestFile(dataType, encoding, compression, pattern, allMetadata);
            } catch (Exception e) {
              LOGGER.error(
                  "Failed to generate file for {}/{}/{}/{}",
                  dataType,
                  encoding,
                  compression,
                  pattern,
                  e);
            }
          }
        }
      }
    }
  }

  private static void generateTestFile(
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compression,
      String pattern,
      List<TestFileMetadata> allMetadata)
      throws Exception {

    String fileName =
        String.format(
            "%s_%s_%s_%s.tsfile",
            dataType.name().toLowerCase(),
            encoding.name().toLowerCase(),
            compression.name().toLowerCase(),
            pattern);

    File file = new File(OUTPUT_DIR, fileName);
    LOGGER.info("Generating test file: {}", fileName);

    List<Object> expectedValues = new ArrayList<>();

    try (TsFileWriter writer = new TsFileWriter(file)) {
      // Create schema
      IMeasurementSchema schema = new MeasurementSchema(SENSOR, dataType, encoding, compression);

      writer.registerTimeseries(new Path(DEVICE), Arrays.asList(schema));

      // Write data
      Tablet tablet = new Tablet(DEVICE, Arrays.asList(schema));

      for (int i = 0; i < VALUE_COUNT; i++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, i);

        Object value = generateValue(dataType, pattern, i);
        expectedValues.add(value);

        switch (dataType) {
          case INT32:
            tablet.addValue(SENSOR, row, (int) value);
            break;
          case INT64:
            tablet.addValue(SENSOR, row, (long) value);
            break;
          case FLOAT:
            tablet.addValue(SENSOR, row, (float) value);
            break;
          case DOUBLE:
            tablet.addValue(SENSOR, row, (double) value);
            break;
          case BOOLEAN:
            tablet.addValue(SENSOR, row, (boolean) value);
            break;
          case TEXT:
            tablet.addValue(SENSOR, row, new Binary(((String) value).getBytes()));
            break;
        }

        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.writeTree(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() > 0) {
        writer.writeTree(tablet);
        tablet.reset();
      }
    }

    // Verify the file can be read
    verifyFile(file, dataType, expectedValues);

    // Store metadata
    TestFileMetadata metadata =
        new TestFileMetadata(
            fileName,
            dataType.name(),
            encoding.name(),
            compression.name(),
            pattern,
            VALUE_COUNT,
            expectedValues);
    allMetadata.add(metadata);
  }

  private static void verifyFile(File file, TSDataType dataType, List<Object> expectedValues)
      throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath());
        TsFileReader tsFileReader = new TsFileReader(reader)) {

      Path path = new Path(DEVICE, SENSOR, true);
      QueryExpression queryExpression = QueryExpression.create(Arrays.asList(path), null);
      QueryDataSet dataSet = tsFileReader.query(queryExpression);

      int index = 0;
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        Object actualValue =
            convertFieldValue(record.getFields().get(0).getObjectValue(dataType), dataType);
        Object expectedValue = expectedValues.get(index);

        if (!valuesEqual(actualValue, expectedValue, dataType)) {
          throw new IOException(
              String.format(
                  "Verification failed at index %d: expected %s but got %s",
                  index, expectedValue, actualValue));
        }
        index++;
      }

      if (index != expectedValues.size()) {
        throw new IOException(
            String.format(
                "Verification failed: expected %d values but read %d",
                expectedValues.size(), index));
      }
    }
  }

  private static Object convertFieldValue(Object value, TSDataType dataType) {
    if (value instanceof Binary) {
      return new String(((Binary) value).getValues());
    }
    return value;
  }

  private static boolean valuesEqual(Object actual, Object expected, TSDataType dataType) {
    if (dataType == TSDataType.FLOAT) {
      return Math.abs((Float) actual - (Float) expected) < 1e-6;
    } else if (dataType == TSDataType.DOUBLE) {
      return Math.abs((Double) actual - (Double) expected) < 1e-9;
    } else {
      return actual.equals(expected);
    }
  }

  private static Object generateValue(TSDataType dataType, String pattern, int index) {
    switch (pattern) {
      case "sequential":
        return generateSequentialValue(dataType, index);
      case "repeated":
        return generateRepeatedValue(dataType, index);
      case "alternating":
        return generateAlternatingValue(dataType, index);
      default:
        throw new IllegalArgumentException("Unknown pattern: " + pattern);
    }
  }

  private static Object generateSequentialValue(TSDataType dataType, int index) {
    switch (dataType) {
      case INT32:
        return index;
      case INT64:
        return (long) index;
      case FLOAT:
        return (float) index;
      case DOUBLE:
        return (double) index;
      case BOOLEAN:
        return index % 2 == 0;
      case TEXT:
        return "value_" + index;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private static Object generateRepeatedValue(TSDataType dataType, int index) {
    int groupSize = 10;
    int groupValue = index / groupSize;
    switch (dataType) {
      case INT32:
        return groupValue;
      case INT64:
        return (long) groupValue;
      case FLOAT:
        return (float) groupValue;
      case DOUBLE:
        return (double) groupValue;
      case BOOLEAN:
        return groupValue % 2 == 0;
      case TEXT:
        return "value_" + groupValue;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private static Object generateAlternatingValue(TSDataType dataType, int index) {
    switch (dataType) {
      case INT32:
        return index % 2 == 0 ? 100 : 200;
      case INT64:
        return index % 2 == 0 ? 100L : 200L;
      case FLOAT:
        return index % 2 == 0 ? 100.0f : 200.0f;
      case DOUBLE:
        return index % 2 == 0 ? 100.0 : 200.0;
      case BOOLEAN:
        return index % 2 == 0;
      case TEXT:
        return index % 2 == 0 ? "valueA" : "valueB";
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private static List<TSEncoding> getCompatibleEncodings(TSDataType dataType) {
    List<TSEncoding> encodings = new ArrayList<>();

    switch (dataType) {
      case BOOLEAN:
        encodings.add(TSEncoding.PLAIN);
        encodings.add(TSEncoding.RLE);
        break;

      case INT32:
      case INT64:
        encodings.add(TSEncoding.PLAIN);
        encodings.add(TSEncoding.RLE);
        encodings.add(TSEncoding.TS_2DIFF);
        encodings.add(TSEncoding.GORILLA);
        encodings.add(TSEncoding.ZIGZAG);
        break;

      case FLOAT:
      case DOUBLE:
        encodings.add(TSEncoding.PLAIN);
        encodings.add(TSEncoding.RLE);
        encodings.add(TSEncoding.TS_2DIFF);
        encodings.add(TSEncoding.GORILLA_V1);
        encodings.add(TSEncoding.GORILLA);
        break;

      case TEXT:
        encodings.add(TSEncoding.PLAIN);
        encodings.add(TSEncoding.DICTIONARY);
        break;

      default:
        encodings.add(TSEncoding.PLAIN);
    }

    return encodings;
  }

  private static CompressionType[] getTestCompressions() {
    return new CompressionType[] {
      CompressionType.UNCOMPRESSED,
      CompressionType.GZIP,
      CompressionType.LZ4,
      CompressionType.SNAPPY,
      CompressionType.ZSTD
    };
  }

  private static void writeMetadataJson(List<TestFileMetadata> metadata) throws IOException {
    File jsonFile = new File(OUTPUT_DIR, "test-metadata.json");
    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    try (FileWriter writer = new FileWriter(jsonFile)) {
      gson.toJson(metadata, writer);
    }

    LOGGER.info("Wrote metadata to {}", jsonFile.getAbsolutePath());
  }

  private static void deleteDirectory(File directory) throws IOException {
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          Files.delete(file.toPath());
        }
      }
    }
    Files.delete(directory.toPath());
  }
}
