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

package org.apache.tsfile.file.metadata.evolution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TsFileSchemaRewriterTest {
  private static final String TEST_FILE_PATH = "target/test.tsfile";
  private File tsFile;

  @Before
  public void setUp() throws Exception {
    tsFile = new File(TEST_FILE_PATH);
    if (tsFile.exists()) {
      assertTrue(tsFile.delete());
    }

    // Create a test TsFile with some data
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      // Register a timeseries
      writer.registerTimeseries(
          "d1",
          new MeasurementSchema("s1", TSDataType.INT32));

      // Write some data points
      TSRecord record = new TSRecord("d1", 1L);
      record.addTuple(new IntDataPoint("s1", 123));
      writer.writeRecord(record);

      record = new TSRecord("d1", 2L);
      record.addTuple(new IntDataPoint("s1", 456));
      writer.writeRecord(record);
    }
  }

  @After
  public void tearDown() {
    if (tsFile != null && tsFile.exists()) {
      assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testAppendSingleProperty() throws IOException {
    // Create rewriter and append a single property
    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(TEST_FILE_PATH);
    List<Pair<String, String>> newProperties = new ArrayList<>();
    newProperties.add(new Pair<>("test_key", "test_value"));
    rewriter.appendProperties(newProperties);

    // Verify the property was added
    try (TsFileSequenceReader reader = new TsFileSequenceReader(TEST_FILE_PATH)) {
      TsFileMetadata metadata = reader.readFileMetadata();
      assertEquals("test_value", metadata.getTsFileProperties().get("test_key"));
    }
  }

  @Test
  public void testAppendMultipleProperties() throws IOException {
    // Create rewriter and append multiple properties
    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(TEST_FILE_PATH);
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("key1", "value1");
    newProperties.put("key2", "value2");
    newProperties.put("key3", "value3");
    rewriter.appendProperties(newProperties.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(
        Collectors.toList()));

    // Verify all properties were added
    try (TsFileSequenceReader reader = new TsFileSequenceReader(TEST_FILE_PATH)) {
      TsFileMetadata metadata = reader.readFileMetadata();
      Map<String, String> properties = metadata.getTsFileProperties();
      assertEquals("value1", properties.get("key1"));
      assertEquals("value2", properties.get("key2"));
      assertEquals("value3", properties.get("key3"));
    }
  }

  @Test
  public void testAppendPropertiesMultipleTimes() throws IOException {
    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(TEST_FILE_PATH);

    // First append
    Map<String, String> firstProperties = new HashMap<>();
    firstProperties.put("key1", "value1");
    rewriter.appendProperties(firstProperties.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(
        Collectors.toList()));

    // Second append
    Map<String, String> secondProperties = new HashMap<>();
    secondProperties.put("key2", "value2");
    rewriter.appendProperties(secondProperties.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(
        Collectors.toList()));

    // Third append with update to existing property
    Map<String, String> thirdProperties = new HashMap<>();
    thirdProperties.put("key1", "new_value1");
    thirdProperties.put("key3", "value3");
    rewriter.appendProperties(thirdProperties.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(
        Collectors.toList()));

    // Verify final state
    try (TsFileSequenceReader reader = new TsFileSequenceReader(TEST_FILE_PATH)) {
      TsFileMetadata metadata = reader.readFileMetadata();
      Map<String, String> properties = metadata.getTsFileProperties();
      assertEquals("new_value1", properties.get("key1")); // Updated value
      assertEquals("value2", properties.get("key2")); // Unchanged value
      assertEquals("value3", properties.get("key3")); // New value
    }
  }

  @Test
  public void testAppendEmptyProperties() throws IOException {
    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(TEST_FILE_PATH);
    rewriter.appendProperties(Collections.emptyList());

    // Verify file is still valid and only encryption-related properties were added
    try (TsFileSequenceReader reader = new TsFileSequenceReader(TEST_FILE_PATH)) {
      TsFileMetadata metadata = reader.readFileMetadata();
      assertEquals(3, metadata.getTsFileProperties().size());
      assertEquals("0", metadata.getTsFileProperties().get("encryptLevel"));
      assertEquals("org.apache.tsfile.encrypt.UNENCRYPTED", metadata.getTsFileProperties().get("encryptType"));
      assertEquals("", metadata.getTsFileProperties().get("encryptKey"));
    }
  }

  @Test(expected = IOException.class)
  public void testAppendPropertiesToNonExistentFile() throws IOException {
    String nonExistentFile = "non_existent.tsfile";
    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(nonExistentFile);
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    rewriter.appendProperties(properties.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(
        Collectors.toList()));
  }

  @Test
  public void testAppendLargeProperties() throws IOException {
    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(TEST_FILE_PATH);
    Map<String, String> largeProperties = new HashMap<>();

    // Add many properties with relatively large values
    for (int i = 0; i < 1000; i++) {
      StringBuilder value = new StringBuilder();
      for (int j = 0; j < 100; j++) {
        value.append("value").append(i).append("_").append(j);
      }
      largeProperties.put("key" + i, value.toString());
    }

    rewriter.appendProperties(largeProperties.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue())).collect(
        Collectors.toList()));

    // Verify all properties were written correctly
    try (TsFileSequenceReader reader = new TsFileSequenceReader(TEST_FILE_PATH)) {
      TsFileMetadata metadata = reader.readFileMetadata();
      Map<String, String> properties = metadata.getTsFileProperties();

      for (int i = 0; i < 1000; i++) {
        StringBuilder expectedValue = new StringBuilder();
        for (int j = 0; j < 100; j++) {
          expectedValue.append("value").append(i).append("_").append(j);
        }
        assertEquals(expectedValue.toString(), properties.get("key" + i));
      }
    }
  }
}
