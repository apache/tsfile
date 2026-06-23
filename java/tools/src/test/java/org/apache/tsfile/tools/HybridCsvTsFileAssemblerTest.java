/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HybridCsvTsFileAssemblerTest {

  private static final String TEST_DIR = "target" + File.separator + "hybridImportTest";

  @Before
  public void setUp() {
    new File(TEST_DIR).mkdirs();
  }

  @After
  public void tearDown() {
    deleteRecursive(new File(TEST_DIR));
  }

  @Test
  public void testHybridImport() throws Exception {
    String schemaPath = TEST_DIR + File.separator + "shared.schema";
    writeSchema(schemaPath);

    String mainCsv = TEST_DIR + File.separator + "main.csv";
    writeFile(
        mainCsv,
        "Region,DeviceId,Time,Temperature,Pressure\n"
            + "hebei,1,1000,80.0,1000.0\n"
            + "hebei,1,2000,81.0,1001.0\n");

    String sup1 = TEST_DIR + File.separator + "exp1.csv";
    writeFile(
        sup1,
        "Region,DeviceId,Temperature,Pressure\n" + "hebei,1,82.0,1002.0\n" + "hebei,1,83.0,1003.0\n");

    String sup2 = TEST_DIR + File.separator + "exp2.csv";
    writeFile(sup2, "Region,DeviceId,Temperature,Pressure\n" + "hebei,2,90.0,1100.0\n");

    String configPath = TEST_DIR + File.separator + "hybrid.conf";
    writeFile(
        configPath,
        "output_tsfile=" + TEST_DIR + File.separator + "combined.tsfile\n"
            + "shared_schema=" + schemaPath + "\n"
            + "main_csv=" + mainCsv + "\n"
            + "main_batch_id=main\n"
            + "supplement_csv=" + sup1 + "\n"
            + "supplement_batch_id=exp1\n"
            + "supplement_csv=" + sup2 + "\n"
            + "supplement_batch_id=exp2\n");

    HybridImportConfig config = HybridImportConfigParser.parse(configPath);
    HybridCsvTsFileAssembler.execute(config);

    File tsfile = new File(TEST_DIR, "combined.tsfile");
    assertTrue(tsfile.exists());

    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsfile.getAbsolutePath())) {
      Map<String, TableSchema> tableSchemas = reader.getTableSchemaMap();
      assertEquals(1, tableSchemas.size());
      assertTrue(tableSchemas.containsKey("lab"));

      List<IDeviceID> devices = reader.getAllDevices();
      // main: 1 device (hebei,1,main), exp1: 1 device, exp2: 1 device
      assertEquals(3, devices.size());

      int mainGroups = 0;
      int exp1Groups = 0;
      int exp2Groups = 0;
      for (IDeviceID device : devices) {
        String deviceStr = device.toString();
        if (deviceStr.contains("main")) {
          mainGroups++;
        } else if (deviceStr.contains("exp1")) {
          exp1Groups++;
        } else if (deviceStr.contains("exp2")) {
          exp2Groups++;
        }
      }
      assertEquals(1, mainGroups);
      assertEquals(1, exp1Groups);
      assertEquals(1, exp2Groups);
    }
  }

  @Test
  public void testSyntheticTabletBuilderTimestamps() throws Exception {
    ImportSchema schema = buildBaseSchema();
    schema = ImportSchemaUtils.withBatchIdTag(schema, "batch_id", "exp1");

    SyntheticTabletBuilder builder = new SyntheticTabletBuilder(schema, true);
    SourceBatch batch =
        SourceBatch.fromRows(
            java.util.Arrays.asList("Region", "DeviceId", "Temperature", "Pressure"),
            java.util.Arrays.asList(
                new Object[] {"hebei", "1", "80.0", "1000.0"},
                new Object[] {"hebei", "1", "81.0", "1001.0"}));

    org.apache.tsfile.write.record.Tablet tablet = builder.build(batch);
    assertEquals(2, tablet.getRowSize());
    assertEquals(1L, tablet.getTimestamps()[0]);
    assertEquals(2L, tablet.getTimestamps()[1]);
  }

  private static ImportSchema buildBaseSchema() {
    ImportSchema schema = new ImportSchema();
    schema.setTableName("lab");
    schema.setTimeColumnName("Time");
    schema.setTimePrecision("ms");
    schema.setHasHeader(true);
    java.util.List<ImportSchema.TagColumn> tags = new java.util.ArrayList<>();
    tags.add(new ImportSchema.TagColumn("Region"));
    tags.add(new ImportSchema.TagColumn("DeviceId"));
    schema.setTagColumns(tags);
    schema.setSourceColumns(
        java.util.Arrays.asList(
            new ImportSchema.SourceColumn("Region", TSDataType.TEXT),
            new ImportSchema.SourceColumn("DeviceId", TSDataType.TEXT),
            new ImportSchema.SourceColumn("Time", TSDataType.INT64),
            new ImportSchema.SourceColumn("Temperature", TSDataType.FLOAT),
            new ImportSchema.SourceColumn("Pressure", TSDataType.DOUBLE)));
    return schema;
  }

  private static void writeSchema(String path) throws IOException {
    writeFile(
        path,
        "table_name=lab\n"
            + "time_column=Time\n"
            + "has_header=true\n"
            + "separator=,\n"
            + "tag_columns\n"
            + "Region\n"
            + "DeviceId\n"
            + "source_columns\n"
            + "Region TEXT,\n"
            + "DeviceId TEXT,\n"
            + "Time INT64,\n"
            + "Temperature FLOAT,\n"
            + "Pressure DOUBLE,\n");
  }

  private static void writeFile(String path, String content) throws IOException {
    try (BufferedWriter w = new BufferedWriter(new FileWriter(path))) {
      w.write(content);
    }
  }

  private static void deleteRecursive(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteRecursive(f);
        }
        f.delete();
      }
    }
    dir.delete();
  }
}
