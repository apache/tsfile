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
package org.apache.tsfile.tools;

import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ImportExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportExecutor.class);

  private final ImportSchema importSchema;
  private final TimeConverter timeConverter;
  private final TabletBuilder tabletBuilder;

  public ImportExecutor(ImportSchema importSchema) {
    this.importSchema = importSchema;
    this.timeConverter = new TimeConverter(importSchema.getTimePrecision());
    this.tabletBuilder = new TabletBuilder(importSchema, timeConverter);
  }

  public boolean execute(SourceReader reader, String outputDir, String sourceBaseName) {
    try {
      Files.createDirectories(Paths.get(outputDir));
    } catch (IOException e) {
      LOGGER.error("Failed to create output directory: " + outputDir, e);
      return false;
    }

    int chunkIndex = 0;
    boolean hasData = false;
    boolean allSuccess = true;

    SourceBatch batch;
    while ((batch = reader.readBatch()) != null) {
      if (batch.isEmpty()) {
        continue;
      }
      hasData = true;
      chunkIndex++;
      String tsFileName = buildOutputFileName(sourceBaseName, chunkIndex);
      boolean ok = writeTsFile(batch, outputDir, tsFileName);
      if (!ok) {
        allSuccess = false;
        LOGGER.error("Failed to write chunk " + chunkIndex + " to " + tsFileName);
      }
    }

    if (!hasData) {
      LOGGER.warn("No data read from source: " + sourceBaseName);
    }

    if (chunkIndex == 1) {
      String singleName = sourceBaseName + ".tsfile";
      File indexed = new File(outputDir, buildOutputFileName(sourceBaseName, 1));
      File single = new File(outputDir, singleName);
      if (indexed.exists() && !single.exists()) {
        if (!indexed.renameTo(single)) {
          LOGGER.warn("Failed to rename " + indexed.getName() + " to " + singleName);
        }
      }
    }

    return allSuccess;
  }

  private boolean writeTsFile(SourceBatch batch, String outputDir, String fileName) {
    File tsFile = new File(outputDir, fileName);
    TsFileWriter writer = null;
    boolean success = false;
    try {
      writer = new TsFileWriter(tsFile);
      writer.setGenerateTableSchema(true);
      writer.registerTableSchema(tabletBuilder.getTableSchema());
      Tablet tablet = tabletBuilder.build(batch);
      writer.writeTable(tablet);
      success = true;
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to write file: " + tsFile.getAbsolutePath(), e);
      return false;
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close file: " + tsFile.getAbsolutePath(), e);
        }
      }
      if (!success) {
        deleteFile(tsFile);
      }
    }
  }

  private static String buildOutputFileName(String baseName, int chunkIndex) {
    return baseName + "_" + chunkIndex + ".tsfile";
  }

  private static void deleteFile(File file) {
    if (file.exists() && !file.delete()) {
      LOGGER.warn("Failed to delete: " + file.getAbsolutePath());
    }
  }
}
