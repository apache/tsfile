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

import org.apache.tsfile.i18n.Messages;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Parses a hybrid import config file (key=value lines). Example:
 *
 * <pre>
 * output_tsfile=combined.tsfile
 * shared_schema=main.schema
 * main_csv=timeseries.csv
 * main_batch_id=main
 * batch_id_tag=batch_id
 * validate_uniform_tags=true
 * supplement_csv=exp1.csv
 * supplement_batch_id=exp1
 * supplement_csv=exp2.csv
 * supplement_batch_id=exp2
 * </pre>
 */
public class HybridImportConfigParser {

  private HybridImportConfigParser() {}

  public static HybridImportConfig parse(String filePath) throws IOException {
    HybridImportConfig config = new HybridImportConfig();
    String pendingSupplementCsv = null;

    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#") || line.startsWith("//")) {
          continue;
        }
        if (line.startsWith("output_tsfile=")) {
          config.setOutputTsfile(extractValue(line));
        } else if (line.startsWith("shared_schema=")) {
          config.setSharedSchemaPath(extractValue(line));
        } else if (line.startsWith("main_csv=")) {
          config.setMainCsvPath(extractValue(line));
        } else if (line.startsWith("main_batch_id=")) {
          config.setMainBatchId(extractValue(line));
        } else if (line.startsWith("batch_id_tag=")) {
          config.setBatchIdTag(extractValue(line));
        } else if (line.startsWith("validate_uniform_tags=")) {
          config.setValidateUniformTags(Boolean.parseBoolean(extractValue(line)));
        } else if (line.startsWith("supplement_sort_by_variance=")) {
          config.setSupplementSortByVariance(Boolean.parseBoolean(extractValue(line)));
        } else if (line.startsWith("read_chunk_size=")) {
          config.setReadChunkSizeBytes(TsFileTool.parseBlockSize(extractValue(line)));
        } else if (line.startsWith("supplement_csv=")) {
          pendingSupplementCsv = extractValue(line);
        } else if (line.startsWith("supplement_batch_id=")) {
          if (pendingSupplementCsv == null) {
            throw new IllegalArgumentException(
                Messages.get("error.tools.hybrid_supplement_batch_id_order"));
          }
          config.addSupplement(pendingSupplementCsv, extractValue(line));
          pendingSupplementCsv = null;
        } else {
          throw new IllegalArgumentException(
              Messages.format("error.tools.hybrid_unknown_config_line", line));
        }
      }
    }

    if (pendingSupplementCsv != null) {
      throw new IllegalArgumentException(
          Messages.format("error.tools.hybrid_supplement_csv_without_batch_id", pendingSupplementCsv));
    }
    validate(config);
    return config;
  }

  private static void validate(HybridImportConfig config) {
    if (config.getOutputTsfile() == null || config.getOutputTsfile().isEmpty()) {
      throw new IllegalArgumentException(Messages.get("error.tools.hybrid_output_tsfile_required"));
    }
    if (config.getSharedSchemaPath() == null || config.getSharedSchemaPath().isEmpty()) {
      throw new IllegalArgumentException(Messages.get("error.tools.hybrid_shared_schema_required"));
    }
    if (config.getMainCsvPath() == null || config.getMainCsvPath().isEmpty()) {
      throw new IllegalArgumentException(Messages.get("error.tools.hybrid_main_csv_required"));
    }
    if (!config.getMainCsvFile().exists()) {
      throw new IllegalArgumentException(
          Messages.format("error.tools.hybrid_main_csv_not_found", config.getMainCsvPath()));
    }
    if (!new java.io.File(config.getSharedSchemaPath()).exists()) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.tools.hybrid_shared_schema_not_found", config.getSharedSchemaPath()));
    }
    for (HybridImportConfig.SupplementEntry entry : config.getSupplements()) {
      if (!entry.getCsvFile().exists()) {
        throw new IllegalArgumentException(
            Messages.format("error.tools.hybrid_supplement_csv_not_found", entry.getCsvPath()));
      }
    }
  }

  private static String extractValue(String line) {
    int idx = line.indexOf('=');
    if (idx < 0 || idx == line.length() - 1) {
      throw new IllegalArgumentException(
          Messages.format("error.tools.hybrid_invalid_config_line", line));
    }
    return line.substring(idx + 1).trim();
  }
}
