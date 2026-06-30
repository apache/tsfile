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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Configuration for hybrid CSV import (main time-series CSV + supplement CSVs). */
public class HybridImportConfig {

  private String outputTsfile;
  private String sharedSchemaPath;
  private String mainCsvPath;
  private String mainBatchId = "main";
  private String batchIdTag = ImportSchemaUtils.DEFAULT_BATCH_ID_TAG;
  private boolean validateUniformTags = true;
  /** Sort supplement rows by FIELD variance priority (ascending multi-key) before write. */
  private boolean supplementSortByVariance = true;
  private long readChunkSizeBytes = 256L * 1024 * 1024;

  private final List<SupplementEntry> supplements = new ArrayList<>();

  public static class SupplementEntry {
    private final String csvPath;
    private final String batchId;

    public SupplementEntry(String csvPath, String batchId) {
      this.csvPath = csvPath;
      this.batchId = batchId;
    }

    public String getCsvPath() {
      return csvPath;
    }

    public String getBatchId() {
      return batchId;
    }

    public File getCsvFile() {
      return new File(csvPath);
    }
  }

  public String getOutputTsfile() {
    return outputTsfile;
  }

  public void setOutputTsfile(String outputTsfile) {
    this.outputTsfile = outputTsfile;
  }

  public String getSharedSchemaPath() {
    return sharedSchemaPath;
  }

  public void setSharedSchemaPath(String sharedSchemaPath) {
    this.sharedSchemaPath = sharedSchemaPath;
  }

  public String getMainCsvPath() {
    return mainCsvPath;
  }

  public void setMainCsvPath(String mainCsvPath) {
    this.mainCsvPath = mainCsvPath;
  }

  public String getMainBatchId() {
    return mainBatchId;
  }

  public void setMainBatchId(String mainBatchId) {
    this.mainBatchId = mainBatchId;
  }

  public String getBatchIdTag() {
    return batchIdTag;
  }

  public void setBatchIdTag(String batchIdTag) {
    this.batchIdTag = batchIdTag;
  }

  public boolean isValidateUniformTags() {
    return validateUniformTags;
  }

  public void setValidateUniformTags(boolean validateUniformTags) {
    this.validateUniformTags = validateUniformTags;
  }

  public boolean isSupplementSortByVariance() {
    return supplementSortByVariance;
  }

  public void setSupplementSortByVariance(boolean supplementSortByVariance) {
    this.supplementSortByVariance = supplementSortByVariance;
  }

  public long getReadChunkSizeBytes() {
    return readChunkSizeBytes;
  }

  public void setReadChunkSizeBytes(long readChunkSizeBytes) {
    this.readChunkSizeBytes = readChunkSizeBytes;
  }

  public List<SupplementEntry> getSupplements() {
    return Collections.unmodifiableList(supplements);
  }

  public void addSupplement(String csvPath, String batchId) {
    supplements.add(new SupplementEntry(csvPath, batchId));
  }

  public File getOutputFile() {
    return new File(outputTsfile);
  }

  public File getMainCsvFile() {
    return new File(mainCsvPath);
  }
}
