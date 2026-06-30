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

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Writes one TsFile from a main time-series CSV and zero or more supplement CSVs (no time column).
 * Each supplement CSV is sorted internally; ids are consecutive within its ChunkGroup and chained
 * across supplement files (file2 starts at maxId(file1)+1).
 */
public class HybridCsvTsFileAssembler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HybridCsvTsFileAssembler.class);

  private HybridCsvTsFileAssembler() {}

  /**
   * Executes hybrid import according to {@code config}.
   *
   * @return true if all data was written successfully
   */
  public static boolean execute(HybridImportConfig config) throws IOException, WriteProcessException {
    ImportSchema baseSchema = ImportSchemaParser.parse(config.getSharedSchemaPath());
    ImportSchema mainSchema =
        ImportSchemaUtils.withBatchIdTag(
            baseSchema, config.getBatchIdTag(), config.getMainBatchId());

    File output = config.getOutputFile();
    if (output.getParentFile() != null) {
      output.getParentFile().mkdirs();
    }
    if (output.exists() && !output.delete()) {
      LOGGER.warn(
          Messages.get("log.tools.hybrid_delete_output_failed"), output.getAbsolutePath());
    }

    TsFileWriter writer = null;
    try {
      writer = new TsFileWriter(output);
      writer.setGenerateTableSchema(true);

      TabletBuilder mainBuilder = new TabletBuilder(mainSchema, new TimeConverter(mainSchema.getTimePrecision()));
      writer.registerTableSchema(mainBuilder.getTableSchema());

      writeMainCsv(config, mainSchema, mainBuilder, writer);
      writeSupplementCsvs(config, baseSchema, writer);

      return true;
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private static void writeMainCsv(
      HybridImportConfig config,
      ImportSchema mainSchema,
      TabletBuilder mainBuilder,
      TsFileWriter writer)
      throws IOException, WriteProcessException {
    LOGGER.info(Messages.get("log.tools.hybrid_writing_main_csv"), config.getMainCsvPath());
    try (CsvSourceReader reader =
        new CsvSourceReader(
            config.getMainCsvFile(), mainSchema, config.getReadChunkSizeBytes())) {
      SourceBatch batch;
      while ((batch = reader.readBatch()) != null) {
        if (batch.isEmpty()) {
          continue;
        }
        writer.writeTable(mainBuilder.build(batch));
      }
    }
  }

  private static void writeSupplementCsvs(
      HybridImportConfig config, ImportSchema baseSchema, TsFileWriter writer)
      throws IOException, WriteProcessException {
    long nextSupplementId = 1;

    for (HybridImportConfig.SupplementEntry entry : config.getSupplements()) {
      ImportSchema supplementSchema =
          ImportSchemaUtils.withBatchIdTag(
              baseSchema, config.getBatchIdTag(), entry.getBatchId());
      LOGGER.info(
          Messages.get("log.tools.hybrid_writing_supplement_csv"),
          entry.getCsvPath(),
          entry.getBatchId(),
          nextSupplementId);

      try (SupplementCsvSourceReader reader =
          new SupplementCsvSourceReader(
              entry.getCsvFile(), supplementSchema, config.getReadChunkSizeBytes())) {
        SourceBatch batch = SupplementVarianceSorter.readAll(reader);
        if (batch.isEmpty()) {
          continue;
        }
        if (config.isSupplementSortByVariance()) {
          batch = SupplementVarianceSorter.sortByVariancePriority(batch, baseSchema);
        }

        SyntheticTabletBuilder tabletBuilder =
            new SyntheticTabletBuilder(supplementSchema, config.isValidateUniformTags());
        long fileRowCount = batch.getRowCount();
        long[] timestamps = buildConsecutiveTimestamps(nextSupplementId, fileRowCount);
        writeSupplementBatchInChunks(
            batch, timestamps, tabletBuilder, writer, config.getReadChunkSizeBytes());
        nextSupplementId += fileRowCount;
      }
    }
  }

  /** Builds {@code [startId, startId+1, …, startId+rowCount-1]}. */
  static long[] buildConsecutiveTimestamps(long startId, long rowCount) {
    if (rowCount > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          Messages.format("error.tools.hybrid_supplement_too_many_rows", rowCount));
    }
    int n = (int) rowCount;
    long[] timestamps = new long[n];
    for (int i = 0; i < n; i++) {
      timestamps[i] = startId + i;
    }
    return timestamps;
  }

  private static void writeSupplementBatchInChunks(
      SourceBatch batch,
      long[] timestamps,
      SyntheticTabletBuilder tabletBuilder,
      TsFileWriter writer,
      long chunkSizeBytes)
      throws IOException, WriteProcessException {
    int maxRowsPerChunk = estimateMaxRowsPerChunk(batch, chunkSizeBytes);
    if (maxRowsPerChunk <= 0 || batch.getRowCount() <= maxRowsPerChunk) {
      writer.writeTable(tabletBuilder.build(batch, timestamps));
      return;
    }
    for (int start = 0; start < batch.getRowCount(); start += maxRowsPerChunk) {
      int end = Math.min(start + maxRowsPerChunk, batch.getRowCount());
      SourceBatch rowSlice = sliceRows(batch, start, end);
      long[] timeSlice = Arrays.copyOfRange(timestamps, start, end);
      writer.writeTable(tabletBuilder.build(rowSlice, timeSlice));
    }
  }

  private static int estimateMaxRowsPerChunk(SourceBatch batch, long chunkSizeBytes) {
    if (batch.getRowCount() == 0) {
      return 0;
    }
    long estimatedBytesPerRow = Math.max(32L, (long) batch.getColumnCount() * 32L);
    int maxRows = (int) (chunkSizeBytes / estimatedBytesPerRow);
    return Math.max(1, Math.min(batch.getRowCount(), maxRows));
  }

  private static SourceBatch sliceRows(SourceBatch batch, int startRowInclusive, int endRowExclusive) {
    int rowCount = endRowExclusive - startRowInclusive;
    int colCount = batch.getColumnCount();
    Object[][] colData = new Object[colCount][rowCount];
    for (int c = 0; c < colCount; c++) {
      Object[] column = batch.getColumn(c);
      System.arraycopy(column, startRowInclusive, colData[c], 0, rowCount);
    }
    return new SourceBatch(batch.getColumnNames(), colData, rowCount);
  }
}
