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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.i18n.Messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Pre-processes a single supplement CSV: compute per-FIELD-column variance within that file,
 * order columns by variance descending (higher variance = higher priority), then sort its rows
 * ascending with a multi-key comparator ({@link Arrays#sort}).
 */
public final class SupplementVarianceSorter {

  private static final Logger LOGGER = LoggerFactory.getLogger(SupplementVarianceSorter.class);

  private SupplementVarianceSorter() {}

  /**
   * Returns a new {@link SourceBatch} with rows sorted according to FIELD column variance priority.
   * Variance is computed only from rows in {@code batch} (single supplement CSV).
   */
  public static SourceBatch sortByVariancePriority(SourceBatch batch, ImportSchema schema) {
    if (batch == null || batch.isEmpty()) {
      return batch;
    }

    List<ImportSchema.SourceColumn> fieldColumns = schema.fieldColumns();
    if (fieldColumns.isEmpty()) {
      return batch;
    }

    int[] batchColumnIndexByField = resolveBatchColumnIndices(batch, fieldColumns);
    double[] variances = computeVariances(batch, fieldColumns, batchColumnIndexByField);
    int[] priorityFieldOrder = sortFieldIndicesByVarianceDesc(variances);

    logPriority(fieldColumns, variances, priorityFieldOrder);

    Integer[] rowOrder = new Integer[batch.getRowCount()];
    for (int i = 0; i < rowOrder.length; i++) {
      rowOrder[i] = i;
    }

    Arrays.sort(
        rowOrder,
        new RowComparator(batch, fieldColumns, batchColumnIndexByField, priorityFieldOrder));

    return reorderRows(batch, rowOrder);
  }

  private static int[] resolveBatchColumnIndices(
      SourceBatch batch, List<ImportSchema.SourceColumn> fieldColumns) {
    int[] indices = new int[fieldColumns.size()];
    for (int f = 0; f < fieldColumns.size(); f++) {
      String name = fieldColumns.get(f).getName();
      int batchIdx = -1;
      for (int c = 0; c < batch.getColumnCount(); c++) {
        if (name.equals(batch.getColumnName(c))) {
          batchIdx = c;
          break;
        }
      }
      if (batchIdx < 0) {
        throw new IllegalArgumentException(
            Messages.format("error.tools.hybrid_field_column_not_in_batch", name));
      }
      indices[f] = batchIdx;
    }
    return indices;
  }

  private static double[] computeVariances(
      SourceBatch batch,
      List<ImportSchema.SourceColumn> fieldColumns,
      int[] batchColumnIndexByField) {
    int fieldCount = fieldColumns.size();
    double[] variances = new double[fieldCount];
    int rowCount = batch.getRowCount();

    for (int f = 0; f < fieldCount; f++) {
      int batchCol = batchColumnIndexByField[f];
      TSDataType type = fieldColumns.get(f).getDataType();
      if (!isNumericType(type)) {
        variances[f] = 0.0;
        continue;
      }

      double sum = 0.0;
      int count = 0;
      for (int r = 0; r < rowCount; r++) {
        Double v = toDouble(batch.getValue(r, batchCol), type);
        if (v != null) {
          sum += v;
          count++;
        }
      }
      if (count < 2) {
        variances[f] = 0.0;
        continue;
      }
      double mean = sum / count;
      double sumSq = 0.0;
      for (int r = 0; r < rowCount; r++) {
        Double v = toDouble(batch.getValue(r, batchCol), type);
        if (v != null) {
          double d = v - mean;
          sumSq += d * d;
        }
      }
      variances[f] = sumSq / count;
    }
    return variances;
  }

  private static int[] sortFieldIndicesByVarianceDesc(double[] variances) {
    Integer[] order = new Integer[variances.length];
    for (int i = 0; i < order.length; i++) {
      order[i] = i;
    }
    Arrays.sort(
        order,
        (a, b) -> {
          int cmp = Double.compare(variances[b], variances[a]);
          if (cmp != 0) {
            return cmp;
          }
          return Integer.compare(a, b);
        });
    int[] result = new int[order.length];
    for (int i = 0; i < order.length; i++) {
      result[i] = order[i];
    }
    return result;
  }

  private static SourceBatch reorderRows(SourceBatch batch, Integer[] rowOrder) {
    int rowCount = rowOrder.length;
    int colCount = batch.getColumnCount();
    Object[][] newColData = new Object[colCount][rowCount];
    for (int newRow = 0; newRow < rowCount; newRow++) {
      int oldRow = rowOrder[newRow];
      for (int c = 0; c < colCount; c++) {
        newColData[c][newRow] = batch.getValue(oldRow, c);
      }
    }
    return new SourceBatch(batch.getColumnNames(), newColData, rowCount);
  }

  private static void logPriority(
      List<ImportSchema.SourceColumn> fieldColumns,
      double[] variances,
      int[] priorityFieldOrder) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < priorityFieldOrder.length; i++) {
      int fieldIdx = priorityFieldOrder[i];
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fieldColumns.get(fieldIdx).getName())
          .append("(variance=")
          .append(variances[fieldIdx])
          .append(")");
    }
    LOGGER.info(Messages.get("log.tools.hybrid_variance_sort_priority"), sb.toString());
  }

  private static boolean isNumericType(TSDataType type) {
    switch (type) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DATE:
        return true;
      default:
        return false;
    }
  }

  private static Double toDouble(Object value, TSDataType type) {
    if (isNullValue(value)) {
      return null;
    }
    try {
      switch (type) {
        case BOOLEAN:
          if (value instanceof Boolean) {
            return ((Boolean) value) ? 1.0 : 0.0;
          }
          return Boolean.parseBoolean(value.toString()) ? 1.0 : 0.0;
        case INT32:
        case DATE:
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.parseDouble(value.toString());
        case INT64:
        case TIMESTAMP:
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.parseDouble(value.toString());
        case FLOAT:
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.parseDouble(value.toString());
        case DOUBLE:
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.parseDouble(value.toString());
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean isNullValue(Object value) {
    if (value == null) {
      return true;
    }
    if (value instanceof String) {
      return ((String) value).isEmpty();
    }
    return false;
  }

  private static final class RowComparator implements Comparator<Integer> {

    private final SourceBatch batch;
    private final List<ImportSchema.SourceColumn> fieldColumns;
    private final int[] batchColumnIndexByField;
    private final int[] priorityFieldOrder;

    private RowComparator(
        SourceBatch batch,
        List<ImportSchema.SourceColumn> fieldColumns,
        int[] batchColumnIndexByField,
        int[] priorityFieldOrder) {
      this.batch = batch;
      this.fieldColumns = fieldColumns;
      this.batchColumnIndexByField = batchColumnIndexByField;
      this.priorityFieldOrder = priorityFieldOrder;
    }

    @Override
    public int compare(Integer rowA, Integer rowB) {
      for (int fieldIdx : priorityFieldOrder) {
        int batchCol = batchColumnIndexByField[fieldIdx];
        TSDataType type = fieldColumns.get(fieldIdx).getDataType();
        int cmp = compareCell(batch, rowA, rowB, batchCol, type);
        if (cmp != 0) {
          return cmp;
        }
      }
      return Integer.compare(rowA, rowB);
    }

    private int compareCell(
        SourceBatch batch, int rowA, int rowB, int batchCol, TSDataType type) {
      Object va = batch.getValue(rowA, batchCol);
      Object vb = batch.getValue(rowB, batchCol);
      boolean na = isNullValue(va);
      boolean nb = isNullValue(vb);
      if (na && nb) {
        return 0;
      }
      if (na) {
        return 1;
      }
      if (nb) {
        return -1;
      }
      if (isNumericType(type)) {
        Double da = toDouble(va, type);
        Double db = toDouble(vb, type);
        if (da == null && db == null) {
          return 0;
        }
        if (da == null) {
          return 1;
        }
        if (db == null) {
          return -1;
        }
        return Double.compare(da, db);
      }
      return va.toString().compareTo(vb.toString());
    }
  }

  /** Reads all batches from {@code reader} into one {@link SourceBatch}. */
  public static SourceBatch readAll(SupplementCsvSourceReader reader) {
    List<SourceBatch> parts = new ArrayList<>();
    SourceBatch batch;
    while ((batch = reader.readBatch()) != null) {
      if (!batch.isEmpty()) {
        parts.add(batch);
      }
    }
    return SourceBatch.concat(parts);
  }
}
