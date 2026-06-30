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

import java.util.Arrays;
import java.util.List;

public class SourceBatch {

  private final String[] columnNames;
  private final Object[][] columnData;
  private final int rowCount;

  public SourceBatch(String[] columnNames, Object[][] columnData, int rowCount) {
    this.columnNames = columnNames;
    this.columnData = columnData;
    this.rowCount = rowCount;
  }

  public static SourceBatch fromRows(List<String> columnNames, List<Object[]> rows) {
    int colCount = columnNames.size();
    int rowCount = rows.size();
    String[] names = columnNames.toArray(new String[0]);
    Object[][] colData = new Object[colCount][rowCount];
    for (int r = 0; r < rowCount; r++) {
      Object[] row = rows.get(r);
      for (int c = 0; c < colCount; c++) {
        colData[c][r] = c < row.length ? row[c] : null;
      }
    }
    return new SourceBatch(names, colData, rowCount);
  }

  public int getRowCount() {
    return rowCount;
  }

  public int getColumnCount() {
    return columnNames.length;
  }

  public String getColumnName(int columnIndex) {
    return columnNames[columnIndex];
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public Object getValue(int rowIndex, int columnIndex) {
    return columnData[columnIndex][rowIndex];
  }

  public Object[] getColumn(int columnIndex) {
    return columnData[columnIndex];
  }

  public boolean isEmpty() {
    return rowCount == 0;
  }

  /** Concatenates multiple batches with identical column names into one batch. */
  public static SourceBatch concat(List<SourceBatch> batches) {
    if (batches == null || batches.isEmpty()) {
      return new SourceBatch(new String[0], new Object[0][0], 0);
    }
    SourceBatch first = batches.get(0);
    if (batches.size() == 1) {
      return first;
    }
    int totalRows = 0;
    for (SourceBatch batch : batches) {
      totalRows += batch.getRowCount();
    }
    int colCount = first.getColumnCount();
    String[] names = first.getColumnNames();
    Object[][] colData = new Object[colCount][totalRows];
    int offset = 0;
    for (SourceBatch batch : batches) {
      if (batch.getColumnCount() != colCount) {
        throw new IllegalArgumentException("Cannot concat batches with different column counts");
      }
      for (int c = 0; c < colCount; c++) {
        if (!names[c].equals(batch.getColumnName(c))) {
          throw new IllegalArgumentException(
              "Cannot concat batches with different column names: "
                  + names[c]
                  + " vs "
                  + batch.getColumnName(c));
        }
        System.arraycopy(batch.getColumn(c), 0, colData[c], offset, batch.getRowCount());
      }
      offset += batch.getRowCount();
    }
    return new SourceBatch(names, colData, totalRows);
  }

  @Override
  public String toString() {
    return "SourceBatch{columns=" + Arrays.toString(columnNames) + ", rows=" + rowCount + '}';
  }
}
