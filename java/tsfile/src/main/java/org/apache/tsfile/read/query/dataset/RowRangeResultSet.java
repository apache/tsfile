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

package org.apache.tsfile.read.query.dataset;

import org.apache.tsfile.write.record.TSRecord;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Iterator;

/**
 * A {@link ResultSet} wrapper that applies row-level offset and limit.
 *
 * <p>Takes the inner ResultSet and releases it on {@link #close()}. Once the limit is reached,
 * {@link #next()} returns {@code false} immediately without calling the underlying ResultSet,
 * avoiding unnecessary data loading.
 *
 * @param offset Number of leading rows to skip (must be &gt;= 0).
 * @param limit Maximum number of rows to return. A value &lt; 0 means no limit.
 */
public class RowRangeResultSet implements ResultSet {

  private final ResultSet inner;
  private final int offset;
  private final int limit;
  private int returnedCount;
  private boolean offsetSkipped;

  public RowRangeResultSet(ResultSet inner, int offset, int limit) {
    this.inner = inner;
    this.offset = Math.max(0, offset);
    this.limit = limit;
    this.returnedCount = 0;
    this.offsetSkipped = false;
  }

  @Override
  public ResultSetMetadata getMetadata() {
    return inner.getMetadata();
  }

  @Override
  public boolean next() throws IOException {
    // Skip the first `offset` rows on the first call.
    if (!offsetSkipped) {
      for (int i = 0; i < offset; i++) {
        if (!inner.next()) {
          offsetSkipped = true;
          return false;
        }
      }
      offsetSkipped = true;
    }

    // Limit reached: return false without touching inner ResultSet.
    // This is the key "pushdown" effect: no further chunk/page loading occurs.
    if (limit >= 0 && returnedCount >= limit) {
      return false;
    }

    boolean hasNext = inner.next();
    if (hasNext) {
      returnedCount++;
    }
    return hasNext;
  }

  @Override
  public int getInt(String columnName) {
    return inner.getInt(columnName);
  }

  @Override
  public int getInt(int columnIndex) {
    return inner.getInt(columnIndex);
  }

  @Override
  public long getLong(String columnName) {
    return inner.getLong(columnName);
  }

  @Override
  public long getLong(int columnIndex) {
    return inner.getLong(columnIndex);
  }

  @Override
  public float getFloat(String columnName) {
    return inner.getFloat(columnName);
  }

  @Override
  public float getFloat(int columnIndex) {
    return inner.getFloat(columnIndex);
  }

  @Override
  public double getDouble(String columnName) {
    return inner.getDouble(columnName);
  }

  @Override
  public double getDouble(int columnIndex) {
    return inner.getDouble(columnIndex);
  }

  @Override
  public boolean getBoolean(String columnName) {
    return inner.getBoolean(columnName);
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    return inner.getBoolean(columnIndex);
  }

  @Override
  public String getString(String columnName) {
    return inner.getString(columnName);
  }

  @Override
  public String getString(int columnIndex) {
    return inner.getString(columnIndex);
  }

  @Override
  public LocalDate getDate(String columnName) {
    return inner.getDate(columnName);
  }

  @Override
  public LocalDate getDate(int columnIndex) {
    return inner.getDate(columnIndex);
  }

  @Override
  public byte[] getBinary(String columnName) {
    return inner.getBinary(columnName);
  }

  @Override
  public byte[] getBinary(int columnIndex) {
    return inner.getBinary(columnIndex);
  }

  @Override
  public boolean isNull(String columnName) {
    return inner.isNull(columnName);
  }

  @Override
  public boolean isNull(int columnIndex) {
    return inner.isNull(columnIndex);
  }

  @Override
  public void close() {
    inner.close();
  }

  @Override
  public Iterator<TSRecord> iterator() {
    return inner.iterator();
  }
}
