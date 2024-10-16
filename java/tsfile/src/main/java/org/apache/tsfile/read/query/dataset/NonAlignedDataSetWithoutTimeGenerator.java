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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.reader.series.AbstractFileSeriesReader;

import java.io.IOException;
import java.util.List;

import static org.apache.tsfile.read.query.dataset.DataSetWithoutTimeGenerator.putValueToField;

public class NonAlignedDataSetWithoutTimeGenerator extends QueryDataSet {
  private final List<AbstractFileSeriesReader> readers;
  private BatchData batchData;

  public NonAlignedDataSetWithoutTimeGenerator(
      final List<Path> paths,
      final List<TSDataType> dataTypes,
      final List<AbstractFileSeriesReader> readers) {
    super(paths, dataTypes);
    this.readers = readers;
    this.columnNum = 1;
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return batchData.hasCurrent();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    RowRecord rowRecord = null;

    if (batchData.hasCurrent()) {
      rowRecord = new RowRecord(batchData.currentTime());
      rowRecord.addField(putValueToField(batchData));
      batchData.next();
    }

    while (!batchData.hasCurrent() && !readers.isEmpty()) {
      while (!readers.get(0).hasNextBatch()) {
        readers.remove(0);
      }
      if (!readers.isEmpty()) {
        batchData = readers.get(0).nextBatch();
      }
    }
    return rowRecord;
  }
}
