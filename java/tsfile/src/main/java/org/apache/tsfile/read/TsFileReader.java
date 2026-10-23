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

package org.apache.tsfile.read;

import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.executor.TsFileExecutor;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TsFileReader implements AutoCloseable {

  private TsFileSequenceReader fileReader;
  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;
  private TsFileExecutor tsfileExecutor;
  private final Set<QueryDataSet> queryDataSets = Collections.synchronizedSet(new HashSet<>());

  @TsFileApi
  public TsFileReader(File file) throws IOException {
    this(new TsFileSequenceReader(file.getPath()));
  }

  /** Constructor, create ReadOnlyTsFile with {@link TsFileSequenceReader}. */
  public TsFileReader(TsFileSequenceReader fileReader) throws IOException {
    this.fileReader = fileReader;
    this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
    this.chunkLoader = new CachedChunkLoaderImpl(fileReader);
    this.tsfileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
  }

  public QueryDataSet query(QueryExpression queryExpression) throws IOException {
    return retainQuery(tsfileExecutor.execute(queryExpression));
  }

  public QueryDataSet query(
      QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset)
      throws IOException {
    return retainQuery(
        tsfileExecutor.execute(queryExpression, partitionStartOffset, partitionEndOffset));
  }

  private QueryDataSet retainQuery(QueryDataSet dataSet) throws IOException {
    if (fileReader.hasFileEncryptionHeader()) {
      synchronized (queryDataSets) {
        queryDataSets.removeIf(QueryDataSet::isClosed);
        queryDataSets.add(dataSet);
      }
    }
    return dataSet;
  }

  public List<String> getAllDeviceIds() throws IOException {
    List<IDeviceID> deviceIDList = fileReader.getAllDevices();
    List<String> deviceIds = new ArrayList<>(deviceIDList.size());
    for (IDeviceID deviceID : deviceIDList) {
      deviceIds.add(deviceID.toString());
    }
    return deviceIds;
  }

  public List<MeasurementSchema> getMeasurement(IDeviceID deviceID) throws IOException {
    Map<String, TSDataType> measurementMap = fileReader.getMeasurement(deviceID);
    return measurementMap.entrySet().stream()
        .map(entry -> new MeasurementSchema(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  @TsFileApi
  public void close() throws IOException {
    IOException failure = null;
    try {
      synchronized (queryDataSets) {
        for (QueryDataSet dataSet : queryDataSets) {
          try {
            dataSet.close();
          } catch (IOException e) {
            if (failure == null) {
              failure = e;
            } else {
              failure.addSuppressed(e);
            }
          }
        }
      }
    } finally {
      queryDataSets.clear();
      try {
        fileReader.close();
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
