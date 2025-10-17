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

import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.record.TSRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TreeResultSet extends AbstractResultSet {
  private QueryDataSet queryDataSet;
  private List<String> deviceList;
  private List<String> measurementList;
  private Map<Path, Integer> pathIndexMap;
  private Map<String, Map<String, Path>> cachedPaths;

  public TreeResultSet(
      QueryDataSet queryDataSet, List<String> deviceIds, List<String> measurementNames) {
    super(
        queryDataSet.getPaths().stream().map(Path::toString).collect(Collectors.toList()),
        queryDataSet.getDataTypes());
    this.queryDataSet = queryDataSet;
    this.deviceList = deviceIds;
    this.measurementList = measurementNames;
    List<Path> paths = queryDataSet.getPaths();
    this.pathIndexMap =
        IntStream.range(0, paths.size()).boxed().collect(Collectors.toMap(paths::get, i -> i));
    this.cachedPaths = new HashMap<>();
    for (String device : deviceList) {
      Map<String, Path> measurementPathMap = new HashMap<>();
      for (String measurement : measurementList) {
        measurementPathMap.put(
            measurement, new Path(new StringArrayDeviceID(device), measurement, false));
      }
      cachedPaths.put(device, measurementPathMap);
    }
  }

  @TsFileApi
  public boolean next() throws IOException {
    while (queryDataSet.hasNext()) {
      currentRow = queryDataSet.next();
      if (currentRow.isAllNull()) {
        continue;
      }
      return true;
    }
    return false;
  }

  @TsFileApi
  public void close() {
    // nothing to be done
  }

  @Override
  public Iterator<TSRecord> recordIterator() {
    return new TreeResultSet.RecordIterator();
  }

  private class RecordIterator implements Iterator<TSRecord> {
    private final LinkedList<TSRecord> recordBuffer = new LinkedList<>();
    private boolean exhausted = false;

    @Override
    public boolean hasNext() {
      if (!recordBuffer.isEmpty()) {
        return true;
      }
      if (exhausted) {
        return false;
      }

      try {
        return fetchRecords();
      } catch (IOException e) {
        throw new NoSuchElementException(e.toString());
      }
    }

    private boolean fetchRecords() throws IOException {
      boolean hasNewRecords = false;
      while (TreeResultSet.this.next()) {
        for (String device : deviceList) {
          TSRecord record = new TSRecord(device, getLong("Time"));
          record.addPoint("id", device);

          Map<String, Path> devicePaths = cachedPaths.get(device);
          for (String measurement : measurementList) {
            Path path = devicePaths.get(measurement);
            Integer pathIdx = pathIndexMap.get(path);
            if (pathIdx != null) {
              Field field = currentRow.getField(pathIdx);
              switch (field.getDataType()) {
                case INT32:
                case DATE:
                  record.addPoint(measurement, field.getIntV());
                  break;
                case INT64:
                case TIMESTAMP:
                  record.addPoint(measurement, field.getLongV());
                  break;
                case FLOAT:
                  record.addPoint(measurement, field.getFloatV());
                  break;
                case DOUBLE:
                  record.addPoint(measurement, field.getDoubleV());
                  break;
                case STRING:
                case TEXT:
                  record.addPoint(measurement, field.getStringValue());
                  break;
                case BLOB:
                  record.addPoint(measurement, field.getBinaryV().getValues());
                  break;
                case BOOLEAN:
                  record.addPoint(measurement, field.getBoolV());
                  break;
                case VECTOR:
                case UNKNOWN:
                default:
                  break;
              }
            } else {
              record.dataPointList.add(null);
            }
          }
          recordBuffer.add(record);
          hasNewRecords = true;
        }
      }
      if (!hasNewRecords) {
        exhausted = true;
        return false;
      }
      return true;
    }

    @Override
    public TSRecord next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return recordBuffer.poll();
    }
  }
}
