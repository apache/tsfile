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

package org.apache.tsfile.read.common;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;

public class TimeSeries {
  private IDeviceID deviceId;
  private String measurementName;

  public TimeSeries(IDeviceID deviceId, String measurementName) {
    this.deviceId = deviceId;
    this.measurementName = measurementName;
  }

  public IDeviceID getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(IDeviceID deviceId) {
    this.deviceId = deviceId;
  }

  public String getMeasurementName() {
    return measurementName;
  }

  public void setMeasurementName(String measurementName) {
    this.measurementName = measurementName;
  }

  public static List<TimeSeries> getPathList(IDeviceID deviceId, String... measurements) {
    List<TimeSeries> pathList = new ArrayList<>(measurements.length);
    for (String measurement : measurements) {
      pathList.add(new TimeSeries(deviceId, measurement));
    }
    return pathList;
  }
}
