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

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Objects;

public class FullPath extends Path {
  public FullPath(IDeviceID device, String measurement) {
    this.device = device;
    this.measurement = measurement;
  }

  @Override
  public String getFullPath() {
    if (fullPath == null) {
      fullPath = device.toString() + TsFileConstant.PATH_SEPARATOR + measurement;
    }
    return fullPath;
  }

  @Override
  public int hashCode() {
    return device.hashCode() + measurement.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Path)) {
      return false;
    }
    return Objects.equals(this.device, ((Path) obj).getIDeviceID())
        && Objects.equals(this.measurement, ((Path) obj).getMeasurement());
  }

  @Override
  public int compareTo(Path path) {
    int deviceCompare = device.compareTo(path.getIDeviceID());
    if (deviceCompare != 0) {
      return deviceCompare;
    }
    return measurement.compareTo(path.getMeasurement());
  }
}
