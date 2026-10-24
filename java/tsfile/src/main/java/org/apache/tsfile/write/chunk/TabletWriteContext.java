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

package org.apache.tsfile.write.chunk;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.i18n.Messages;

/** Per-column state shared by the type-specialized Tablet write loops. */
public final class TabletWriteContext {
  private final IDeviceID deviceId;
  private final String measurementId;
  private boolean hasLastTime;
  private long lastTime;
  private int pointCount;

  public TabletWriteContext(IDeviceID deviceId, String measurementId, Long lastTime) {
    this.deviceId = deviceId;
    this.measurementId = measurementId;
    this.hasLastTime = lastTime != null;
    this.lastTime = lastTime == null ? 0 : lastTime;
  }

  public void checkTime(long time) throws WriteProcessException {
    if (hasLastTime && time <= lastTime) {
      throw new WriteProcessException(
          Messages.format(
              "error.write.chunk_group_non_aligned_out_of_order",
              deviceId,
              TsFileConstant.PATH_SEPARATOR,
              measurementId,
              lastTime));
    }
  }

  /** Advance only after a successful write, so failures retain the last accepted timestamp. */
  public void recordTime(long time) {
    lastTime = time;
    hasLastTime = true;
    pointCount++;
  }

  public long getLastTime() {
    return lastTime;
  }

  public int getPointCount() {
    return pointCount;
  }
}
