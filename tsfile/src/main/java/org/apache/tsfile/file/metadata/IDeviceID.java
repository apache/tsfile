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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.utils.Accountable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/** Device id interface. */
public interface IDeviceID extends Comparable<IDeviceID>, Accountable, Serializable {

  Logger LOGGER = LoggerFactory.getLogger(IDeviceID.class);

  int serialize(ByteBuffer byteBuffer);

  int serialize(OutputStream outputStream) throws IOException;

  byte[] getBytes();

  boolean isEmpty();

  /**
   * @return the table name associated with the device. For a path-DeviceId, like "root.a.b.c.d", it
   *     is converted according to a fixed rule, like assuming the first three levels ("root.a.b")
   *     as the table name; for a tuple-deviceId, like "(table1, beijing, turbine)", it is the first
   *     element in the deviceId, namely "table1".
   */
  String getTableName();

  /**
   * @return how many segments this DeviceId consists of. For a path-DeviceId, like "root.a.b.c.d",
   *     it is 5; fot a tuple-DeviceId, like "(table1, beijing, turbine)", it is 3.
   */
  int segmentNum();

  /**
   * @param i the sequence number of the segment that should be returned.
   * @return i-th segment in this DeviceId.
   * @throws ArrayIndexOutOfBoundsException if i >= segmentNum().
   */
  Object segment(int i);

  default int serializedSize() {
    LOGGER.debug(
        "Using default inefficient implementation of serialized size by {}", this.getClass());
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      serialize(baos);
      return baos.size();
    } catch (IOException e) {
      LOGGER.error("Failed to serialize device ID: {}", this, e);
      return -1;
    }
  }

  interface Deserializer {
    IDeviceID deserializeFrom(ByteBuffer byteBuffer);

    IDeviceID deserializeFrom(InputStream inputStream) throws IOException;

    Deserializer DEFAULT_DESERIALIZER = StringArrayDeviceID.getDESERIALIZER();
  }

  interface Factory {
    IDeviceID create(String deviceIdString);

    /**
     * The first segment is the table name and the rests are id columns.
     *
     * @param segments Example: ["table0", "id0", "id1"]
     * @return a deviceId corresponding to the segments
     */
    IDeviceID create(String[] segments);

    Factory DEFAULT_FACTORY = StringArrayDeviceID.getFACTORY();
  }
}
