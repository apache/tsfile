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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.WriteUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class StringArrayDeviceID implements IDeviceID {

  private static final Deserializer DESERIALIZER =
      new Deserializer() {
        @Override
        public IDeviceID deserializeFrom(ByteBuffer byteBuffer) {
          return deserialize(byteBuffer);
        }

        @Override
        public IDeviceID deserializeFrom(InputStream inputStream) throws IOException {
          return deserialize(inputStream);
        }
      };

  private static final Factory FACTORY =
      new Factory() {
        @Override
        public IDeviceID create(String deviceIdString) {
          return new StringArrayDeviceID(deviceIdString);
        }
      };

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(StringArrayDeviceID.class);

  // TODO: change to Object[] and rename to just ArrayDeviceID
  // or we can just use a tuple like Relational DB.
  private final String[] segments;

  public StringArrayDeviceID(String[] segments) {
    this.segments = segments;
  }

  public StringArrayDeviceID(String deviceIdString) {
    this.segments = deviceIdString.split(TsFileConstant.PATH_SEPARATER_NO_REGEX);
  }

  public static Deserializer getDESERIALIZER() {
    return DESERIALIZER;
  }

  public static Factory getFACTORY() {
    return FACTORY;
  }

  @Override
  public int serialize(ByteBuffer byteBuffer) {
    int cnt = 0;
    cnt += ReadWriteIOUtils.write(segments.length, byteBuffer);
    for (String segment : segments) {
      cnt += ReadWriteIOUtils.write(segment, byteBuffer);
    }
    return cnt;
  }

  @Override
  public int serialize(OutputStream outputStream) throws IOException {
    int cnt = 0;
    cnt += ReadWriteIOUtils.write(segments.length, outputStream);
    for (String segment : segments) {
      cnt += ReadWriteIOUtils.write(segment, outputStream);
    }
    return cnt;
  }

  public static StringArrayDeviceID deserialize(ByteBuffer byteBuffer) {
    final int cnt = byteBuffer.getInt();
    String[] segments = new String[cnt];
    for (int i = 0; i < cnt; i++) {
      final int stringSize = byteBuffer.getInt();
      byte[] stringBytes = new byte[stringSize];
      byteBuffer.get(stringBytes);
      segments[i] = new String(stringBytes, TSFileConfig.STRING_CHARSET);
    }
    return new StringArrayDeviceID(segments);
  }

  public static StringArrayDeviceID deserialize(InputStream stream) throws IOException {
    final int cnt = ReadWriteIOUtils.readInt(stream);
    String[] segments = new String[cnt];
    for (int i = 0; i < cnt; i++) {
      final int stringSize = ReadWriteIOUtils.readInt(stream);
      byte[] stringBytes = new byte[stringSize];
      final int readCnt = stream.read(stringBytes);
      if (readCnt != stringSize) {
        throw new IOException(String.format("Expected %d bytes but read %d", stringSize, readCnt));
      }
      segments[i] = new String(stringBytes, TSFileConfig.STRING_CHARSET);
    }
    return new StringArrayDeviceID(segments);
  }

  @Override
  public byte[] getBytes() {
    ByteArrayOutputStream publicBAOS = new ByteArrayOutputStream(256);
    for (String segment : segments) {
      try {
        publicBAOS.write(segment.getBytes(TSFileConfig.STRING_CHARSET));
      } catch (IOException e) {
        throw new TsFileRuntimeException(e);
      }
    }
    return publicBAOS.toByteArray();
  }

  @Override
  public boolean isEmpty() {
    return segments == null || segments.length == 0;
  }

  @Override
  public String getTableName() {
    return segments[0];
  }

  @Override
  public int segmentNum() {
    return segments.length;
  }

  @Override
  public String segment(int i) {
    return segments[i];
  }

  @Override
  public int compareTo(IDeviceID o) {
    int thisSegmentNum = segmentNum();
    int otherSegmentNum = o.segmentNum();
    for (int i = 0; i < thisSegmentNum; i++) {
      if (i >= otherSegmentNum) {
        // the other ID is a prefix of this one
        return 1;
      }
      final int comp =
          Objects.compare(this.segment(i), ((String) o.segment(i)), WriteUtils::compareStrings);
      if (comp != 0) {
        // the partial comparison has a result
        return comp;
      }
    }

    if (thisSegmentNum < otherSegmentNum) {
      // this ID is a prefix of the other one
      return -1;
    }

    // two ID equal
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + RamUsageEstimator.sizeOf(segments);
  }

  @Override
  public int serializedSize() {
    int cnt = Integer.BYTES;
    for (String segment : segments) {
      cnt += Integer.BYTES;
      cnt += segment.getBytes(TSFileConfig.STRING_CHARSET).length;
    }
    return cnt;
  }

  @Override
  public String toString() {
    return String.join(".", segments);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringArrayDeviceID deviceID = (StringArrayDeviceID) o;
    return Objects.deepEquals(segments, deviceID.segments);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(segments);
  }
}
