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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

public class ArrayDeviceID implements IDeviceID {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ArrayDeviceID.class);

  private String[] segments;

  public ArrayDeviceID(String[] segments) {
    this.segments = segments;
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

  @Override
  public byte[] getBytes() {
    ByteArrayOutputStream publicBAOS = new ByteArrayOutputStream(256);
    for (String segment : segments) {
      try {
        publicBAOS.write(segment.getBytes(StandardCharsets.UTF_8));
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
      final int comp = this.segment(i).compareTo(o.segment(i));
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
}
