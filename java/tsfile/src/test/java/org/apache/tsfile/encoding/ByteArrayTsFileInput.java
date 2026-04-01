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

package org.apache.tsfile.encoding;

import org.apache.tsfile.read.reader.TsFileInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/** {@link TsFileInput} over a byte array (no disk I/O) for read-path benchmarking. */
public final class ByteArrayTsFileInput implements TsFileInput {

  private final byte[] data;
  private long position;

  public ByteArrayTsFileInput(byte[] data) {
    this.data = data;
    this.position = 0L;
  }

  @Override
  public long size() {
    return data.length;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public TsFileInput position(long newPosition) {
    this.position = newPosition;
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (position >= data.length) {
      return -1;
    }
    int max = (int) Math.min(dst.remaining(), data.length - position);
    if (max <= 0) {
      return 0;
    }
    dst.put(data, (int) position, max);
    position += max;
    return max;
  }

  @Override
  public int read(ByteBuffer dst, long pos) throws IOException {
    if (pos >= data.length) {
      return -1;
    }
    int max = (int) Math.min(dst.remaining(), data.length - pos);
    if (max <= 0) {
      return 0;
    }
    dst.put(data, (int) pos, max);
    return max;
  }

  /**
   * Stream reads advance {@link #position} so {@link org.apache.tsfile.read.TsFileSequenceReader}
   * sequential parsing (readChunkHeader / readPageHeader / readChunkGroupHeader) stays aligned with
   * {@link #read(ByteBuffer)}-based reads.
   */
  @Override
  public InputStream wrapAsInputStream() {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        if (position >= data.length) {
          return -1;
        }
        return data[(int) position++] & 0xff;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (position >= data.length) {
          return -1;
        }
        int n = (int) Math.min(len, data.length - position);
        System.arraycopy(data, (int) position, b, off, n);
        position += n;
        return n;
      }
    };
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public String getFilePath() {
    return "byte[]://tsfile-benchmark";
  }
}
