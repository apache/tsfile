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

import org.apache.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** In-memory {@link TsFileOutput} for benchmarking encode time without disk I/O. */
public final class MemoryTsFileOutput implements TsFileOutput {

  private byte[] buf = new byte[4 * 1024 * 1024];
  private int len = 0;

  private void ensure(int add) {
    int need = len + add;
    if (need <= buf.length) {
      return;
    }
    int newCap = buf.length;
    while (newCap < need) {
      newCap = newCap < (1 << 30) ? newCap << 1 : need;
    }
    buf = Arrays.copyOf(buf, newCap);
  }

  @Override
  public void write(byte[] b) throws IOException {
    ensure(b.length);
    System.arraycopy(b, 0, buf, len, b.length);
    len += b.length;
  }

  @Override
  public void write(byte b) throws IOException {
    ensure(1);
    buf[len++] = b;
  }

  @Override
  public void write(ByteBuffer b) throws IOException {
    int n = b.remaining();
    ensure(n);
    if (b.hasArray()) {
      System.arraycopy(b.array(), b.arrayOffset() + b.position(), buf, len, n);
      len += n;
      b.position(b.limit());
    } else {
      for (int i = 0; i < n; i++) {
        buf[len++] = b.get();
      }
    }
  }

  @Override
  public long getPosition() {
    return len;
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public OutputStream wrapAsStream() {
    return new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        MemoryTsFileOutput.this.write((byte) b);
      }

      @Override
      public void write(byte[] b, int off, int ln) throws IOException {
        ensure(ln);
        System.arraycopy(b, off, buf, len, ln);
        len += ln;
      }
    };
  }

  @Override
  public void flush() {
    // no-op
  }

  @Override
  public void truncate(long size) throws IOException {
    if (size < 0 || size > Integer.MAX_VALUE) {
      throw new IOException("invalid truncate size: " + size);
    }
    len = (int) size;
  }

  @Override
  public void force() {
    // no-op (no disk)
  }

  public byte[] toByteArray() {
    return Arrays.copyOf(buf, len);
  }
}
