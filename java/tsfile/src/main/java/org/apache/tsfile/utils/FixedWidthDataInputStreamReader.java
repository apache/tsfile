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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tsfile.utils;

import org.apache.tsfile.i18n.Messages;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

final class FixedWidthDataInputStreamReader {

  private static final String RETURN_ERROR_KEY = "error.utils.rwio_short_read";
  private static final ThreadLocal<ReusableBuffers> REUSABLE_BUFFERS =
      ThreadLocal.withInitial(ReusableBuffers::new);

  private FixedWidthDataInputStreamReader() {}

  static long read(DataInputStream inputStream, int length, boolean eofException)
      throws IOException {
    ReusableBuffers reusableBuffers = REUSABLE_BUFFERS.get();
    boolean useReusableBuffer = !reusableBuffers.inUse;
    if (useReusableBuffer) {
      reusableBuffers.inUse = true;
    }

    try {
      byte[] bytes = useReusableBuffer ? reusableBuffers.forLength(length) : new byte[length];
      int readCount = 0;
      while (readCount < length) {
        int count = inputStream.read(bytes, readCount, length - readCount);
        if (count < 0) {
          break;
        }
        if (count == 0) {
          int value = inputStream.read();
          if (value < 0) {
            break;
          }
          bytes[readCount++] = (byte) value;
        } else {
          readCount += count;
        }
      }
      if (readCount != length) {
        String message = Messages.format(RETURN_ERROR_KEY, length, readCount);
        if (eofException) {
          throw new EOFException(message);
        }
        throw new IOException(message);
      }

      long value = 0;
      for (int i = 0; i < length; i++) {
        value = (value << Byte.SIZE) | (bytes[i] & 0xff);
      }
      return value;
    } finally {
      if (useReusableBuffer) {
        reusableBuffers.inUse = false;
      }
    }
  }

  private static final class ReusableBuffers {
    private final byte[] shortBytes = new byte[Short.BYTES];
    private final byte[] intBytes = new byte[Integer.BYTES];
    private final byte[] longBytes = new byte[Long.BYTES];
    private boolean inUse;

    private byte[] forLength(int length) {
      if (length == Short.BYTES) {
        return shortBytes;
      }
      if (length == Integer.BYTES) {
        return intBytes;
      }
      if (length == Long.BYTES) {
        return longBytes;
      }
      throw new IllegalArgumentException(String.valueOf(length));
    }
  }
}
