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
package org.apache.tsfile.write.page;

import java.nio.ByteBuffer;

/**
 * Reusable scratch buffers for building one page body. They are owned by a page writer, are valid
 * only until the page data has been copied, and must not be retained by callers.
 */
final class PageBodyBuffer {

  private ByteBuffer uncompressed;
  private byte[] compressed;

  /** Returns a cleared heap buffer with at least {@code capacity} bytes. */
  ByteBuffer uncompressed(int capacity) {
    if (uncompressed == null || uncompressed.capacity() < capacity) {
      uncompressed = ByteBuffer.allocate(capacity);
    } else {
      uncompressed.clear();
    }
    return uncompressed;
  }

  /** Retained heap bytes of both scratch buffers. */
  long retainedBytes() {
    return (uncompressed == null ? 0 : uncompressed.capacity())
        + (compressed == null ? 0 : compressed.length);
  }

  /** Returns an output array with at least {@code capacity} bytes for the compressor. */
  byte[] compressed(int capacity) {
    if (compressed == null || compressed.length < capacity) {
      compressed = new byte[capacity];
    }
    return compressed;
  }
}
