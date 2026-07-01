/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tsfile.external.commons.io.output;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implements a ThreadSafe version of {@link AbstractByteArrayOutputStream} using instance
 * synchronization.
 */
// @ThreadSafe
public class ByteArrayOutputStream extends AbstractByteArrayOutputStream {

  /**
   * Constructs a new byte array output stream. The buffer capacity is initially {@value
   * AbstractByteArrayOutputStream#DEFAULT_SIZE} bytes, though its size increases if necessary.
   */
  public ByteArrayOutputStream() {
    this(DEFAULT_SIZE);
  }

  /**
   * Constructs a new byte array output stream, with a buffer capacity of the specified size, in
   * bytes.
   *
   * @param size the initial size
   * @throws IllegalArgumentException if size is negative
   */
  public ByteArrayOutputStream(final int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: " + size);
    }
    synchronized (this) {
      needNewBuffer(size);
    }
  }

  /**
   * @see java.io.ByteArrayOutputStream#reset()
   */
  @Override
  public synchronized void reset() {
    resetImpl();
  }

  @Override
  public synchronized int size() {
    return count;
  }

  @Override
  public synchronized byte[] toByteArray() {
    return toByteArrayImpl();
  }

  @Override
  public synchronized InputStream toInputStream() {
    return toInputStream(java.io.ByteArrayInputStream::new);
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }
    synchronized (this) {
      writeImpl(b, off, len);
    }
  }

  @Override
  public synchronized int write(final InputStream in) throws IOException {
    return writeImpl(in);
  }

  @Override
  public synchronized void write(final int b) {
    writeImpl(b);
  }

  @Override
  public synchronized void writeTo(final OutputStream out) throws IOException {
    writeToImpl(out);
  }
}
