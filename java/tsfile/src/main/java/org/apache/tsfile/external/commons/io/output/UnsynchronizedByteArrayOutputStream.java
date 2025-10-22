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

import org.apache.tsfile.external.commons.io.build.AbstractOrigin;
import org.apache.tsfile.external.commons.io.build.AbstractStreamBuilder;
import org.apache.tsfile.external.commons.io.function.Uncheck;
import org.apache.tsfile.external.commons.io.input.UnsynchronizedByteArrayInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implements a version of {@link AbstractByteArrayOutputStream} <b>without</b> any concurrent
 * thread safety.
 *
 * <p>To build an instance, see {@link Builder}.
 *
 * @since 2.7
 */
// @NotThreadSafe
public final class UnsynchronizedByteArrayOutputStream extends AbstractByteArrayOutputStream {

  /**
   * Builds a new {@link UnsynchronizedByteArrayOutputStream} instance.
   *
   * <p>Using File IO:
   *
   * <pre>{@code
   * UnsynchronizedByteArrayOutputStream s = UnsynchronizedByteArrayOutputStream.builder()
   *   .setBufferSize(8192)
   *   .get();
   * }</pre>
   *
   * <p>Using NIO Path:
   *
   * <pre>{@code
   * UnsynchronizedByteArrayOutputStream s = UnsynchronizedByteArrayOutputStream.builder()
   *   .setBufferSize(8192)
   *   .get();
   * }</pre>
   */
  public static class Builder
      extends AbstractStreamBuilder<UnsynchronizedByteArrayOutputStream, Builder> {

    /**
     * Constructs a new instance.
     *
     * <p>This builder use the aspect buffer size.
     *
     * @return a new instance.
     * @see AbstractOrigin#getByteArray()
     */
    @Override
    public UnsynchronizedByteArrayOutputStream get() {
      return new UnsynchronizedByteArrayOutputStream(getBufferSize());
    }
  }

  /**
   * Constructs a new {@link Builder}.
   *
   * @return a new {@link Builder}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Constructs a new byte array output stream. The buffer capacity is initially
   *
   * <p>{@value AbstractByteArrayOutputStream#DEFAULT_SIZE} bytes, though its size increases if
   * necessary.
   *
   * @deprecated Use {@link #builder()}, {@link Builder}, and {@link Builder#get()}.
   */
  @Deprecated
  public UnsynchronizedByteArrayOutputStream() {
    this(DEFAULT_SIZE);
  }

  /**
   * Constructs a new byte array output stream, with a buffer capacity of the specified size, in
   * bytes.
   *
   * @param size the initial size
   * @throws IllegalArgumentException if size is negative
   * @deprecated Use {@link #builder()}, {@link Builder}, and {@link Builder#get()}. Will be private
   *     in 3.0.0.
   */
  @Deprecated
  public UnsynchronizedByteArrayOutputStream(final int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: " + size);
    }
    needNewBuffer(size);
  }

  /**
   * @see java.io.ByteArrayOutputStream#reset()
   */
  @Override
  public void reset() {
    resetImpl();
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public byte[] toByteArray() {
    return toByteArrayImpl();
  }

  @Override
  public InputStream toInputStream() {
    // @formatter:off
    return toInputStream(
        (buffer, offset, length) ->
            Uncheck.get(
                () ->
                    UnsynchronizedByteArrayInputStream.builder()
                        .setByteArray(buffer)
                        .setOffset(offset)
                        .setLength(length)
                        .get()));
    // @formatter:on
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) {
      throw new IndexOutOfBoundsException(String.format("offset=%,d, length=%,d", off, len));
    }
    if (len == 0) {
      return;
    }
    writeImpl(b, off, len);
  }

  @Override
  public int write(final InputStream in) throws IOException {
    return writeImpl(in);
  }

  @Override
  public void write(final int b) {
    writeImpl(b);
  }

  @Override
  public void writeTo(final OutputStream out) throws IOException {
    writeToImpl(out);
  }
}
