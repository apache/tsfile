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

package org.apache.tsfile.external.commons.io.build;

import org.apache.tsfile.external.commons.io.Charsets;
import org.apache.tsfile.external.commons.io.IOUtils;
import org.apache.tsfile.external.commons.io.file.PathUtils;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.IntUnaryOperator;

/**
 * Abstracts building a typed instance of {@code T}.
 *
 * @param <T> the type of instances to build.
 * @param <B> the type of builder subclass.
 * @since 2.12.0
 */
public abstract class AbstractStreamBuilder<T, B extends AbstractStreamBuilder<T, B>>
    extends AbstractOriginSupplier<T, B> {

  private static final int DEFAULT_MAX_VALUE = Integer.MAX_VALUE;

  private static final OpenOption[] DEFAULT_OPEN_OPTIONS = PathUtils.EMPTY_OPEN_OPTION_ARRAY;

  /**
   * The buffer size, defaults to {@link IOUtils#DEFAULT_BUFFER_SIZE} ({@value
   * IOUtils#DEFAULT_BUFFER_SIZE}).
   */
  private int bufferSize = IOUtils.DEFAULT_BUFFER_SIZE;

  /**
   * The buffer size, defaults to {@link IOUtils#DEFAULT_BUFFER_SIZE} ({@value
   * IOUtils#DEFAULT_BUFFER_SIZE}).
   */
  private int bufferSizeDefault = IOUtils.DEFAULT_BUFFER_SIZE;

  /** The maximum buffer size. */
  private int bufferSizeMax = DEFAULT_MAX_VALUE;

  /** The Charset, defaults to {@link Charset#defaultCharset()}. */
  private Charset charset = Charset.defaultCharset();

  /** The Charset, defaults to {@link Charset#defaultCharset()}. */
  private Charset charsetDefault = Charset.defaultCharset();

  private OpenOption[] openOptions = DEFAULT_OPEN_OPTIONS;

  /**
   * The default checking behavior for a buffer size request. Throws a {@link
   * IllegalArgumentException} by default.
   */
  private final IntUnaryOperator defaultSizeChecker =
      size -> size > bufferSizeMax ? throwIae(size, bufferSizeMax) : size;

  /** The checking behavior for a buffer size request. */
  private IntUnaryOperator bufferSizeChecker = defaultSizeChecker;

  /**
   * Applies the buffer size request.
   *
   * @param size the size request.
   * @return the size to use, usually the input, or can throw an unchecked exception, like {@link
   *     IllegalArgumentException}.
   */
  private int checkBufferSize(final int size) {
    return bufferSizeChecker.applyAsInt(size);
  }

  /**
   * Gets the buffer size, defaults to {@link IOUtils#DEFAULT_BUFFER_SIZE} ({@value
   * IOUtils#DEFAULT_BUFFER_SIZE}).
   *
   * @return the buffer size, defaults to {@link IOUtils#DEFAULT_BUFFER_SIZE} ({@value
   *     IOUtils#DEFAULT_BUFFER_SIZE}).
   */
  protected int getBufferSize() {
    return bufferSize;
  }

  /**
   * Gets the Charset, defaults to {@link Charset#defaultCharset()}.
   *
   * @return the Charset, defaults to {@link Charset#defaultCharset()}.
   */
  public Charset getCharset() {
    return charset;
  }

  /**
   * Gets the Charset default, defaults to {@link Charset#defaultCharset()}.
   *
   * @return the Charset default, defaults to {@link Charset#defaultCharset()}.
   */
  protected Charset getCharsetDefault() {
    return charsetDefault;
  }

  protected OpenOption[] getOpenOptions() {
    return openOptions;
  }

  /**
   * Gets a Path from the origin.
   *
   * @return A Path
   * @throws UnsupportedOperationException if the origin cannot be converted to a Path.
   * @throws IllegalStateException if the {@code origin} is {@code null}.
   * @see AbstractOrigin#getPath()
   * @since 2.13.0
   */
  protected Path getPath() {
    return checkOrigin().getPath();
  }

  /**
   * Gets an writer from the origin with open options.
   *
   * @return An writer.
   * @throws IOException if an I/O error occurs.
   * @throws UnsupportedOperationException if the origin cannot be converted to a Writer.
   * @throws IllegalStateException if the {@code origin} is {@code null}.
   * @see AbstractOrigin#getOutputStream(OpenOption...)
   * @since 2.13.0
   */
  protected Writer getWriter() throws IOException {
    return checkOrigin().getWriter(getCharset(), getOpenOptions());
  }

  /**
   * Sets the Charset.
   *
   * <p>Subclasses may ignore this setting.
   *
   * @param charset the Charset, null resets to the default.
   * @return this.
   */
  public B setCharset(final Charset charset) {
    this.charset = Charsets.toCharset(charset, charsetDefault);
    return asThis();
  }

  /**
   * Sets the Charset.
   *
   * <p>Subclasses may ignore this setting.
   *
   * @param charset the Charset name, null resets to the default.
   * @return this.
   */
  public B setCharset(final String charset) {
    return setCharset(Charsets.toCharset(charset, charsetDefault));
  }

  private int throwIae(final int size, final int max) {
    throw new IllegalArgumentException(String.format("Request %,d exceeds maximum %,d", size, max));
  }
}
