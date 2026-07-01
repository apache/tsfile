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

package org.apache.tsfile.external.commons.io;

import org.apache.tsfile.external.commons.io.function.IOSupplier;
import org.apache.tsfile.external.commons.io.function.IOTriFunction;
import org.apache.tsfile.external.commons.io.output.ByteArrayOutputStream;
import org.apache.tsfile.external.commons.io.output.StringBuilderWriter;
import org.apache.tsfile.external.commons.io.output.ThresholdingOutputStream;
import org.apache.tsfile.external.commons.io.output.UnsynchronizedByteArrayOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;

public class IOUtils {

  /**
   * A singleton empty byte array.
   *
   * @since 2.9.0
   */
  public static final byte[] EMPTY_BYTE_ARRAY = {};

  /**
   * Represents the end-of-file (or stream).
   *
   * @since 2.5 (made public)
   */
  public static final int EOF = -1;

  /** The default buffer size ({@value}) to use in copy methods. */
  public static final int DEFAULT_BUFFER_SIZE = 8192;

  public static int length(final Object[] array) {
    return array == null ? 0 : array.length;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /** Internal char array buffer, intended for both reading and writing. */
  private static final ThreadLocal<char[]> SCRATCH_CHAR_BUFFER_RW =
      ThreadLocal.withInitial(IOUtils::charArray);

  /**
   * Returns a new char array of size {@link #DEFAULT_BUFFER_SIZE}.
   *
   * @return a new char array of size {@link #DEFAULT_BUFFER_SIZE}.
   * @since 2.9.0
   */
  private static char[] charArray() {
    return charArray(DEFAULT_BUFFER_SIZE);
  }

  /**
   * Returns a new char array of the given size.
   *
   * <p>TODO Consider guarding or warning against large allocations...
   *
   * @param size array size.
   * @return a new char array of the given size.
   * @since 2.9.0
   */
  private static char[] charArray(final int size) {
    return new char[size];
  }

  /**
   * Gets the contents of a {@link Reader} as a {@code byte[]} using the specified character
   * encoding.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedReader}.
   *
   * @param reader the {@link Reader} to read
   * @param charset the charset to use, null means platform default
   * @return the requested byte array
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   * @since 2.3
   */
  public static byte[] toByteArray(final Reader reader, final Charset charset) throws IOException {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      copy(reader, output, charset);
      return output.toByteArray();
    }
  }

  /**
   * Copies chars from a {@link Reader} to bytes on an {@link OutputStream} using the specified
   * character encoding, and calling flush.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedReader}.
   *
   * <p>Due to the implementation of OutputStreamWriter, this method performs a flush.
   *
   * <p>This method uses {@link OutputStreamWriter}.
   *
   * @param reader the {@link Reader} to read
   * @param output the {@link OutputStream} to write to
   * @param outputCharset the charset to use for the OutputStream, null means platform default
   * @throws NullPointerException if the input or output is null
   * @throws IOException if an I/O error occurs
   * @since 2.3
   */
  public static void copy(
      final Reader reader, final OutputStream output, final Charset outputCharset)
      throws IOException {
    final OutputStreamWriter writer =
        new OutputStreamWriter(output, Charsets.toCharset(outputCharset));
    copy(reader, writer);
    // XXX Unless anyone is planning on rewriting OutputStreamWriter,
    // we have to flush here.
    writer.flush();
  }

  /**
   * Gets the contents of an {@link InputStream} as a {@code byte[]}.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * @param inputStream the {@link InputStream} to read.
   * @return the requested byte array.
   * @throws NullPointerException if the InputStream is {@code null}.
   * @throws IOException if an I/O error occurs or reading more than {@link Integer#MAX_VALUE}
   *     occurs.
   */
  public static byte[] toByteArray(final InputStream inputStream) throws IOException {
    // We use a ThresholdingOutputStream to avoid reading AND writing more than Integer.MAX_VALUE.
    try (UnsynchronizedByteArrayOutputStream ubaOutput =
            UnsynchronizedByteArrayOutputStream.builder().get();
        ThresholdingOutputStream thresholdOutput =
            new ThresholdingOutputStream(
                Integer.MAX_VALUE,
                os -> {
                  throw new IllegalArgumentException(
                      String.format(
                          "Cannot read more than %,d into a byte array", Integer.MAX_VALUE));
                },
                os -> ubaOutput)) {
      copy(inputStream, thresholdOutput);
      return ubaOutput.toByteArray();
    }
  }

  /**
   * Returns a new byte array of size {@link #DEFAULT_BUFFER_SIZE}.
   *
   * @return a new byte array of size {@link #DEFAULT_BUFFER_SIZE}.
   * @since 2.9.0
   */
  public static byte[] byteArray() {
    return byteArray(DEFAULT_BUFFER_SIZE);
  }

  /**
   * Returns a new byte array of the given size.
   *
   * <p>TODO Consider guarding or warning against large allocations...
   *
   * @param size array size.
   * @return a new byte array of the given size.
   * @throws NegativeArraySizeException if the size is negative.
   * @since 2.9.0
   */
  public static byte[] byteArray(final int size) {
    return new byte[size];
  }

  /**
   * Copies bytes from an {@link InputStream} to an {@link OutputStream}.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * <p>Large streams (over 2GB) will return a bytes copied value of {@code -1} after the copy has
   * completed since the correct number of bytes cannot be returned as an int. For large streams use
   * the {@link #copyLarge(InputStream, OutputStream)} method.
   *
   * @param inputStream the {@link InputStream} to read.
   * @param outputStream the {@link OutputStream} to write.
   * @return the number of bytes copied, or -1 if greater than {@link Integer#MAX_VALUE}.
   * @throws NullPointerException if the InputStream is {@code null}.
   * @throws NullPointerException if the OutputStream is {@code null}.
   * @throws IOException if an I/O error occurs.
   * @since 1.1
   */
  public static int copy(final InputStream inputStream, final OutputStream outputStream)
      throws IOException {
    final long count = copyLarge(inputStream, outputStream);
    return count > Integer.MAX_VALUE ? EOF : (int) count;
  }

  /**
   * Copies bytes from an {@link InputStream} to an {@link OutputStream} using an internal buffer of
   * the given size.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * @param inputStream the {@link InputStream} to read.
   * @param outputStream the {@link OutputStream} to write to
   * @param bufferSize the bufferSize used to copy from the input to the output
   * @return the number of bytes copied.
   * @throws NullPointerException if the InputStream is {@code null}.
   * @throws NullPointerException if the OutputStream is {@code null}.
   * @throws IOException if an I/O error occurs.
   * @since 2.5
   */
  public static long copy(
      final InputStream inputStream, final OutputStream outputStream, final int bufferSize)
      throws IOException {
    return copyLarge(inputStream, outputStream, IOUtils.byteArray(bufferSize));
  }

  /**
   * Copies bytes from a large (over 2GB) {@link InputStream} to an {@link OutputStream}.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * <p>The buffer size is given by {@link #DEFAULT_BUFFER_SIZE}.
   *
   * @param inputStream the {@link InputStream} to read.
   * @param outputStream the {@link OutputStream} to write.
   * @return the number of bytes copied.
   * @throws NullPointerException if the InputStream is {@code null}.
   * @throws NullPointerException if the OutputStream is {@code null}.
   * @throws IOException if an I/O error occurs.
   * @since 1.3
   */
  public static long copyLarge(final InputStream inputStream, final OutputStream outputStream)
      throws IOException {
    return copy(inputStream, outputStream, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Copies bytes from a large (over 2GB) {@link InputStream} to an {@link OutputStream}.
   *
   * <p>This method uses the provided buffer, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * @param inputStream the {@link InputStream} to read.
   * @param outputStream the {@link OutputStream} to write.
   * @param buffer the buffer to use for the copy
   * @return the number of bytes copied.
   * @throws NullPointerException if the InputStream is {@code null}.
   * @throws NullPointerException if the OutputStream is {@code null}.
   * @throws IOException if an I/O error occurs.
   * @since 2.2
   */
  @SuppressWarnings("resource") // streams are closed by the caller.
  public static long copyLarge(
      final InputStream inputStream, final OutputStream outputStream, final byte[] buffer)
      throws IOException {
    Objects.requireNonNull(inputStream, "inputStream");
    Objects.requireNonNull(outputStream, "outputStream");
    long count = 0;
    int n;
    while (EOF != (n = inputStream.read(buffer))) {
      outputStream.write(buffer, 0, n);
      count += n;
    }
    return count;
  }

  /**
   * Gets the contents of a {@link Reader} as a String.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedReader}.
   *
   * @param reader the {@link Reader} to read
   * @return the requested String
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   */
  public static String toString(final Reader reader) throws IOException {
    try (StringBuilderWriter sw = new StringBuilderWriter()) {
      copy(reader, sw);
      return sw.toString();
    }
  }

  /**
   * Copies chars from a {@link Reader} to a {@link Writer}.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedReader}.
   *
   * <p>Large streams (over 2GB) will return a chars copied value of {@code -1} after the copy has
   * completed since the correct number of chars cannot be returned as an int. For large streams use
   * the {@link #copyLarge(Reader, Writer)} method.
   *
   * @param reader the {@link Reader} to read.
   * @param writer the {@link Writer} to write.
   * @return the number of characters copied, or -1 if &gt; Integer.MAX_VALUE
   * @throws NullPointerException if the input or output is null
   * @throws IOException if an I/O error occurs
   * @since 1.1
   */
  public static int copy(final Reader reader, final Writer writer) throws IOException {
    final long count = copyLarge(reader, writer);
    if (count > Integer.MAX_VALUE) {
      return EOF;
    }
    return (int) count;
  }

  /**
   * Copies chars from a large (over 2GB) {@link Reader} to a {@link Writer}.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedReader}.
   *
   * <p>The buffer size is given by {@link #DEFAULT_BUFFER_SIZE}.
   *
   * @param reader the {@link Reader} to source.
   * @param writer the {@link Writer} to target.
   * @return the number of characters copied
   * @throws NullPointerException if the input or output is null
   * @throws IOException if an I/O error occurs
   * @since 1.3
   */
  public static long copyLarge(final Reader reader, final Writer writer) throws IOException {
    return copyLarge(reader, writer, getScratchCharArray());
  }

  /**
   * Copies chars from a large (over 2GB) {@link Reader} to a {@link Writer}.
   *
   * <p>This method uses the provided buffer, so there is no need to use a {@link BufferedReader}.
   *
   * @param reader the {@link Reader} to source.
   * @param writer the {@link Writer} to target.
   * @param buffer the buffer to be used for the copy
   * @return the number of characters copied
   * @throws NullPointerException if the input or output is null
   * @throws IOException if an I/O error occurs
   * @since 2.2
   */
  public static long copyLarge(final Reader reader, final Writer writer, final char[] buffer)
      throws IOException {
    long count = 0;
    int n;
    while (EOF != (n = reader.read(buffer))) {
      writer.write(buffer, 0, n);
      count += n;
    }
    return count;
  }

  /**
   * Gets the char byte array buffer, intended for both reading and writing.
   *
   * @return the char byte array buffer, intended for both reading and writing.
   */
  static char[] getScratchCharArray() {
    return fill0(SCRATCH_CHAR_BUFFER_RW.get());
  }

  /**
   * Fills the given array with 0s.
   *
   * @param arr The array to fill.
   * @return The given array.
   */
  private static char[] fill0(final char[] arr) {
    Arrays.fill(arr, (char) 0);
    return arr;
  }

  /**
   * Gets the contents of an input as a {@code byte[]}.
   *
   * @param input the input to read.
   * @param size the size of the input to read, where 0 &lt; {@code size} &lt;= length of input.
   * @return byte [] of length {@code size}.
   * @throws IOException if an I/O error occurs or input length is smaller than parameter {@code
   *     size}.
   * @throws IllegalArgumentException if {@code size} is less than zero.
   */
  static byte[] toByteArray(
      final IOTriFunction<byte[], Integer, Integer, Integer> input, final int size)
      throws IOException {

    if (size < 0) {
      throw new IllegalArgumentException("Size must be equal or greater than zero: " + size);
    }

    if (size == 0) {
      return EMPTY_BYTE_ARRAY;
    }

    final byte[] data = byteArray(size);
    int offset = 0;
    int read;

    while (offset < size && (read = input.apply(data, offset, size - offset)) != EOF) {
      offset += read;
    }

    if (offset != size) {
      throw new IOException("Unexpected read size, current: " + offset + ", expected: " + size);
    }

    return data;
  }

  /**
   * Writes chars from a {@link String} to bytes on an {@link OutputStream} using the specified
   * character encoding.
   *
   * <p>This method uses {@link String#getBytes(String)}.
   *
   * @param data the {@link String} to write, null ignored
   * @param output the {@link OutputStream} to write to
   * @param charset the charset to use, null means platform default
   * @throws NullPointerException if output is null
   * @throws IOException if an I/O error occurs
   * @since 2.3
   */
  @SuppressWarnings("resource")
  public static void write(final String data, final OutputStream output, final Charset charset)
      throws IOException {
    if (data != null) {
      // Use Charset#encode(String), since calling String#getBytes(Charset) might result in
      // NegativeArraySizeException or OutOfMemoryError.
      // The underlying OutputStream should not be closed, so the channel is not closed.
      Channels.newChannel(output).write(Charsets.toCharset(charset).encode(data));
    }
  }

  /**
   * Gets the contents of an {@link InputStream} from a supplier as a String using the specified
   * character encoding.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * @param input supplies the {@link InputStream} to read
   * @param charset the charset to use, null means platform default
   * @return the requested String
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   * @since 2.12.0
   */
  public static String toString(final IOSupplier<InputStream> input, final Charset charset)
      throws IOException {
    return toString(
        input,
        charset,
        () -> {
          throw new NullPointerException("input");
        });
  }

  /**
   * Gets the contents of an {@link InputStream} from a supplier as a String using the specified
   * character encoding.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * @param input supplies the {@link InputStream} to read
   * @param charset the charset to use, null means platform default
   * @param defaultString the default return value if the supplier or its value is null.
   * @return the requested String
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   * @since 2.12.0
   */
  public static String toString(
      final IOSupplier<InputStream> input,
      final Charset charset,
      final IOSupplier<String> defaultString)
      throws IOException {
    if (input == null) {
      return defaultString.get();
    }
    try (InputStream inputStream = input.get()) {
      return inputStream != null ? toString(inputStream, charset) : defaultString.get();
    }
  }

  /**
   * Gets the contents of an {@link InputStream} as a String using the specified character encoding.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * @param input the {@link InputStream} to read
   * @param charset the charset to use, null means platform default
   * @return the requested String
   * @throws NullPointerException if the input is null
   * @throws IOException if an I/O error occurs
   * @since 2.3
   */
  public static String toString(final InputStream input, final Charset charset) throws IOException {
    try (StringBuilderWriter sw = new StringBuilderWriter()) {
      copy(input, sw, charset);
      return sw.toString();
    }
  }

  /**
   * Copies bytes from an {@link InputStream} to chars on a {@link Writer} using the specified
   * character encoding.
   *
   * <p>This method buffers the input internally, so there is no need to use a {@link
   * BufferedInputStream}.
   *
   * <p>This method uses {@link InputStreamReader}.
   *
   * @param input the {@link InputStream} to read
   * @param writer the {@link Writer} to write to
   * @param inputCharset the charset to use for the input stream, null means platform default
   * @throws NullPointerException if the input or output is null
   * @throws IOException if an I/O error occurs
   * @since 2.3
   */
  public static void copy(final InputStream input, final Writer writer, final Charset inputCharset)
      throws IOException {
    final InputStreamReader reader = new InputStreamReader(input, Charsets.toCharset(inputCharset));
    copy(reader, writer);
  }
}
