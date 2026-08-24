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

package org.apache.tsfile.read.reader;

import org.apache.tsfile.i18n.Messages;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.Objects;

/** A local TsFile input that caches file read results in a {@link ByteBuffer}. */
public class BufferedTsFileInput extends LocalTsFileInput {

  private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;

  private final ByteBuffer buffer;
  private long bufferStartPosition = -1;
  private long logicalPosition;

  public BufferedTsFileInput(Path file) throws IOException {
    this(file, DEFAULT_BUFFER_SIZE);
  }

  public BufferedTsFileInput(Path file, int bufferSize) throws IOException {
    super(validateBufferSize(file, bufferSize));
    buffer = ByteBuffer.allocate(bufferSize);
    buffer.limit(0);
  }

  private static Path validateBufferSize(Path file, int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException(
          Messages.get("error.utils.buffer_size_not_positive_input"));
    }
    return file;
  }

  @Override
  public long position() throws IOException {
    ensureOpen();
    return logicalPosition;
  }

  @Override
  public BufferedTsFileInput position(long newPosition) throws IOException {
    ensureOpen();
    if (newPosition < 0) {
      throw new IllegalArgumentException();
    }
    logicalPosition = newPosition;
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int readSize = read(dst, logicalPosition);
    if (readSize > 0) {
      logicalPosition += readSize;
    }
    return readSize;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    Objects.requireNonNull(dst);
    ensureOpen();
    if (position < 0) {
      throw new IllegalArgumentException();
    }
    if (!dst.hasRemaining()) {
      return 0;
    }

    int totalReadSize = 0;
    long currentPosition = position;
    while (dst.hasRemaining()) {
      if (isInBuffer(currentPosition)) {
        int copiedSize = copyFromBuffer(dst, currentPosition);
        totalReadSize += copiedSize;
        currentPosition += copiedSize;
        continue;
      }

      if (dst.remaining() >= buffer.capacity()) {
        int readSize = super.read(dst, currentPosition);
        if (readSize <= 0) {
          return totalReadSize == 0 ? readSize : totalReadSize;
        }
        return totalReadSize + readSize;
      }

      int readSize = fillBuffer(currentPosition);
      if (readSize <= 0) {
        return totalReadSize == 0 ? readSize : totalReadSize;
      }
    }
    return totalReadSize;
  }

  private boolean isInBuffer(long position) {
    return position >= bufferStartPosition && position - bufferStartPosition < buffer.limit();
  }

  private int copyFromBuffer(ByteBuffer dst, long position) {
    int bufferOffset = (int) (position - bufferStartPosition);
    int copiedSize = Math.min(dst.remaining(), buffer.limit() - bufferOffset);
    ByteBuffer source = buffer.asReadOnlyBuffer();
    source.position(bufferOffset);
    source.limit(bufferOffset + copiedSize);
    dst.put(source);
    return copiedSize;
  }

  private int fillBuffer(long position) throws IOException {
    buffer.clear();
    int readSize = super.read(buffer, position);
    buffer.flip();
    bufferStartPosition = position;
    return readSize;
  }

  @Override
  public InputStream wrapAsInputStream() {
    return new BufferedTsFileInputStream();
  }

  @Override
  public void close() throws IOException {
    buffer.limit(0);
    super.close();
  }

  private void ensureOpen() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  private class BufferedTsFileInputStream extends InputStream {

    private final ByteBuffer oneByteBuffer = ByteBuffer.allocate(Byte.BYTES);

    @Override
    public int read() throws IOException {
      oneByteBuffer.clear();
      int readSize = BufferedTsFileInput.this.read(oneByteBuffer);
      if (readSize < 0) {
        return -1;
      }
      oneByteBuffer.flip();
      return oneByteBuffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
      return BufferedTsFileInput.this.read(ByteBuffer.wrap(bytes, offset, length));
    }

    @Override
    public long skip(long skippedBytes) throws IOException {
      if (skippedBytes <= 0) {
        return 0;
      }
      long currentPosition = BufferedTsFileInput.this.position();
      long remainingSize = Math.max(0, BufferedTsFileInput.this.size() - currentPosition);
      long actualSkippedBytes = Math.min(skippedBytes, remainingSize);
      BufferedTsFileInput.this.position(currentPosition + actualSkippedBytes);
      return actualSkippedBytes;
    }

    @Override
    public int available() throws IOException {
      long remainingSize =
          Math.max(0, BufferedTsFileInput.this.size() - BufferedTsFileInput.this.position());
      return (int) Math.min(remainingSize, Integer.MAX_VALUE);
    }

    @Override
    public void close() throws IOException {
      BufferedTsFileInput.this.close();
    }
  }
}
