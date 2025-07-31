package org.apache.tsfile.common.bitStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {
  private final ByteBuffer buf;
  private final int startPos;

  public ByteBufferBackedInputStream(ByteBuffer buf) {
    this.buf = buf;
    this.startPos = buf.position();
  }

  @Override
  public int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return buf.get() & 0xFF;
  }

  @Override
  public int available() {
    return buf.remaining();
  }

  public int getConsumed() {
    return buf.position() - startPos;
  }
}
