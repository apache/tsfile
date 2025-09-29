package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SeparateStorageDecoder extends Decoder {

  public SeparateStorageDecoder() {
    super(TSEncoding.SEPARATE_STORAGE);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'hasNext'");
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'reset'");
  }

  public static class IntSeparateStorageDecoder extends SeparateStorageDecoder {
    public IntSeparateStorageDecoder() {
      super();
    }
  }
}
