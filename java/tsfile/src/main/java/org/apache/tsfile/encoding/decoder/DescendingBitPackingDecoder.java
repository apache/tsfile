package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DescendingBitPackingDecoder extends Decoder {
  public DescendingBitPackingDecoder() {
    super(TSEncoding.DESCENDING_BIT_PACKING);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    // TODO Auto-generated method stub
    throw new TsFileDecodingException("Not implemented yet");
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    // TODO Auto-generated method stub
    throw new TsFileDecodingException("Not implemented yet");
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
    throw new TsFileDecodingException("Not implemented yet");
  }

  public static class IntDescendingBitPackingDecoder extends DescendingBitPackingDecoder {
    public IntDescendingBitPackingDecoder() {
      super();
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return Math.toIntExact(super.readLong(buffer));
    }
  }
}
