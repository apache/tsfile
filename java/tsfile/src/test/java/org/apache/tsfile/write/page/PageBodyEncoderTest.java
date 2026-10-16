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

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptionProviderRegistry;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.encrypt.PageCryptoContext;
import org.apache.tsfile.encrypt.TestAeadEncryptionProvider;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class PageBodyEncoderTest {

  @BeforeClass
  public static void registerProvider() {
    EncryptionProviderRegistry.registerProvider(TestAeadEncryptionProvider.INSTANCE);
  }

  @AfterClass
  public static void unregisterProvider() {
    EncryptionProviderRegistry.unregisterProvider(TestAeadEncryptionProvider.PROVIDER_ID);
  }

  @Test
  public void testUncompressedBodyWrapsSliceWithoutCopying() throws Exception {
    ByteBuffer input = slicedInput();
    ByteBuffer encoded =
        PageBodyEncoder.encode(
            input,
            input.remaining(),
            ICompressor.getCompressor(CompressionType.UNCOMPRESSED),
            null,
            0,
            0);
    assertSame(input.array(), encoded.array());
    assertEquals(input.remaining(), encoded.remaining());
    assertEquals(input.arrayOffset() + input.position(), encoded.position());
    assertArrayEquals(remainingBytes(input), remainingBytes(encoded));
    assertEquals(2, input.position());
  }

  @Test
  public void testCompressedBodyUsesOnlySliceAndValidOutputBytes() throws Exception {
    for (CompressionType compression :
        new CompressionType[] {CompressionType.GZIP, CompressionType.LZ4}) {
      ByteBuffer input = slicedInput();
      ByteBuffer encoded =
          PageBodyEncoder.encode(
              input, input.remaining(), ICompressor.getCompressor(compression), null, 0, 0);
      byte[] restored = new byte[input.remaining()];
      assertEquals(
          restored.length,
          IUnCompressor.getUnCompressor(compression)
              .uncompress(
                  encoded.array(),
                  encoded.arrayOffset() + encoded.position(),
                  encoded.remaining(),
                  restored,
                  0));
      assertArrayEquals(remainingBytes(input), restored);
      assertEquals(2, input.position());
    }
  }

  @Test
  public void testEncryptedBodyRoundTripForSlicedInput() throws Exception {
    for (CompressionType compression :
        new CompressionType[] {
          CompressionType.UNCOMPRESSED, CompressionType.GZIP, CompressionType.LZ4
        }) {
      ByteBuffer input = slicedInput();
      try (EncryptParameter parameter =
          TestAeadEncryptionProvider.createParameter(new byte[16], new byte[16])) {
        ByteBuffer encoded =
            PageBodyEncoder.encode(
                input, input.remaining(), ICompressor.getCompressor(compression), parameter, 3, 7);
        PageCryptoContext context =
            PageCryptoContext.forDecryption(
                parameter, input.remaining(), encoded.remaining(), 3, 7);
        byte[] compressed =
            IDecryptor.getDecryptor(parameter)
                .decryptPage(
                    encoded.array(),
                    encoded.arrayOffset() + encoded.position(),
                    encoded.remaining(),
                    context);
        byte[] restored = new byte[input.remaining()];
        assertEquals(
            restored.length,
            IUnCompressor.getUnCompressor(compression)
                .uncompress(compressed, 0, compressed.length, restored, 0));
        assertArrayEquals(remainingBytes(input), restored);
        assertEquals(2, input.position());
      }
    }
  }

  private static ByteBuffer slicedInput() {
    byte[] bytes = new byte[140];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
    ByteBuffer parent = ByteBuffer.wrap(bytes);
    parent.position(3);
    parent.limit(135);
    ByteBuffer slice = parent.slice();
    slice.position(2);
    slice.limit(130);
    return slice;
  }

  private static byte[] remainingBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }
}
