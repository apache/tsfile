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
package org.apache.tsfile.read;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.TestAeadEncryptionProvider;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.FileEncryptionHeader;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.reader.BufferedTsFileInput;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TsFileSequenceReaderHeaderTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testPlainMarkerIsReadWithVersion() throws Exception {
    ByteArrayOutputStream bytes = baseHeader();
    bytes.write(MetaMarker.SEPARATOR);
    BufferedTsFileInput input = input(bytes);
    AtomicLong ioBytes = new AtomicLong();
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(input, false, false, ioBytes::addAndGet)) {
      assertFalse(reader.hasFileEncryptionHeader());
      assertEquals(7, reader.getDataStartOffset());
      assertEquals(0, reader.position());
      assertEquals(2, ioBytes.get());
      verify(input, times(1)).read(any(ByteBuffer.class));
      verify(input, never()).position(7);
      verify(input, never()).wrapAsInputStream();
    }
  }

  @Test
  public void testEncryptedMarkerIsReadWithVersion() throws Exception {
    ByteArrayOutputStream bytes = baseHeader();
    try (EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(new byte[16], new byte[16])) {
      FileEncryptionHeader.serialize(parameter, bytes);
    }
    BufferedTsFileInput input = input(bytes);
    AtomicLong ioBytes = new AtomicLong();
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(input, false, false, ioBytes::addAndGet)) {
      assertTrue(reader.hasFileEncryptionHeader());
      assertEquals(bytes.size(), reader.getDataStartOffset());
      assertEquals(0, reader.position());
      assertEquals(bytes.size() - 6, ioBytes.get());
      verify(input, never()).position(7);
    }
  }

  @Test
  public void testBaseHeaderWithoutMarker() throws Exception {
    AtomicLong ioBytes = new AtomicLong();
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(input(baseHeader()), false, false, ioBytes::addAndGet)) {
      assertFalse(reader.hasFileEncryptionHeader());
      assertEquals(7, reader.getDataStartOffset());
      assertEquals(1, ioBytes.get());
    }
  }

  @Test
  public void testShortReadsStillPrefetchMarker() throws Exception {
    ByteArrayOutputStream bytes = baseHeader();
    bytes.write(MetaMarker.SEPARATOR);
    BufferedTsFileInput input = input(bytes);
    doAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int limit = buffer.limit();
              buffer.limit(buffer.position() + 1);
              try {
                return invocation.callRealMethod();
              } finally {
                buffer.limit(limit);
              }
            })
        .when(input)
        .read(any(ByteBuffer.class));
    try (TsFileSequenceReader reader = new TsFileSequenceReader(input, false, false, null)) {
      assertFalse(reader.hasFileEncryptionHeader());
      verify(input, times(2)).read(any(ByteBuffer.class));
      verify(input, never()).wrapAsInputStream();
    }
  }

  @Test
  public void testLazyHeaderLoadingPreservesPosition() throws Exception {
    ByteArrayOutputStream bytes = baseHeader();
    bytes.write(MetaMarker.SEPARATOR);
    BufferedTsFileInput input = input(bytes);
    input.position(8);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(input, 8L, 0)) {
      assertFalse(reader.hasFileEncryptionHeader());
      assertEquals(8, reader.position());
    }
  }

  @Test
  public void testChunkHeaderIncludesOrdinalInDescription() {
    ChunkHeader header =
        new ChunkHeader(
            "s1", 10, TSDataType.INT64, CompressionType.LZ4, TSEncoding.PLAIN, 1, 0, 42);
    assertTrue(header.toString().contains("chunkOrdinal=42"));
    ChunkHeader plainHeader =
        new ChunkHeader("s1", 10, TSDataType.INT64, CompressionType.LZ4, TSEncoding.PLAIN, 1);
    assertTrue(plainHeader.toString().contains("chunkOrdinal=-1"));
  }

  private ByteArrayOutputStream baseHeader() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    bytes.write(TSFileConfig.MAGIC_STRING.getBytes(TSFileConfig.STRING_CHARSET));
    bytes.write(TSFileConfig.VERSION_NUMBER);
    return bytes;
  }

  private BufferedTsFileInput input(ByteArrayOutputStream bytes) throws Exception {
    File file = folder.newFile();
    Files.write(file.toPath(), bytes.toByteArray());
    return spy(new BufferedTsFileInput(file.toPath()));
  }
}
