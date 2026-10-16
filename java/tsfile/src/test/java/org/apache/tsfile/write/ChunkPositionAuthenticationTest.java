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

package org.apache.tsfile.write;

import org.apache.tsfile.encrypt.ChunkCryptoContext;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptionProviderRegistry;
import org.apache.tsfile.encrypt.IEncrypt;
import org.apache.tsfile.encrypt.IEncryptProvider;
import org.apache.tsfile.encrypt.TestAeadEncryptionProvider;
import org.apache.tsfile.encrypt.UNENCRYPTED;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.LocalTsFileInput;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ChunkPositionAuthenticationTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void registerProvider() {
    EncryptionProviderRegistry.registerProvider(TestAeadEncryptionProvider.INSTANCE);
  }

  @AfterClass
  public static void unregisterProvider() {
    EncryptionProviderRegistry.unregisterProvider(TestAeadEncryptionProvider.PROVIDER_ID);
  }

  @Test
  public void testProofBindsOffsetOrdinalAndFileIdentity() {
    try (EncryptParameter parameter = parameter();
        EncryptParameter other =
            TestAeadEncryptionProvider.createParameter(
                new byte[16], new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})) {
      byte[] proof = parameter.authenticateChunk(100, 3);
      assertEquals(parameter.getChunkAuthenticationSize(), proof.length);
      parameter.verifyChunkAuthentication(proof, 100, 3);
      assertThrows(
          EncryptException.class, () -> parameter.verifyChunkAuthentication(proof, 101, 3));
      assertThrows(
          EncryptException.class, () -> parameter.verifyChunkAuthentication(proof, 100, 4));
      assertThrows(EncryptException.class, () -> other.verifyChunkAuthentication(proof, 100, 3));
      byte[] corrupted = proof.clone();
      corrupted[0] ^= 1;
      assertThrows(
          EncryptException.class, () -> parameter.verifyChunkAuthentication(corrupted, 100, 3));
      corrupted[0] ^= 1;
      corrupted[corrupted.length - 1] ^= 1;
      assertThrows(
          EncryptException.class, () -> parameter.verifyChunkAuthentication(corrupted, 100, 3));
      assertThrows(
          EncryptException.class, () -> parameter.verifyChunkAuthentication(new byte[0], 100, 3));
      assertFalse(Arrays.equals(proof, parameter.authenticateChunk(100, 3)));
      ChunkCryptoContext context = ChunkCryptoContext.forChunk(parameter, 100, 3);
      byte[] aad = context.getAssociatedData();
      aad[0] ^= 1;
      assertFalse(Arrays.equals(aad, context.getAssociatedData()));
      assertThrows(EncryptException.class, () -> ChunkCryptoContext.forChunk(parameter, -1, 3));
      assertThrows(EncryptException.class, () -> ChunkCryptoContext.forChunk(parameter, 100, -1));
      parameter.close();
      assertThrows(EncryptException.class, () -> parameter.authenticateChunk(100, 3));
    }
  }

  @Test
  public void testUnsupportedProviderAndMissingHeaderProofFailClosed() throws Exception {
    try (EncryptParameter parameter = parameter();
        IEncrypt unsupported = new UNENCRYPTED(null)) {
      ChunkCryptoContext context = ChunkCryptoContext.forChunk(parameter, 100, 3);
      assertThrows(EncryptException.class, () -> unsupported.authenticateChunk(context));
      assertThrows(
          EncryptException.class,
          () -> unsupported.verifyChunkAuthentication(new byte[28], context));
      ChunkHeader header =
          new ChunkHeader(
              "s1", 0, TSDataType.INT64, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0, 0, 3);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      assertThrows(EncryptException.class, () -> header.serializeTo(output));
      assertEquals(0, output.size());
      assertThrows(EncryptException.class, header::getSerializedSize);
      ByteBuffer buffer = ByteBuffer.allocate(128);
      assertThrows(EncryptException.class, () -> header.serializeTo(buffer));
      assertEquals(0, buffer.position());
    }
  }

  @Test
  public void testWholeChunkSwapRejectedByBothReadPathsAndQuery() throws Exception {
    File file = writeFile();
    List<ChunkMetadata> metadata = metadata(file);
    swapChunks(file, metadata);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      assertThrows(
          EncryptException.class,
          () -> reader.readChunkHeader(metadata.get(0).getOffsetOfChunkHeader()));
      reader.position(metadata.get(1).getOffsetOfChunkHeader());
      byte marker = reader.readMarker();
      assertThrows(EncryptException.class, () -> reader.readChunkHeader(marker));
      assertThrows(EncryptException.class, () -> reader.readMemChunk(metadata.get(0)));
    }
    Exception failure =
        assertThrows(
            Exception.class,
            () -> {
              try (TsFileReader reader =
                  new TsFileReader(new TsFileSequenceReader(file.getPath()))) {
                QueryDataSet data =
                    reader.query(
                        QueryExpression.create(
                            Collections.singletonList(new Path("d1", "s1", true)), null));
                while (data.hasNext()) {
                  data.next();
                }
              }
            });
    assertTrue(EncryptException.isEncryptionFailure(failure));
  }

  @Test
  public void testRecoveryDoesNotTruncateAwayAuthenticationFailure() throws Exception {
    File file = writeFile();
    List<ChunkMetadata> metadata = metadata(file);
    int end;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      ChunkHeader last = reader.readChunkHeader(metadata.get(1).getOffsetOfChunkHeader());
      end =
          Math.toIntExact(
              metadata.get(1).getOffsetOfChunkHeader()
                  + last.getSerializedSize()
                  + last.getDataSize());
    }
    swapChunks(file, metadata);
    byte[] bytes = Arrays.copyOf(Files.readAllBytes(file.toPath()), end);
    Files.write(file.toPath(), bytes);
    IOException failure = assertThrows(IOException.class, () -> new RestorableTsFileIOWriter(file));
    assertTrue(EncryptException.isEncryptionFailure(failure));
    assertArrayEquals(bytes, Files.readAllBytes(file.toPath()));
  }

  @Test
  public void testIncompleteTailProofCanBeRecovered() throws Exception {
    File file = writeFile();
    List<ChunkMetadata> metadata = metadata(file);
    int end;
    long dataStart;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      ChunkHeader first = reader.readChunkHeader(metadata.get(0).getOffsetOfChunkHeader());
      end =
          Math.toIntExact(
              metadata.get(0).getOffsetOfChunkHeader()
                  + first.getSerializedSize()
                  - first.getChunkAuthenticationSize()
                  + 10);
      dataStart = reader.getDataStartOffset();
    }
    Files.write(file.toPath(), Arrays.copyOf(Files.readAllBytes(file.toPath()), end));
    try (RestorableTsFileIOWriter recovered = new RestorableTsFileIOWriter(file)) {
      assertTrue(recovered.canWrite());
      assertEquals(dataStart, file.length());
    }
  }

  @Test
  public void testEmptyValueChunkHasPositionProof() throws Exception {
    File file = new File(folder.getRoot(), "empty.tsfile");
    long offset;
    try (EncryptParameter parameter = parameter();
        TsFileIOWriter writer = new TsFileIOWriter(file, parameter)) {
      writer.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"));
      offset = writer.getPos();
      writer.writeEmptyValueChunk(
          "s1",
          CompressionType.UNCOMPRESSED,
          TSDataType.INT64,
          TSEncoding.PLAIN,
          Statistics.getStatsByType(TSDataType.INT64));
      writer.endChunkGroup();
      writer.endFile();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      ChunkHeader header = reader.readChunkHeader(offset);
      assertEquals(0, header.getDataSize());
      assertEquals(28, header.getChunkAuthentication().length);
      ChunkHeader plain =
          new ChunkHeader(
              "s1", 0, TSDataType.INT64, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0);
      try (Chunk encrypted = new Chunk(header, ByteBuffer.allocate(0));
          Chunk plaintext = new Chunk(plain, ByteBuffer.allocate(0))) {
        assertEquals(
            RamUsageEstimator.sizeOfByteArray(28),
            encrypted.getRetainedSizeInBytes() - plaintext.getRetainedSizeInBytes());
      }
      ByteBuffer buffer = ByteBuffer.allocate(header.getSerializedSize());
      assertEquals(header.getSerializedSize(), header.serializeTo(buffer));
    }
  }

  @Test
  public void testVariableProofHeaderRoundTripAndFragmentedReads() throws Exception {
    for (int size : new int[] {28, 32, 128, ChunkCryptoContext.MAX_AUTHENTICATION_SIZE}) {
      ChunkHeader source =
          new ChunkHeader(
              "s1", 0, TSDataType.INT64, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0, 0, 3);
      byte[] proof = new byte[size];
      Arrays.fill(proof, (byte) 0x5A);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      int length = source.serializeTo(output, proof);
      byte[] bytes = output.toByteArray();
      assertEquals(bytes.length, length);
      assertEquals(ChunkHeader.getSerializedSize("s1", 0, size), length);
      ByteArrayInputStream stream =
          new ByteArrayInputStream(bytes, 1, bytes.length - 1) {
            @Override
            public synchronized int read(byte[] target, int offset, int count) {
              return super.read(target, offset, Math.min(count, 2));
            }
          };
      ChunkHeader sequential = ChunkHeader.deserializeFrom(stream, bytes[0], size);
      assertArrayEquals(proof, sequential.getChunkAuthentication());
      assertEquals(length, sequential.getSerializedSize());
      assertEquals(0, stream.available());
      ByteBuffer buffer = ByteBuffer.allocate(length);
      assertEquals(length, sequential.serializeTo(buffer));
      assertArrayEquals(bytes, buffer.array());
      File file = folder.newFile();
      Files.write(file.toPath(), bytes);
      try (CloseableInput input =
          new CloseableInput(file) {
            @Override
            public int read(ByteBuffer target, long position) throws IOException {
              int limit = target.limit();
              target.limit(Math.min(limit, target.position() + 2));
              try {
                return super.read(target, position);
              } finally {
                target.limit(limit);
              }
            }
          }) {
        input.position(2);
        ChunkHeader positional = ChunkHeader.deserializeFrom(input, 0, null, size);
        assertEquals(length, positional.getSerializedSize());
        assertArrayEquals(proof, positional.getChunkAuthentication());
        assertEquals(2, input.position());
      }
      for (int truncatedLength : new int[] {length - size - 1, length - 1}) {
        byte[] truncated = Arrays.copyOf(bytes, truncatedLength);
        assertThrows(
            EOFException.class,
            () ->
                ChunkHeader.deserializeFrom(
                    new ByteArrayInputStream(truncated, 1, truncated.length - 1), bytes[0], size));
        Files.write(file.toPath(), truncated);
        try (CloseableInput input = new CloseableInput(file)) {
          assertThrows(EOFException.class, () -> ChunkHeader.deserializeFrom(input, 0, null, size));
        }
      }
    }
  }

  @Test
  public void testInvalidOrMismatchedLengthIsRejectedBeforeProofRead() throws Exception {
    ChunkHeader source =
        new ChunkHeader(
            "s1", 0, TSDataType.INT64, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 0, 0, 3);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    source.serializeTo(output, new byte[32]);
    byte[] prefix = Arrays.copyOf(output.toByteArray(), output.size() - 32);
    for (int size : new int[] {-1, 0, 28, 33, ChunkCryptoContext.MAX_AUTHENTICATION_SIZE + 1}) {
      ByteBuffer.wrap(prefix).putInt(prefix.length - Integer.BYTES, size);
      assertThrows(
          EncryptException.class,
          () ->
              ChunkHeader.deserializeFrom(
                  new ByteArrayInputStream(prefix, 1, prefix.length - 1), prefix[0], 32));
      File file = folder.newFile();
      Files.write(file.toPath(), prefix);
      try (CloseableInput input = new CloseableInput(file)) {
        assertThrows(EncryptException.class, () -> ChunkHeader.deserializeFrom(input, 0, null, 32));
      }
    }
  }

  @Test
  public void testProviderMustDeclareBoundedProofSizeAndReturnMatchingProof() {
    for (int declared : new int[] {-1, 0, ChunkCryptoContext.MAX_AUTHENTICATION_SIZE + 1, 32}) {
      String id = "test-proof-size-" + declared;
      EncryptionProviderRegistry.registerProvider(
          new IEncryptProvider() {
            @Override
            public String getProviderId() {
              return id;
            }

            @Override
            public IEncrypt create(EncryptParameter parameter) {
              return new UNENCRYPTED(null) {
                @Override
                public int getChunkAuthenticationSize() {
                  return declared;
                }

                @Override
                public byte[] authenticateChunk(ChunkCryptoContext context) {
                  return new byte[28];
                }
              };
            }
          });
      try (EncryptParameter parameter =
          EncryptParameter.pageAeadBuilder()
              .providerId(id)
              .profileId("test")
              .fileCryptoId(new byte[16])
              .build()) {
        if (declared != 32) {
          assertThrows(EncryptException.class, parameter::getChunkAuthenticationSize);
        }
        assertThrows(EncryptException.class, () -> parameter.authenticateChunk(100, 0));
        assertThrows(
            EncryptException.class,
            () -> parameter.verifyChunkAuthentication(new byte[28], 100, 0));
      } finally {
        EncryptionProviderRegistry.unregisterProvider(id);
      }
    }
  }

  @Test
  public void test32ByteProofSupportsQueryBothReadersAndRecovery() throws Exception {
    File file = writeFile(true);
    List<ChunkMetadata> metadata = metadata(file);
    long dataEnd;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      for (ChunkMetadata item : metadata) {
        ChunkHeader header = reader.readChunkHeader(item.getOffsetOfChunkHeader());
        assertEquals(32, header.getChunkAuthenticationSize());
        reader.position(item.getOffsetOfChunkHeader());
        assertArrayEquals(
            header.getChunkAuthentication(),
            reader.readChunkHeader(reader.readMarker()).getChunkAuthentication());
      }
      ChunkMetadata last = metadata.get(1);
      ChunkHeader header = reader.readChunkHeader(last.getOffsetOfChunkHeader());
      dataEnd = last.getOffsetOfChunkHeader() + header.getSerializedSize() + header.getDataSize();
    }
    try (TsFileReader reader = new TsFileReader(file)) {
      QueryDataSet data =
          reader.query(
              QueryExpression.create(Collections.singletonList(new Path("d1", "s1", true)), null));
      int points = 0;
      while (data.hasNext()) {
        assertEquals(++points, data.next().getTimestamp());
      }
      assertEquals(2, points);
    }
    // Keep the separator so recovery knows the final chunk group is complete.
    Files.write(file.toPath(), Arrays.copyOf(Files.readAllBytes(file.toPath()), (int) dataEnd + 1));
    try (RestorableTsFileIOWriter recovered = new RestorableTsFileIOWriter(file)) {
      assertTrue(recovered.canWrite());
      assertEquals(dataEnd, file.length());
    }
  }

  @Test
  public void test32ByteWholeChunkSwapFailsAuthentication() throws Exception {
    File file = writeFile(true);
    List<ChunkMetadata> metadata = metadata(file);
    swapChunks(file, metadata);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      assertThrows(
          EncryptException.class,
          () -> reader.readChunkHeader(metadata.get(0).getOffsetOfChunkHeader()));
      reader.position(metadata.get(1).getOffsetOfChunkHeader());
      byte marker = reader.readMarker();
      assertThrows(EncryptException.class, () -> reader.readChunkHeader(marker));
    }
  }

  @Test
  public void testAlteredProofLengthCannotBeRecoveredAsTruncatedTail() throws Exception {
    File file = writeFile(true);
    List<ChunkMetadata> metadata = metadata(file);
    int sizeOffset;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      long offset = metadata.get(0).getOffsetOfChunkHeader();
      ChunkHeader header = reader.readChunkHeader(offset);
      sizeOffset = (int) offset + header.getSerializedSize() - 32 - Integer.BYTES;
    }
    byte[] bytes = Arrays.copyOf(Files.readAllBytes(file.toPath()), sizeOffset + Integer.BYTES);
    ByteBuffer.wrap(bytes).putInt(sizeOffset, 33);
    Files.write(file.toPath(), bytes);
    IOException failure = assertThrows(IOException.class, () -> new RestorableTsFileIOWriter(file));
    assertTrue(EncryptException.isEncryptionFailure(failure));
    assertArrayEquals(bytes, Files.readAllBytes(file.toPath()));
  }

  @Test
  public void testFileRenameKeepsPositionsValid() throws Exception {
    File original = writeFile();
    File renamed = new File(folder.getRoot(), "renamed.tsfile");
    Files.move(original.toPath(), renamed.toPath());
    try (TsFileSequenceReader reader = new TsFileSequenceReader(renamed.getPath())) {
      for (ChunkMetadata chunk : reader.getChunkMetadataList(new Path("d1", "s1", true))) {
        reader.readChunkHeader(chunk.getOffsetOfChunkHeader());
      }
    }
  }

  private File writeFile() throws Exception {
    return writeFile(false);
  }

  private File writeFile(boolean macProof) throws Exception {
    File file = new File(folder.getRoot(), "positions" + macProof + ".tsfile");
    try (EncryptParameter parameter =
            macProof
                ? TestAeadEncryptionProvider.createMacParameter(new byte[16], new byte[16])
                : parameter();
        TsFileWriter writer = new TsFileWriter(file, parameter)) {
      writer.registerTimeseries(
          new Path("d1"),
          new MeasurementSchema(
              "s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 11)));
      writer.flush();
      writer.getIOWriter().writePlanIndices();
      writer.writeRecord(new TSRecord("d1", 2).addTuple(new LongDataPoint("s1", 22)));
      writer.flush();
    }
    return file;
  }

  private List<ChunkMetadata> metadata(File file) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      List<ChunkMetadata> result = reader.getChunkMetadataList(new Path("d1", "s1", true));
      assertEquals(2, result.size());
      return result;
    }
  }

  private void swapChunks(File file, List<ChunkMetadata> metadata) throws IOException {
    int length;
    int first = Math.toIntExact(metadata.get(0).getOffsetOfChunkHeader());
    int second = Math.toIntExact(metadata.get(1).getOffsetOfChunkHeader());
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      ChunkHeader a = reader.readChunkHeader((long) first);
      ChunkHeader b = reader.readChunkHeader((long) second);
      length = a.getSerializedSize() + a.getDataSize();
      assertEquals(length, b.getSerializedSize() + b.getDataSize());
    }
    byte[] bytes = Files.readAllBytes(file.toPath());
    byte[] saved = Arrays.copyOfRange(bytes, first, first + length);
    System.arraycopy(bytes, second, bytes, first, length);
    System.arraycopy(saved, 0, bytes, second, length);
    Files.write(file.toPath(), bytes);
  }

  private EncryptParameter parameter() {
    return TestAeadEncryptionProvider.createParameter(new byte[16], new byte[16]);
  }

  private static class CloseableInput extends LocalTsFileInput implements AutoCloseable {
    private CloseableInput(File file) throws IOException {
      super(file.toPath());
    }
  }
}
