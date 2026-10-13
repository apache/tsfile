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

import java.io.ByteArrayOutputStream;
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
      assertEquals(ChunkCryptoContext.AUTHENTICATION_SIZE, proof.length);
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
      ByteBuffer buffer = ByteBuffer.allocate(header.getSerializedSize());
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
                  - ChunkCryptoContext.AUTHENTICATION_SIZE
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
    File file = new File(folder.getRoot(), "positions.tsfile");
    try (EncryptParameter parameter = parameter();
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
}
