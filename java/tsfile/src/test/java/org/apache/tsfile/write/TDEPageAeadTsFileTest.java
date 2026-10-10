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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptionProviderRegistry;
import org.apache.tsfile.encrypt.TestAeadEncryptionProvider;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.BufferedTsFileInput;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.TsFileLastReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.TimeChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TDEPageAeadTsFileTest {

  private final File file =
      new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "tde-page-aead.tsfile");

  @BeforeClass
  public static void setUpEncryptionProvider() {
    EncryptionProviderRegistry.registerProvider(TestAeadEncryptionProvider.INSTANCE);
  }

  @AfterClass
  public static void tearDownEncryptionProvider() {
    EncryptionProviderRegistry.unregisterProvider(TestAeadEncryptionProvider.PROVIDER_ID);
  }

  @Before
  public void setUp() {
    if (!file.getParentFile().exists()) {
      assertTrue(file.getParentFile().mkdirs());
    }
  }

  @After
  public void tearDown() {
    if (file.exists()) {
      assertTrue(file.delete());
    }
  }

  @Test
  public void testAlignedMultiPageReadWrite() throws Exception {
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int previousMaxPointsInPage = config.getMaxNumberOfPointsInPage();
    config.setMaxNumberOfPointsInPage(1);
    byte[] fileCryptoId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    fileCryptoId[0] = 1;
    EncryptParameter encryptParameter =
        TestAeadEncryptionProvider.createParameter(new byte[16], fileCryptoId);

    try {
      List<IMeasurementSchema> schemas =
          Arrays.asList(
              new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE),
              new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      try (TsFileWriter writer = new TsFileWriter(file, encryptParameter)) {
        writer.registerAlignedTimeseries(new Path("d1"), schemas);
        for (int i = 1; i <= 3; i++) {
          writer.writeRecord(
              new TSRecord("d1", i)
                  .addTuple(new LongDataPoint("s1", i * 10L))
                  .addTuple(new LongDataPoint("s2", i * 100L)));
        }
      }

      try (TsFileReader reader = new TsFileReader(new TsFileSequenceReader(file.getPath()))) {
        QueryDataSet dataSet =
            reader.query(
                QueryExpression.create(
                    Arrays.asList(new Path("d1", "s1", true), new Path("d1", "s2", true)), null));
        for (int i = 1; i <= 3; i++) {
          RowRecord record = dataSet.next();
          assertEquals(i, record.getTimestamp());
          assertEquals(i * 10L, record.getFields().get(0).getLongV());
          assertEquals(i * 100L, record.getFields().get(1).getLongV());
        }
        assertFalse(dataSet.hasNext());
      }
    } finally {
      encryptParameter.close();
      config.setMaxNumberOfPointsInPage(previousMaxPointsInPage);
    }
  }

  @Test
  public void testBufferedInputConstructorLoadsEncryptionHeader() throws Exception {
    byte[] fileCryptoId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    fileCryptoId[0] = 1;
    EncryptParameter encryptParameter =
        TestAeadEncryptionProvider.createParameter(new byte[16], fileCryptoId);

    try {
      try (TsFileWriter writer = new TsFileWriter(file, encryptParameter)) {
        writer.registerTimeseries(
            new Path("d1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
        writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 1L)));
      }

      try (TsFileSequenceReader reader =
          new TsFileSequenceReader(new BufferedTsFileInput(file.toPath()), false, false, null)) {
        reader.position(reader.getDataStartOffset());
        assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
      }
    } finally {
      encryptParameter.close();
    }
  }

  @Test
  public void testMetadataOffsetConstructorLoadsEncryptionHeader() throws Exception {
    try (EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH])) {
      try (TsFileWriter writer = new TsFileWriter(file, parameter)) {
        writer.registerTimeseries(
            new Path("d1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
        writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 11L)));
      }
      try (TsFileSequenceReader original = new TsFileSequenceReader(file.getPath())) {
        ChunkMetadata metadata = original.getChunkMetadataList(new Path("d1", "s1", true)).get(0);
        try (TsFileSequenceReader reader =
            new TsFileSequenceReader(
                new BufferedTsFileInput(file.toPath()),
                original.getFileMetadataPos(),
                original.getTsFileMetadataSize())) {
          assertTrue(reader.hasFileEncryptionHeader());
          original.position(metadata.getOffsetOfChunkHeader());
          reader.position(metadata.getOffsetOfChunkHeader());
          assertEquals(
              original.readChunkHeader(original.readMarker()).getChunkOrdinal(),
              reader.readChunkHeader(reader.readMarker()).getChunkOrdinal());
        }
      }
    }
  }

  @Test
  public void testMaterializedChunkSurvivesReaderClose() throws Exception {
    try (EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH])) {
      try (TsFileWriter writer = new TsFileWriter(file, parameter)) {
        writer.registerTimeseries(
            new Path("d1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
        writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 11L)));
      }
      Chunk chunk;
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
        chunk = reader.readMemChunk(reader.getChunkMetadataList(new Path("d1", "s1", true)).get(0));
      }
      try (Chunk ownedChunk = chunk) {
        BatchData data = new ChunkReader(ownedChunk).nextPageData();
        IPointReader points = data.getBatchDataIterator();
        assertTrue(points.hasNextTimeValuePair());
        assertEquals(11L, points.nextTimeValuePair().getValue().getLong());
      }
      assertTrue(chunk.getEncryptParam().isDestroyed());
      chunk.getData().rewind();
      assertThrows(EncryptException.class, () -> new ChunkReader(chunk).nextPageData());
    }
  }

  @Test
  public void testAlignedBlobLastPointDecryptsPage() throws Exception {
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int previousMaxPointsInPage = config.getMaxNumberOfPointsInPage();
    config.setMaxNumberOfPointsInPage(1);
    try (EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH])) {
      List<IMeasurementSchema> schemas =
          Arrays.asList(new MeasurementSchema("s1", TSDataType.BLOB, TSEncoding.PLAIN));
      try (TsFileWriter writer = new TsFileWriter(file, parameter)) {
        writer.registerAlignedTimeseries(new Path("d1"), schemas);
        Tablet tablet = new Tablet("d1", schemas, 2);
        for (int i = 0; i < 2; i++) {
          tablet.addTimestamp(i, i + 1);
          tablet.addValue(i, 0, ("value-" + i).getBytes(StandardCharsets.UTF_8));
        }
        writer.writeTree(tablet);
      }
      try (TsFileLastReader reader = new TsFileLastReader(file.getPath(), true, false)) {
        Pair<?, List<Pair<String, TimeValuePair>>> last = reader.next();
        assertEquals("s1", last.right.get(1).left);
        assertEquals(2L, last.right.get(1).right.getTimestamp());
        assertEquals(
            new Binary("value-1", StandardCharsets.UTF_8),
            last.right.get(1).right.getValue().getBinary());
      }
    } finally {
      config.setMaxNumberOfPointsInPage(previousMaxPointsInPage);
    }
  }

  @Test
  public void testUnencryptedWriterRejectsChunkWithOrdinal() throws Exception {
    try (TsFileIOWriter writer = new TsFileIOWriter(file)) {
      ChunkHeader header =
          new ChunkHeader(
              "s1", 0, TSDataType.INT64, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 1, 0, 0);
      Chunk chunk = new Chunk(header, ByteBuffer.allocate(0));
      assertThrows(IOException.class, () -> writer.writeChunk(chunk));
      assertThrows(IOException.class, () -> writer.writeChunk(chunk, null));
    }
  }

  @Test
  public void testLegacyReadPageOverloadsRejectPageAeadBeforeReading() throws Exception {
    EncryptParameter encryptParameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      try (TsFileWriter writer = new TsFileWriter(file, encryptParameter)) {
        writer.registerTimeseries(
            new Path("d1"),
            new MeasurementSchema(
                "s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 11L)));
      }

      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
        ChunkMetadata metadata = reader.getChunkMetadataList(new Path("d1", "s1", true)).get(0);
        reader.position(metadata.getOffsetOfChunkHeader());
        ChunkHeader chunkHeader = reader.readChunkHeader(reader.readMarker());
        PageHeader pageHeader = reader.readPageHeader(chunkHeader.getDataType(), false);
        long pageBodyOffset = reader.position();

        IOException noPageIndex =
            assertThrows(
                IOException.class,
                () -> reader.readPage(pageHeader, chunkHeader.getCompressionType()));
        assertTrue(noPageIndex.getMessage().contains("chunkOrdinal"));
        assertEquals(pageBodyOffset, reader.position());

        IOException noChunkOrdinal =
            assertThrows(
                IOException.class,
                () -> reader.readPage(pageHeader, chunkHeader.getCompressionType(), 0));
        assertTrue(noChunkOrdinal.getMessage().contains("chunkOrdinal"));
        assertEquals(pageBodyOffset, reader.position());

        assertTrue(
            reader
                    .readPage(
                        pageHeader,
                        chunkHeader.getCompressionType(),
                        0,
                        chunkHeader.getChunkOrdinal())
                    .remaining()
                > 0);
      }
    } finally {
      encryptParameter.close();
    }
  }

  @Test
  public void testChunkWriterReuseAllocatesDistinctOrdinals() throws Exception {
    EncryptParameter encryptParameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      try (TsFileWriter writer = new TsFileWriter(file, encryptParameter)) {
        writer.registerTimeseries(
            new Path("d1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
        writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 11L)));
        writer.flush();
        writer.writeRecord(new TSRecord("d1", 2).addTuple(new LongDataPoint("s1", 22L)));
      }

      try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(file.getPath());
          TsFileReader reader = new TsFileReader(sequenceReader)) {
        List<ChunkMetadata> chunks =
            sequenceReader.getChunkMetadataList(new Path("d1", "s1", true));
        assertEquals(2, chunks.size());
        long firstOrdinal =
            sequenceReader.readMemChunk(chunks.get(0)).getHeader().getChunkOrdinal();
        long secondOrdinal =
            sequenceReader.readMemChunk(chunks.get(1)).getHeader().getChunkOrdinal();
        assertTrue(firstOrdinal >= 0);
        assertNotEquals(firstOrdinal, secondOrdinal);

        QueryDataSet dataSet =
            reader.query(QueryExpression.create(Arrays.asList(new Path("d1", "s1", true)), null));
        assertEquals(11L, dataSet.next().getFields().get(0).getLongV());
        assertEquals(22L, dataSet.next().getFields().get(0).getLongV());
        assertFalse(dataSet.hasNext());
      }
    } finally {
      encryptParameter.close();
    }
  }

  @Test
  public void testEncryptedChunkRewriteUsesNewOrdinal() throws Exception {
    EncryptParameter encryptParameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      try (TsFileWriter writer = new TsFileWriter(file, encryptParameter)) {
        writer.registerTimeseries(
            new Path("d1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
        writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 11L)));
      }

      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
        Chunk source =
            reader.readMemChunk(reader.getChunkMetadataList(new Path("d1", "s1", true)).get(0));
        Chunk rewritten = source.rewrite(TSDataType.DOUBLE);
        assertNotEquals(
            source.getHeader().getChunkOrdinal(), rewritten.getHeader().getChunkOrdinal());
        for (IPageReader page : new ChunkReader(rewritten).loadPageReaderList()) {
          BatchData data = page.getAllSatisfiedPageData(true);
          IPointReader points = data.getBatchDataIterator();
          assertTrue(points.hasNextTimeValuePair());
          TimeValuePair point = points.nextTimeValuePair();
          assertEquals(1L, point.getTimestamp());
          assertEquals(11.0, point.getValue().getDouble(), 0.0);
          assertFalse(points.hasNextTimeValuePair());
        }
      }
    } finally {
      encryptParameter.close();
    }
  }

  @Test
  public void testSwappingEqualSizedPagesBetweenChunksFailsAuthentication() throws Exception {
    EncryptParameter encryptParameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      try (TsFileWriter writer = new TsFileWriter(file, encryptParameter)) {
        writer.registerTimeseries(
            new Path("d1"),
            new MeasurementSchema(
                "s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
        writer.writeRecord(new TSRecord("d1", 1).addTuple(new LongDataPoint("s1", 11L)));
        writer.flush();
        writer.writeRecord(new TSRecord("d1", 2).addTuple(new LongDataPoint("s1", 22L)));
      }

      long[] pageBodyOffsets = new long[2];
      int[] pageBodySizes = new int[2];
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
        List<ChunkMetadata> chunks = reader.getChunkMetadataList(new Path("d1", "s1", true));
        assertEquals(2, chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
          Chunk chunk = reader.readMemChunk(chunks.get(i));
          ByteBuffer chunkData = chunk.getData();
          PageHeader pageHeader = PageHeader.deserializeFrom(chunkData, chunk.getChunkStatistic());
          pageBodyOffsets[i] =
              chunks.get(i).getOffsetOfChunkHeader()
                  + chunk.getHeader().getSerializedSize()
                  + chunkData.position();
          pageBodySizes[i] = pageHeader.getCompressedSize();
        }
      }
      assertEquals(pageBodySizes[0], pageBodySizes[1]);
      try (RandomAccessFile data = new RandomAccessFile(file, "rw")) {
        byte[] firstPage = new byte[pageBodySizes[0]];
        byte[] secondPage = new byte[pageBodySizes[1]];
        data.seek(pageBodyOffsets[0]);
        data.readFully(firstPage);
        data.seek(pageBodyOffsets[1]);
        data.readFully(secondPage);
        data.seek(pageBodyOffsets[0]);
        data.write(secondPage);
        data.seek(pageBodyOffsets[1]);
        data.write(firstPage);
      }

      try (TsFileReader reader = new TsFileReader(new TsFileSequenceReader(file.getPath()))) {
        assertThrows(
            Exception.class,
            () ->
                reader
                    .query(QueryExpression.create(Arrays.asList(new Path("d1", "s1", true)), null))
                    .next());
      }
    } finally {
      encryptParameter.close();
    }
  }

  @Test
  public void testRejectUnsafeEncryptedChunkReuse() throws Exception {
    File targetFile = new File(file.getPath() + ".target");
    byte[] sourceFileCryptoId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    byte[] targetFileCryptoId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    sourceFileCryptoId[0] = 1;
    targetFileCryptoId[0] = 2;
    EncryptParameter sourceParameter =
        TestAeadEncryptionProvider.createParameter(new byte[16], sourceFileCryptoId);
    EncryptParameter targetParameter =
        TestAeadEncryptionProvider.createParameter(new byte[16], targetFileCryptoId);

    try (TsFileIOWriter writer = new TsFileIOWriter(targetFile, targetParameter)) {
      ChunkHeader header =
          new ChunkHeader(
              "s1", 0, TSDataType.INT64, CompressionType.UNCOMPRESSED, TSEncoding.PLAIN, 1);
      Chunk sourceChunk = new Chunk(header, ByteBuffer.allocate(0), sourceParameter);

      assertThrows(IOException.class, () -> writer.writeChunk(sourceChunk));
      Chunk sameContextChunk = new Chunk(header, ByteBuffer.allocate(0), targetParameter);
      assertThrows(IOException.class, () -> writer.writeChunk(sameContextChunk));
      assertThrows(IOException.class, () -> sourceChunk.mergeChunkByAppendPage(sourceChunk));
    } finally {
      sourceParameter.close();
      targetParameter.close();
      if (targetFile.exists()) {
        assertTrue(targetFile.delete());
      }
    }
  }

  @Test
  public void testFileEncryptionContextCannotChangeAfterHeader() throws Exception {
    EncryptParameter first =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    byte[] secondFileId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    secondFileId[0] = 1;
    EncryptParameter second =
        TestAeadEncryptionProvider.createParameter(new byte[16], secondFileId);
    try (TsFileIOWriter writer = new TsFileIOWriter(file, first);
        EncryptParameter firstCopy = first.copy()) {
      writer.setEncryptParam(first);
      assertThrows(EncryptException.class, () -> writer.setEncryptParam(second));
      assertThrows(EncryptException.class, () -> writer.setEncryptParam(firstCopy));
      assertThrows(
          EncryptException.class,
          () -> writer.setEncryptParam("0", "org.apache.tsfile.encrypt.UNENCRYPTED", null));
      assertTrue(writer.getEncryptParameter() == first);
    } finally {
      first.close();
      second.close();
    }
  }

  @Test
  public void testChunkFromDifferentFileContextIsRejectedBeforeWriting() throws Exception {
    EncryptParameter target =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    byte[] sourceFileId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    sourceFileId[0] = 1;
    EncryptParameter source =
        TestAeadEncryptionProvider.createParameter(new byte[16], sourceFileId);
    try (TsFileIOWriter writer = new TsFileIOWriter(file, target)) {
      ChunkWriterImpl chunk =
          new ChunkWriterImpl(new MeasurementSchema("s1", TSDataType.INT64), source);
      chunk.write(1, 11L);
      long position = writer.getPos();
      assertThrows(IOException.class, () -> chunk.writeToFileWriter(writer));
      assertEquals(position, writer.getPos());

      TimeChunkWriter timeChunk =
          new TimeChunkWriter(
              "",
              CompressionType.UNCOMPRESSED,
              TSEncoding.PLAIN,
              new PlainEncoder(TSDataType.INT64, 0),
              source);
      timeChunk.write(1);
      timeChunk.sealCurrentPage();
      assertThrows(IOException.class, () -> timeChunk.writeAllPagesOfChunkToTsFile(writer));
      assertEquals(position, writer.getPos());

      ValueChunkWriter valueChunk =
          new ValueChunkWriter(
              "s1",
              CompressionType.UNCOMPRESSED,
              TSDataType.INT64,
              TSEncoding.PLAIN,
              new PlainEncoder(TSDataType.INT64, 0),
              source);
      valueChunk.write(1, 11L, false);
      valueChunk.sealCurrentPage();
      assertThrows(IOException.class, () -> valueChunk.writeAllPagesOfChunkToTsFile(writer, null));
      assertEquals(position, writer.getPos());

      assertThrows(
          IOException.class,
          () ->
              writer.startFlushChunk(
                  "s1",
                  CompressionType.UNCOMPRESSED,
                  TSDataType.INT64,
                  TSEncoding.PLAIN,
                  null,
                  0,
                  0,
                  0,
                  0));
      assertEquals(position, writer.getPos());
    } finally {
      source.close();
      target.close();
    }
  }
}
