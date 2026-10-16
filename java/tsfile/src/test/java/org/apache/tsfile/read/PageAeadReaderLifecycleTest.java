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

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptionProviderRegistry;
import org.apache.tsfile.encrypt.TestAeadEncryptionProvider;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.dataset.TreeResultSet;
import org.apache.tsfile.read.reader.BufferedTsFileInput;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.read.reader.series.FileSeriesReader;
import org.apache.tsfile.read.reader.series.FileSeriesReaderByTimestamp;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PageAeadReaderLifecycleTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void registerProvider() {
    EncryptionProviderRegistry.registerProvider(TestAeadEncryptionProvider.INSTANCE);
  }

  @AfterClass
  public static void unregisterProvider() {
    EncryptionProviderRegistry.unregisterProvider(TestAeadEncryptionProvider.PROVIDER_ID);
  }

  @Test
  public void testPlaintextQueriesAreNotStronglyRetained() throws Exception {
    File file = new File(temporaryFolder.getRoot(), "plain.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path("d1"), new MeasurementSchema("s1", TSDataType.INT64));
      for (int i = 1; i <= 3; i++) {
        writer.writeRecord(new TSRecord("d1", i).addTuple(new LongDataPoint("s1", i)));
      }
    }
    try (TsFileReader reader = new TsFileReader(file)) {
      for (int i = 0; i < 100; i++) {
        QueryDataSet query = reader.query(queryExpression());
        assertTrue(query.hasNext());
        query.next();
      }
      java.lang.reflect.Field field = TsFileReader.class.getDeclaredField("queryDataSets");
      field.setAccessible(true);
      assertTrue(((java.util.Set<?>) field.get(reader)).isEmpty());
    }
  }

  @Test
  public void testSeriesScanReleasesEachChunkAndCreatesOneProvider() throws Exception {
    File file = writeFile(false);
    int createCount = TestAeadEncryptionProvider.getCreateCount();
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      FileSeriesReader reader =
          new FileSeriesReader(
              loader,
              new ArrayList<>(sequence.getChunkMetadataList(new Path("d1", "s1", true))),
              null);
      try {
        int count = 0;
        while (reader.hasNextBatch()) {
          assertEquals(++count * 10L, reader.nextBatch().currentValue());
        }
        assertEquals(3, count);
        assertEquals(3, loader.loaded.size());
        assertAllDestroyed(loader.loaded);
        assertEquals(createCount + 1, TestAeadEncryptionProvider.getCreateCount());
      } finally {
        reader.close();
      }
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testTimestampReaderReleasesChunksOnSwitchAndEarlyClose() throws Exception {
    File file = writeFile(false);
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      try (FileSeriesReaderByTimestamp reader =
          new FileSeriesReaderByTimestamp(
              loader, new ArrayList<>(sequence.getChunkMetadataList(new Path("d1", "s1", true))))) {
        assertEquals(10L, reader.getValueInTimestamp(1));
        assertFalse(loader.loaded.get(0).getEncryptParam().isDestroyed());
        assertEquals(20L, reader.getValueInTimestamp(2));
        assertTrue(loader.loaded.get(0).getEncryptParam().isDestroyed());
      }
      assertAllDestroyed(loader.loaded);
    }
  }

  @Test
  public void testTimestampReaderPreservesLoadFailureAndClosesRemainingChunks() throws Exception {
    IChunkLoader loader = mock(IChunkLoader.class);
    AlignedChunkMetadata aligned = mock(AlignedChunkMetadata.class);
    ChunkMetadata timeMetadata = mock(ChunkMetadata.class);
    ChunkMetadata firstMetadata = mock(ChunkMetadata.class);
    ChunkMetadata secondMetadata = mock(ChunkMetadata.class);
    Chunk timeChunk = mock(Chunk.class);
    Chunk firstChunk = mock(Chunk.class);
    IOException loadFailure = new IOException("load failed");
    IllegalStateException closeFailure = new IllegalStateException("close failed");
    when(aligned.getEndTime()).thenReturn(Long.MAX_VALUE);
    when(aligned.getTimeChunkMetadata()).thenReturn(timeMetadata);
    when(aligned.getValueChunkMetadataList())
        .thenReturn(Arrays.asList(firstMetadata, secondMetadata));
    when(loader.loadChunk(timeMetadata)).thenReturn(timeChunk);
    when(loader.loadChunk(firstMetadata)).thenReturn(firstChunk);
    when(loader.loadChunk(secondMetadata)).thenThrow(loadFailure);
    doThrow(closeFailure).when(timeChunk).close();
    try (FileSeriesReaderByTimestamp reader =
        new FileSeriesReaderByTimestamp(loader, Arrays.asList(aligned))) {
      IOException actual = assertThrows(IOException.class, () -> reader.getValueInTimestamp(1));
      assertSame(loadFailure, actual);
      assertEquals(1, actual.getSuppressed().length);
      assertSame(closeFailure, actual.getSuppressed()[0]);
      verify(timeChunk).close();
      verify(firstChunk).close();
    }
  }

  @Test
  public void testCachedChunkReaderSurvivesFileCloseAndReleasesProvider() throws Exception {
    File file = writeFile(false);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    IChunkReader chunkReader;
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      CachedChunkLoaderImpl loader = new CachedChunkLoaderImpl(sequence);
      chunkReader =
          loader.getChunkReader(
              sequence.getChunkMetadataList(new Path("d1", "s1", true)).get(0), null);
    }
    try {
      assertEquals(10L, chunkReader.nextPageData().currentValue());
      assertEquals(closeCount, TestAeadEncryptionProvider.getCloseCount());
    } finally {
      chunkReader.close();
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testAlignedReaderClosesTimeAndAllValueContexts() throws Exception {
    File file = writeFile(true);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    Chunk time;
    List<Chunk> values = new ArrayList<>();
    AlignedChunkReader reader;
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      Map<String, List<ChunkMetadata>> metadata =
          sequence.readChunkMetadataInDevice(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"));
      time = sequence.readMemChunk(metadata.get("").get(0));
      values.add(sequence.readMemChunk(metadata.get("s1").get(0)));
      values.add(sequence.readMemChunk(metadata.get("s2").get(0)));
      values.add(null);
      reader = new AlignedChunkReader(time, values);
    }
    try {
      assertTrue(reader.hasNextSatisfiedPage());
      reader.nextPageData();
    } finally {
      reader.close();
      reader.close();
    }
    assertTrue(time.getEncryptParam().isDestroyed());
    assertAllDestroyed(values);
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testMalformedChunkReaderConstructionReleasesContext() throws Exception {
    File file = writeFile(false);
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath());
        Chunk chunk =
            sequence.readMemChunk(
                sequence.getChunkMetadataList(new Path("d1", "s1", true)).get(0))) {
      chunk.getData().limit(2);
      assertThrows(RuntimeException.class, () -> new ChunkReader(chunk));
      assertTrue(chunk.getEncryptParam().isDestroyed());
    }
  }

  @Test
  public void testMalformedAlignedReaderConstructionReleasesAllContexts() throws Exception {
    File file = writeFile(true);
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      Map<String, List<ChunkMetadata>> metadata =
          sequence.readChunkMetadataInDevice(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"));
      try (Chunk time = sequence.readMemChunk(metadata.get("").get(0));
          Chunk first = sequence.readMemChunk(metadata.get("s1").get(0));
          Chunk second = sequence.readMemChunk(metadata.get("s2").get(0))) {
        time.getData().limit(2);
        assertThrows(
            IOException.class,
            () -> new AlignedChunkReader(time, Arrays.asList(first, second, null)));
        assertAllDestroyed(Arrays.asList(time, first, second));
      }
    }
  }

  @Test
  public void testTreeResultSetEarlyCloseReleasesQueryWithoutClosingFile() throws Exception {
    File file = writeFile(false);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileReader reader = new TsFileReader(file)) {
      QueryDataSet query = reader.query(queryExpression());
      TreeResultSet result = new TreeResultSet(query, Arrays.asList("d1"), Arrays.asList("s1"));
      assertTrue(result.next());
      result.close();
      result.close();
      assertTrue(query.isClosed());
      assertFalse(query.hasNext());
      try (QueryDataSet another = reader.query(queryExpression())) {
        assertTrue(another.hasNext());
        assertEquals(10L, another.next().getFields().get(0).getLongV());
      }
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testFileReaderClosesAnAbandonedQuery() throws Exception {
    File file = writeFile(false);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    QueryDataSet query;
    try (TsFileReader reader = new TsFileReader(file)) {
      query = reader.query(queryExpression());
      assertTrue(query.hasNext());
    }
    assertTrue(query.isClosed());
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testQueryLimitClosesRemainingChunkReferences() throws Exception {
    File file = writeFile(false);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileReader reader = new TsFileReader(file)) {
      QueryDataSet query = reader.query(queryExpression());
      query.setRowLimit(1);
      assertTrue(query.hasNext());
      assertEquals(10L, query.next().getFields().get(0).getLongV());
      assertFalse(query.hasNext());
      assertTrue(query.isClosed());
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testAlignedIteratorFailureClosesPreviouslyLoadedChunks() throws Exception {
    File file = writeFile(true);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (FailingSequenceReader sequence = new FailingSequenceReader(file)) {
      sequence.getEncryptParam().getPageBodyOverhead();
      Map<String, List<ChunkMetadata>> metadata =
          sequence.readChunkMetadataInDevice(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"));
      AlignedChunkMetadata aligned =
          new AlignedChunkMetadata(
              metadata.get("").get(0),
              Arrays.asList(metadata.get("s1").get(0), metadata.get("s2").get(0)));
      TsFileAlignedSeriesReaderIterator iterator =
          new TsFileAlignedSeriesReaderIterator(
              sequence,
              Arrays.asList(aligned),
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN),
                  new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN)));
      assertThrows(IOException.class, iterator::nextReader);
      assertEquals(2, sequence.loaded.size());
      assertAllDestroyed(sequence.loaded);
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testAlignedRewriteReleasesTemporaryReferencesAndKeepsSources() throws Exception {
    File file = writeFile(true);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      Map<String, List<ChunkMetadata>> metadata =
          sequence.readChunkMetadataInDevice(IDeviceID.Factory.DEFAULT_FACTORY.create("d1"));
      try (Chunk time = sequence.readMemChunk(metadata.get("").get(0));
          Chunk value = sequence.readMemChunk(metadata.get("s1").get(0));
          Chunk rewritten = value.rewrite(TSDataType.DOUBLE, time)) {
        assertFalse(time.getEncryptParam().isDestroyed());
        assertFalse(value.getEncryptParam().isDestroyed());
        AlignedChunkReader chunkReader = new AlignedChunkReader(time, Arrays.asList(rewritten));
        TsPrimitiveType[] values = (TsPrimitiveType[]) chunkReader.nextPageData().currentValue();
        assertEquals(10.0, values[0].getDouble(), 0.0);
      }
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testMetadataOffsetConstructorsRemainCompatibleAndLoadLazily() throws Exception {
    File file = writeFile(false);
    try (TsFileSequenceReader original = new TsFileSequenceReader(file.getPath())) {
      ChunkMetadata metadata = original.getChunkMetadataList(new Path("d1", "s1", true)).get(0);
      for (boolean withParameter : new boolean[] {false, true}) {
        BufferedTsFileInput input = new BufferedTsFileInput(file.toPath());
        try (TsFileSequenceReader reader =
            metadataOffsetReader(
                input,
                original.getFileMetadataPos(),
                original.getTsFileMetadataSize(),
                withParameter)) {
          reader.position(metadata.getOffsetOfChunkHeader());
          ChunkHeader header = reader.readChunkHeader(reader.readMarker());
          assertTrue(header.getChunkOrdinal() >= 0);
          try (Chunk chunk = reader.readMemChunk(metadata)) {
            assertEquals(10L, new ChunkReader(chunk).nextPageData().currentValue());
          }
        }
      }
    }
  }

  // Deliberately has no throws clause: callers of the old public constructors must still compile.
  private static TsFileSequenceReader metadataOffsetReader(
      BufferedTsFileInput input, long position, int size, boolean withParameter) {
    return withParameter
        ? new TsFileSequenceReader(input, position, size, null)
        : new TsFileSequenceReader(input, position, size);
  }

  private File writeFile(boolean aligned) throws Exception {
    File file = new File(temporaryFolder.getRoot(), "lifecycle.tsfile");
    try (EncryptParameter parameter =
            TestAeadEncryptionProvider.createParameter(
                new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
        TsFileWriter writer = new TsFileWriter(file, parameter)) {
      MeasurementSchema first = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);
      if (aligned) {
        writer.registerAlignedTimeseries(
            new Path("d1"),
            Arrays.asList(first, new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN)));
      } else {
        writer.registerTimeseries(new Path("d1"), first);
      }
      for (int i = 1; i <= 3; i++) {
        TSRecord record = new TSRecord("d1", i).addTuple(new LongDataPoint("s1", i * 10L));
        if (aligned) {
          record.addTuple(new LongDataPoint("s2", i * 100L));
        }
        writer.writeRecord(record);
        writer.flush();
      }
    }
    return file;
  }

  private static QueryExpression queryExpression() {
    return QueryExpression.create(Arrays.asList(new Path("d1", "s1", true)), null);
  }

  private static void assertAllDestroyed(List<Chunk> chunks) {
    for (Chunk chunk : chunks) {
      if (chunk != null) {
        assertTrue(chunk.getEncryptParam().isDestroyed());
      }
    }
  }

  private static class TrackingChunkLoader extends CachedChunkLoaderImpl {
    private final List<Chunk> loaded = new ArrayList<>();

    private TrackingChunkLoader(TsFileSequenceReader reader) {
      super(reader);
    }

    @Override
    public Chunk loadChunk(ChunkMetadata metadata) throws IOException {
      Chunk chunk = super.loadChunk(metadata);
      loaded.add(chunk);
      return chunk;
    }
  }

  private static class FailingSequenceReader extends TsFileSequenceReader {
    private final List<Chunk> loaded = new ArrayList<>();

    private FailingSequenceReader(File file) throws IOException {
      super(file.getPath());
    }

    @Override
    public Chunk readMemChunk(ChunkMetadata metadata) throws IOException {
      if (loaded.size() == 2) {
        throw new IOException("Injected chunk read failure");
      }
      Chunk chunk = super.readMemChunk(metadata);
      loaded.add(chunk);
      return chunk;
    }
  }
}
