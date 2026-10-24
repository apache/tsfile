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
package org.apache.tsfile.read.reader.block;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptionProviderRegistry;
import org.apache.tsfile.encrypt.TestAeadEncryptionProvider;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.dataset.TableResultSet;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.query.executor.TableQueryExecutor.ColumnMapping;
import org.apache.tsfile.read.query.executor.TableQueryExecutor.TableQueryOrdering;
import org.apache.tsfile.read.query.executor.task.DeviceQueryTask;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PageAeadBlockReaderLifecycleTest {
  private static final List<String> TABLE_COLUMNS = Arrays.asList("device_id", "s1", "s2");
  private static final List<TSDataType> TABLE_TYPES =
      Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64);
  private static final List<ColumnCategory> TABLE_CATEGORIES =
      Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);
  private static final IDeviceID TREE_DEVICE = IDeviceID.Factory.DEFAULT_FACTORY.create("d1");

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
  public void testPublicTableQueryPropagatesAuthenticationFailure() throws Exception {
    File file = writeTableFile(1);
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      loader.tamperTimeColumn = true;
      TableQueryExecutor executor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequence), loader, TableQueryOrdering.DEVICE);
      try (TableResultSet result = tableResult(executor)) {
        Exception failure = assertThrows(Exception.class, result::next);
        assertTrue(EncryptException.isEncryptionFailure(failure));
      }
      assertAllDestroyed(loader.loaded);
    }
  }

  @Test
  public void testPublicTableQueryPropagatesLaterValuePageFailure() throws Exception {
    File file = writeTableFile(1);
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      loader.tamperSecondColumn = true;
      TableQueryExecutor executor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequence), loader, TableQueryOrdering.DEVICE);
      try (TableResultSet result = tableResult(executor)) {
        Exception failure =
            assertThrows(
                Exception.class,
                () -> {
                  while (result.next()) {
                    result.getLong("s2");
                  }
                });
        assertTrue(EncryptException.isEncryptionFailure(failure));
      }
      assertAllDestroyed(loader.loaded);
    }
  }

  @Test
  public void testTableResultEarlyCloseReleasesContextsAndKeepsSharedLoaderOpen() throws Exception {
    File file = writeTableFile(1);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      TableQueryExecutor executor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequence), loader, TableQueryOrdering.DEVICE);
      try (TableResultSet result = tableResult(executor)) {
        assertTrue(result.next());
        assertEquals(1L, result.getLong("s1"));
        assertTrue(loader.loaded.stream().anyMatch(c -> !c.getEncryptParam().isDestroyed()));
        result.close();
        result.close();
        assertAllDestroyed(loader.loaded);
        assertEquals(closeCount, TestAeadEncryptionProvider.getCloseCount());
      }
      // A result only owns its chunks, so another query can still use the same file loader.
      try (TableResultSet another = tableResult(executor)) {
        assertTrue(another.next());
        assertEquals(1L, another.getLong("s1"));
      }
      assertAllDestroyed(loader.loaded);
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testDeviceSwitchAndExhaustionReleaseAllContexts() throws Exception {
    File file = writeTableFile(2);
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      TableQueryExecutor executor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequence), loader, TableQueryOrdering.DEVICE);
      try (TableResultSet result = tableResult(executor)) {
        int rows = 0;
        while (result.next()) {
          rows++;
        }
        assertEquals(4096, rows);
        assertAllDestroyed(loader.loaded);
      }
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testSingleDeviceEarlyCloseReleasesEveryScalarColumn() throws Exception {
    File file = writeScalarFile();
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      IMetadataQuerier metadata = scalarMetadata(sequence);
      DeviceQueryTask task = scalarTask(metadata, loader);
      try (SingleDeviceTsBlockReader reader =
          new SingleDeviceTsBlockReader(task, metadata, loader, 1, null, null)) {
        assertTrue(reader.hasNext());
        assertEquals(1, reader.next().getPositionCount());
        reader.close();
        reader.close();
        assertFalse(reader.hasNext());
        assertEquals(2, loader.loaded.size());
        assertAllDestroyed(loader.loaded);
        assertTrue(sequence.fileSize() > 0);
      }
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testLaterColumnLoadFailureReleasesPreviouslyInitializedColumn() throws Exception {
    File file = writeScalarFile();
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      IOException failure = new IOException("Injected column load failure");
      loader.loadFailure = failure;
      IMetadataQuerier metadata = scalarMetadata(sequence);
      DeviceQueryTask task = scalarTask(metadata, loader);
      assertSame(
          failure,
          assertThrows(
              IOException.class,
              () -> new SingleDeviceTsBlockReader(task, metadata, loader, 1, null, null)));
      assertEquals(1, loader.loaded.size());
      assertAllDestroyed(loader.loaded);
      assertTrue(sequence.fileSize() > 0);
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testLaterColumnDecodeFailureAlsoReleasesTheUnregisteredReader() throws Exception {
    File file = writeScalarFile();
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      loader.tamperSecondColumn = true;
      IMetadataQuerier metadata = scalarMetadata(sequence);
      DeviceQueryTask task = scalarTask(metadata, loader);
      assertThrows(
          EncryptException.class,
          () -> new SingleDeviceTsBlockReader(task, metadata, loader, 1, null, null));
      assertEquals(2, loader.loaded.size());
      assertAllDestroyed(loader.loaded);
      assertTrue(sequence.fileSize() > 0);
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testRuntimeColumnInitializationFailurePreservesCauseAndReleasesReaders()
      throws Exception {
    File file = writeScalarFile();
    int closeCount = TestAeadEncryptionProvider.getCloseCount();
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getPath())) {
      TrackingChunkLoader loader = new TrackingChunkLoader(sequence);
      IMetadataQuerier metadata = scalarMetadata(sequence);
      DeviceQueryTask original = scalarTask(metadata, loader);
      ColumnMapping mapping = spy(original.getColumnMapping());
      IllegalStateException failure = new IllegalStateException("Injected column mapping failure");
      doThrow(failure).when(mapping).getColumnPos("s2");
      DeviceQueryTask task =
          new DeviceQueryTask(
              TREE_DEVICE, original.getColumnNames(), mapping, null, original.getTableSchema());
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () -> new SingleDeviceTsBlockReader(task, metadata, loader, 1, null, null)));
      assertEquals(2, loader.loaded.size());
      assertAllDestroyed(loader.loaded);
    }
    assertEquals(closeCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  private TableResultSet tableResult(TableQueryExecutor executor) throws Exception {
    return new TableResultSet(
        executor.query("t1", TABLE_COLUMNS, null, null, null), TABLE_COLUMNS, TABLE_TYPES, "t1");
  }

  private File writeTableFile(int devices) throws Exception {
    File file = new File(temporaryFolder.getRoot(), "table-lifecycle.tsfile");
    try (EncryptParameter parameter =
            TestAeadEncryptionProvider.createParameter(
                new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
        TsFileWriter writer = new TsFileWriter(file, parameter)) {
      writer.setGenerateTableSchema(true);
      writer.registerTableSchema(
          new TableSchema(
              "t1",
              Arrays.asList(
                  new MeasurementSchema("device_id", TSDataType.STRING),
                  new MeasurementSchema("s1", TSDataType.INT64),
                  new MeasurementSchema("s2", TSDataType.INT64)),
              TABLE_CATEGORIES));
      for (int device = 0; device < devices; device++) {
        Tablet tablet = new Tablet("t1", TABLE_COLUMNS, TABLE_TYPES, TABLE_CATEGORIES, 2048);
        for (int row = 0; row < 2048; row++) {
          tablet.addTimestamp(row, row + 1);
          tablet.addValue("device_id", row, "device-" + device);
          tablet.addValue("s1", row, (long) row + 1);
          tablet.addValue("s2", row, (long) row + 101);
        }
        writer.writeTable(tablet);
      }
    }
    return file;
  }

  private File writeScalarFile() throws Exception {
    File file = new File(temporaryFolder.getRoot(), "scalar-columns.tsfile");
    try (EncryptParameter parameter =
            TestAeadEncryptionProvider.createParameter(
                new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
        TsFileWriter writer = new TsFileWriter(file, parameter)) {
      writer.registerTimeseries(
          new Path("d1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
      writer.registerTimeseries(
          new Path("d1"), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
      for (int row = 1; row <= 3; row++) {
        writer.writeRecord(
            new TSRecord("d1", row)
                .addTuple(new LongDataPoint("s1", row))
                .addTuple(new LongDataPoint("s2", row + 100L)));
      }
    }
    return file;
  }

  private IMetadataQuerier scalarMetadata(TsFileSequenceReader sequence) throws IOException {
    IMetadataQuerier metadata = mock(IMetadataQuerier.class);
    List<IChunkMetadata> first =
        new ArrayList<>(sequence.getChunkMetadataList(new Path("d1", "s1", true)));
    List<IChunkMetadata> second =
        new ArrayList<>(sequence.getChunkMetadataList(new Path("d1", "s2", true)));
    when(metadata.getChunkMetadataLists(eq(TREE_DEVICE), anySet(), isNull()))
        .thenReturn(Arrays.asList(first, second));
    return metadata;
  }

  private DeviceQueryTask scalarTask(IMetadataQuerier metadata, TrackingChunkLoader loader)
      throws Exception {
    List<String> columns = Arrays.asList("s1", "s2");
    TableSchema schema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT64),
                new MeasurementSchema("s2", TSDataType.INT64)),
            Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));
    ColumnMapping mapping =
        new TableQueryExecutor(metadata, loader, TableQueryOrdering.DEVICE).new ColumnMapping();
    mapping.add("s1", 0, schema);
    mapping.add("s2", 1, schema);
    return new DeviceQueryTask(TREE_DEVICE, columns, mapping, null, schema);
  }

  private static void assertAllDestroyed(List<Chunk> chunks) {
    for (Chunk chunk : chunks) {
      assertTrue(chunk.getEncryptParam().isDestroyed());
    }
  }

  private static class TrackingChunkLoader extends CachedChunkLoaderImpl {
    private final List<Chunk> loaded = new ArrayList<>();
    private IOException loadFailure;
    private boolean tamperSecondColumn;
    private boolean tamperTimeColumn;

    private TrackingChunkLoader(TsFileSequenceReader sequence) {
      super(sequence);
    }

    @Override
    public Chunk loadChunk(ChunkMetadata metadata) throws IOException {
      if (loadFailure != null && "s2".equals(metadata.getMeasurementUid())) {
        throw loadFailure;
      }
      Chunk chunk = super.loadChunk(metadata);
      if ((tamperSecondColumn && "s2".equals(metadata.getMeasurementUid()))
          || (tamperTimeColumn && "".equals(metadata.getMeasurementUid()))) {
        byte[] data = chunk.getData().array();
        data[data.length - 1] ^= 1;
      }
      loaded.add(chunk);
      return chunk;
    }
  }
}
