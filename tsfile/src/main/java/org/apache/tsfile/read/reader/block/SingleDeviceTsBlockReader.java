package org.apache.tsfile.read.reader.block;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.query.executor.task.DeviceQueryTask;
import org.apache.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.tsfile.read.reader.series.FileSeriesReader;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleDeviceTsBlockReader implements TsBlockReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleDeviceTsBlockReader.class);
  private final DeviceQueryTask task;
  private final ExpressionTree measurementExpression;
  private final int blockSize;

  private final TsBlock currentBlock;
  private boolean lastBlockReturned = true;
  private final Map<String, MeasurementColumnContext> measureColumnContextMap;
  private final Map<String, IdColumnContext> idColumnContextMap;

  private long nextTime;

  public SingleDeviceTsBlockReader(DeviceQueryTask task, IMetadataQuerier metadataQuerier,
      IChunkLoader chunkLoader, int blockSize, ExpressionTree timeExpression,
      ExpressionTree measurementFilter) throws IOException {
    this.task = task;
    this.blockSize = blockSize;
    this.measurementExpression = measurementFilter;

    this.currentBlock = TsBlock.buildTsBlock(task.getColumnNames(), task.getTableSchema(),
        blockSize);
    this.measureColumnContextMap = new HashMap<>();
    this.idColumnContextMap = new HashMap<>();

    final List<List<IChunkMetadata>> chunkMetadataLists = metadataQuerier.getChunkMetadataLists(
        task.getDeviceID(), task.getColumnMapping()
            .getMeasurementColumns(), task.getIndexRoot());

    Filter timeFilter = timeExpression == null ? null : timeExpression.toFilter();
    for (List<IChunkMetadata> chunkMetadataList : chunkMetadataLists) {
      if (!chunkMetadataList.isEmpty()) {
        final String measurementUid = chunkMetadataList.get(0).getMeasurementUid();
        AbstractFileSeriesReader seriesReader = new FileSeriesReader(chunkLoader,
            chunkMetadataList, timeFilter);
        if (seriesReader.hasNextBatch()) {
          measureColumnContextMap.put(measurementUid, new MeasurementColumnContext(measurementUid,
              task.getColumnMapping().getColumnPos(measurementUid), seriesReader.nextBatch(),
              seriesReader));
        }
      }
    }

    for (String idColumn : task.getColumnMapping().getIdColumns()) {
      final List<Integer> columnPosInResult = task.getColumnMapping().getColumnPos(idColumn);
      final int columnPosInId = task.getTableSchema().findColumnIndex(idColumn);
      idColumnContextMap.put(idColumn, new IdColumnContext(columnPosInResult, columnPosInId));
    }
  }

  @Override
  public boolean hasNext() {
    if (!lastBlockReturned) {
      return true;
    }

    if (measureColumnContextMap.isEmpty()) {
      return false;
    }

    currentBlock.reset();
    nextTime = Long.MAX_VALUE;
    List<MeasurementColumnContext> alignedColumns = new ArrayList<>();

    while (currentBlock.getPositionCount() < blockSize) {
      // find the minimum time among the batches and the associated columns
      for (Entry<String, MeasurementColumnContext> entry : measureColumnContextMap.entrySet()) {
        final BatchData batchData = entry.getValue().currentBatch;
        final long currentTime = batchData.currentTime();
        if (nextTime > currentTime) {
          nextTime = currentTime;
          alignedColumns.clear();
        } else if (nextTime == currentTime) {
          alignedColumns.add(entry.getValue());
        }
      }

      try {
        fillMeasurements(alignedColumns);
      } catch (IOException e) {
        LOGGER.error("Cannot fill measurements", e);
        return false;
      }

      // all columns have exhausted
      if (measureColumnContextMap.isEmpty()) {
        break;
      }
    }

    if (currentBlock.getPositionCount() > 0) {
      fillIds();
      currentBlock.fillTrailingNulls();
      lastBlockReturned = false;
      return true;
    }

    return false;
  }

  private void fillIds() {
    for (Entry<String, IdColumnContext> entry : idColumnContextMap.entrySet()) {
      final IdColumnContext idColumnContext = entry.getValue();
      for (Integer pos : idColumnContext.posInResult) {
        final Column column = currentBlock.getColumn(pos);
        fillIdColumn(column, task.getDeviceID().segment(idColumnContext.posInDeviceId), 0,
            currentBlock.getPositionCount());
      }
    }
  }

  private void fillMeasurements(List<MeasurementColumnContext> alignedColumns) throws IOException {
    if (measurementExpression == null || measurementExpression.satisfy(this)) {
      // use the time to fill the block
      final int positionCount = currentBlock.getPositionCount();
      currentBlock.getTimeColumn().getTimes()[positionCount] = nextTime;
      // project the value columns to the result
      for (final MeasurementColumnContext columnContext : alignedColumns) {
        final BatchData batchData = columnContext.currentBatch;
        final List<Integer> posInResult = columnContext.posInResult;
        for (Integer pos : posInResult) {
          final Column column = currentBlock.getColumn(pos);
          fillMeasurementColumn(column, batchData, positionCount);
        }

        batchData.next();
        if (!batchData.hasCurrent()) {
          // get next batch of the column
          if (columnContext.seriesReader.hasNextBatch()) {
            columnContext.currentBatch = columnContext.seriesReader.nextBatch();
          } else {
            // no more data in this column
            measureColumnContextMap.remove(columnContext.columnName);
          }
        }
      }
      currentBlock.setPositionCount(positionCount + 1);
    }
  }

  private void fillIdColumn(Column column, Object val, int startPos, int endPos) {
    switch (column.getDataType()) {
      case TEXT:
        if (val instanceof String) {
          val = new Binary(((String) val), StandardCharsets.UTF_8);
        }
        Arrays.fill(column.getBinaries(), startPos, endPos, val);
        break;
      case BOOLEAN:
        Arrays.fill(column.getBooleans(), startPos, endPos, ((boolean) val));
        break;
      case INT32:
        Arrays.fill(column.getInts(), startPos, endPos, ((int) val));
        break;
      case INT64:
        Arrays.fill(column.getLongs(), startPos, endPos, ((long) val));
        break;
      case FLOAT:
        Arrays.fill(column.getFloats(), startPos, endPos, ((float) val));
        break;
      case DOUBLE:
        Arrays.fill(column.getDoubles(), startPos, endPos, ((double) val));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + column.getDataType());
    }
    column.setPositionCount(endPos);
  }

  private void fillMeasurementColumn(Column column, BatchData batchData, int pos) {
    switch (batchData.getDataType()) {
      case BOOLEAN:
        column.getBooleans()[pos] = batchData.getBoolean();
        break;
      case DOUBLE:
        column.getDoubles()[pos] = batchData.getDouble();
        break;
        case FLOAT:
          column.getFloats()[pos] = batchData.getFloat();
          break;
      case INT32:
        column.getInts()[pos] = batchData.getInt();
        break;
      case TEXT:
        column.getBinaries()[pos] = batchData.getBinary();
        break;
      case INT64:
        column.getLongs()[pos] = batchData.getLong();
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + batchData.getDataType());
    }
    column.setPositionCount(pos + 1);
  }


  @Override
  public TsBlock next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    lastBlockReturned = true;
    return currentBlock;
  }

  @Override
  public void close() throws Exception {
    // nothing to be done
  }

  // gather necessary fields in this class to avoid redundant map access
  public static class MeasurementColumnContext {

    private final String columnName;
    private final List<Integer> posInResult;
    private BatchData currentBatch;
    private final AbstractFileSeriesReader seriesReader;

    public MeasurementColumnContext(String columnName, List<Integer> posInResult,
        BatchData currentBatch,
        AbstractFileSeriesReader seriesReader) {
      this.columnName = columnName;
      this.posInResult = posInResult;
      this.currentBatch = currentBatch;
      this.seriesReader = seriesReader;
    }
  }

  public static class IdColumnContext {

    private final List<Integer> posInResult;
    private final int posInDeviceId;

    public IdColumnContext(List<Integer> posInResult,
        int posInDeviceId) {
      this.posInResult = posInResult;
      this.posInDeviceId = posInDeviceId;
    }
  }
}
