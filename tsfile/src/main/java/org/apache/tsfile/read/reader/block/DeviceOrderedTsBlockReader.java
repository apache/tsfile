package org.apache.tsfile.read.reader.block;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.query.executor.task.DeviceQueryTask;
import org.apache.tsfile.read.query.executor.task.DeviceTaskIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceOrderedTsBlockReader implements TsBlockReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceOrderedTsBlockReader.class);
  private final DeviceTaskIterator taskIterator;
  private final IMetadataQuerier metadataQuerier;
  private final IChunkLoader chunkLoader;
  private final int blockSize;
  private SingleDeviceTsBlockReader currentReader;
  private ExpressionTree timeFilter;
  private ExpressionTree measurementFilter;

  public DeviceOrderedTsBlockReader(DeviceTaskIterator taskIterator,
      IMetadataQuerier metadataQuerier,
      IChunkLoader chunkLoader, ExpressionTree timeFilter, ExpressionTree measurementFilter,
      int blockSize) {
    this.taskIterator = taskIterator;
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
    this.blockSize = blockSize;
    this.timeFilter = timeFilter;
    this.measurementFilter = measurementFilter;
  }

  @Override
  public boolean hasNext() {
    if (currentReader != null && currentReader.hasNext()) {
      return true;
    }
    while (taskIterator.hasNext()) {
      final DeviceQueryTask nextTask = taskIterator.next();
      try {
        currentReader = new SingleDeviceTsBlockReader(nextTask, metadataQuerier, chunkLoader,
            blockSize, timeFilter, measurementFilter);
      } catch (IOException e) {
        LOGGER.error("Failed to construct reader for {}", nextTask, e);
      }
      if (currentReader.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TsBlock next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return currentReader.next();
  }

  @Override
  public void close() throws Exception {
    if (currentReader != null) {
      currentReader.close();
    }
  }
}
