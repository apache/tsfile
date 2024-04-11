package org.apache.tsfile.read.reader.block;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.query.executor.task.DeviceQueryTask;
import org.apache.tsfile.read.query.executor.task.DeviceTaskIterator;

public class DeviceOrderedTsBlockReader implements TsBlockReader {

  private final DeviceTaskIterator taskIterator;
  private final IMetadataQuerier metadataQuerier;
  private final IChunkLoader chunkLoader;
  private SingleDeviceTsBlockReader currentReader;

  public DeviceOrderedTsBlockReader(DeviceTaskIterator taskIterator,
      IMetadataQuerier metadataQuerier,
      IChunkLoader chunkLoader) {
    this.taskIterator = taskIterator;
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  @Override
  public boolean hasNext() {
    if (currentReader != null && currentReader.hasNext()) {
      return true;
    }
    while (taskIterator.hasNext()) {
      final DeviceQueryTask nextTask = taskIterator.next();
      currentReader = new SingleDeviceTsBlockReader(nextTask, metadataQuerier, chunkLoader);
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
