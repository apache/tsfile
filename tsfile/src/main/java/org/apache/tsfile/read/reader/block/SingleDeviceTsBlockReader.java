package org.apache.tsfile.read.reader.block;

import java.io.IOException;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.query.executor.task.DeviceQueryTask;

public class SingleDeviceTsBlockReader implements TsBlockReader {

  private DeviceQueryTask task;
  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;

  public SingleDeviceTsBlockReader(DeviceQueryTask task, IMetadataQuerier metadataQuerier,
      IChunkLoader chunkLoader) {
    this.task = task;
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;


  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public TsBlock next() throws IOException {
    return null;
  }

  @Override
  public void close() throws Exception {
    // nothing to be done
  }
}
