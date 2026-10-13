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

import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.query.executor.task.DeviceQueryTask;
import org.apache.tsfile.read.query.executor.task.DeviceTaskIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;

public class DeviceOrderedTsBlockReader implements TsBlockReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceOrderedTsBlockReader.class);
  private final DeviceTaskIterator taskIterator;
  private final IMetadataQuerier metadataQuerier;
  private final IChunkLoader chunkLoader;
  private final int blockSize;
  private SingleDeviceTsBlockReader currentReader;
  private ExpressionTree timeFilter;
  private ExpressionTree measurementFilter;
  private boolean closed;

  public DeviceOrderedTsBlockReader(
      DeviceTaskIterator taskIterator,
      IMetadataQuerier metadataQuerier,
      IChunkLoader chunkLoader,
      ExpressionTree timeFilter,
      ExpressionTree measurementFilter,
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
    if (closed) {
      return false;
    }
    if (currentReader != null && currentReader.hasNext()) {
      return true;
    }
    closeCurrentReaderForIteration();
    while (taskIterator.hasNext()) {
      final DeviceQueryTask nextTask = taskIterator.next();
      try {
        currentReader =
            new SingleDeviceTsBlockReader(
                nextTask, metadataQuerier, chunkLoader, blockSize, timeFilter, measurementFilter);
      } catch (IOException e) {
        if (EncryptException.isEncryptionFailure(e)) {
          closed = true;
          throw new UncheckedIOException(e);
        }
        LOGGER.error(Messages.get("log.read.block_reader_construct_error"), nextTask, e);
      }
      if (currentReader != null && currentReader.hasNext()) {
        return true;
      }
      closeCurrentReaderForIteration();
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
    closed = true;
    closeCurrentReader();
  }

  private void closeCurrentReaderForIteration() {
    try {
      closeCurrentReader();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void closeCurrentReader() throws Exception {
    if (currentReader != null) {
      try {
        currentReader.close();
      } finally {
        currentReader = null;
      }
    }
  }
}
