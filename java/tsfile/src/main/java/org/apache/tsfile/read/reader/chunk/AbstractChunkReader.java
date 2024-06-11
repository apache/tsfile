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

package org.apache.tsfile.read.reader.chunk;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.IPageReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public abstract class AbstractChunkReader implements IChunkReader {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractChunkReader.class);

  protected final Decoder defaultTimeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  protected final long readStopTime;

  // any filter, no matter value filter or time filter
  protected final Filter queryFilter;

  // init the pageReaderIterator in subclasses
  protected Iterator<IPageReader> pageReaderIterator;

  protected AbstractChunkReader(long readStopTime, Filter filter) {
    this.readStopTime = readStopTime;
    this.queryFilter = filter;
  }

  /** judge if has next page whose page header satisfies the filter. */
  @Override
  public boolean hasNextSatisfiedPage() {
    if (pageReaderIterator == null) {
      return false;
    }
    return pageReaderIterator.hasNext();
  }

  /**
   * get next data batch.
   *
   * @return next data batch
   * @throws IOException IOException
   */
  @Override
  public BatchData nextPageData() throws IOException {
    return pageReaderIterator.next().getAllSatisfiedPageData();
  }

  @Override
  public void close() {
    pageReaderIterator = null;
  }
}
