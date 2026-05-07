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

package org.apache.tsfile.write.writer;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Writes one chunk's serialized page body ({@link PublicBAOS}) using one {@link TsFileOutput#write}
 * per page (plus {@link TsFileOutput#flush}). Matches how {@link
 * org.apache.tsfile.read.reader.chunk.ChunkReader} walks page headers.
 */
public final class ChunkBodyPagedIoWriter {

  private ChunkBodyPagedIoWriter() {}

  public static void writeChunkBody(
      TsFileOutput out,
      PublicBAOS bytes,
      int numOfPages,
      TSDataType dataType,
      Statistics<? extends Serializable> chunkStatistics)
      throws IOException {
    byte[] raw = bytes.toByteArray();
    boolean onlyOnePageChunk = numOfPages <= 1;
    ByteBuffer buf = ByteBuffer.wrap(raw);
    while (buf.hasRemaining()) {
      int sliceStart = buf.position();
      PageHeader ph;
      if (onlyOnePageChunk && chunkStatistics != null) {
        ph = PageHeader.deserializeFrom(buf, chunkStatistics);
      } else {
        ph = PageHeader.deserializeFrom(buf, dataType);
      }
      if (ph.getUncompressedSize() != 0) {
        int skip = ph.getCompressedSize();
        int p = buf.position();
        buf.position(p + skip);
      }
      int sliceEnd = buf.position();
      out.write(Arrays.copyOfRange(raw, sliceStart, sliceEnd));
      out.flush();
    }
  }
}
