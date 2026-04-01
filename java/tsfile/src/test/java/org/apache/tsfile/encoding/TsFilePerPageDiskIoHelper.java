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

package org.apache.tsfile.encoding;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Splits a TsFile byte array into consecutive segments so that each <em>page</em> inside a chunk
 * body is its own segment (chunk headers and non-chunk gaps are separate segments). Used to time
 * disk I/O as one physical write/read per segment.
 */
public final class TsFilePerPageDiskIoHelper {

  private TsFilePerPageDiskIoHelper() {}

  /**
   * Build non-overlapping {@code [offset, length)} segments covering the whole file, ordered by
   * offset. Chunk data regions are split per page; other bytes are grouped into gap/header
   * segments.
   *
   * <p>Walks the data section in on-disk order using the same marker/page rules as {@link
   * TsFileSequenceReader#selfCheck}, so results stay consistent with {@link ByteArrayTsFileInput}
   * (see {@link TsFileInput#wrapAsInputStream()} position sync).
   */
  public static List<int[]> buildContiguousSegments(byte[] tsfileBytes) throws IOException {
    Objects.requireNonNull(tsfileBytes, "tsfileBytes");
    int fileLen = tsfileBytes.length;
    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;

    ByteArrayTsFileInput input = new ByteArrayTsFileInput(tsfileBytes);
    TsFileSequenceReader reader = new TsFileSequenceReader(input);
    try {
      reader.readFileMetadata();
      input.position(headerLength);

      List<int[]> segments = new ArrayList<>();
      segments.add(new int[] {0, headerLength});

      while (true) {
        long markPos = input.position();
        byte marker = reader.readMarker();
        if (marker == MetaMarker.SEPARATOR) {
          segments.add(new int[] {(int) markPos, fileLen - (int) markPos});
          break;
        }
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            {
              long chunkStart = markPos;
              ChunkHeader chunkHeader = reader.readChunkHeader(marker);
              int headerSize = chunkHeader.getSerializedSize();
              segments.add(new int[] {(int) chunkStart, headerSize});

              int dataSize = chunkHeader.getDataSize();
              if (dataSize > 0) {
                if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.CHUNK_HEADER) {
                  while (dataSize > 0) {
                    long pageStart = input.position();
                    PageHeader pageHeader =
                        reader.readPageHeader(chunkHeader.getDataType(), true);
                    if (pageHeader.getUncompressedSize() != 0) {
                      reader.skipPageData(pageHeader);
                    }
                    long pageEnd = input.position();
                    segments.add(
                        new int[] {(int) pageStart, (int) (pageEnd - pageStart)});
                    dataSize -= pageHeader.getSerializedPageSize();
                  }
                } else {
                  long pageStart = input.position();
                  PageHeader pageHeader =
                      reader.readPageHeader(chunkHeader.getDataType(), false);
                  if (pageHeader.getUncompressedSize() != 0) {
                    reader.skipPageData(pageHeader);
                  }
                  long pageEnd = input.position();
                  segments.add(
                      new int[] {(int) pageStart, (int) (pageEnd - pageStart)});
                }
              }
              break;
            }
          case MetaMarker.CHUNK_GROUP_HEADER:
            {
              long groupStart = markPos;
              reader.readChunkGroupHeader();
              long groupEnd = input.position();
              segments.add(new int[] {(int) groupStart, (int) (groupEnd - groupStart)});
              break;
            }
          case MetaMarker.OPERATION_INDEX_RANGE:
            {
              long opStart = markPos;
              reader.readPlanIndex();
              long opEnd = input.position();
              segments.add(new int[] {(int) opStart, (int) (opEnd - opStart)});
              break;
            }
          default:
            throw new IOException("Unexpected marker " + marker + " at offset " + markPos);
        }
      }

      return mergeGapsAndVerify(segments, fileLen);
    } finally {
      reader.close();
    }
  }

  /** Write each segment with a separate timed {@link FileChannel#write(ByteBuffer, long)} loop. */
  public static long writeAllSegmentsTimed(File outFile, byte[] data, List<int[]> segments)
      throws IOException {
    long totalNs = 0;
    try (FileChannel ch =
        FileChannel.open(
            outFile.toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      for (int[] seg : segments) {
        ByteBuffer bb = ByteBuffer.wrap(data, seg[0], seg[1]);
        long pos = seg[0];
        long t0 = System.nanoTime();
        while (bb.hasRemaining()) {
          int w = ch.write(bb, pos);
          if (w <= 0) {
            throw new IOException("FileChannel.write stalled");
          }
          pos += w;
        }
        totalNs += System.nanoTime() - t0;
      }
    }
    return totalNs;
  }

  /** Read each segment with a separate timed {@link RandomAccessFile#readFully(byte[], int, int)}. */
  public static Pair<byte[], Long> readAllSegmentsTimed(File tsfile, List<int[]> segments)
      throws IOException {
    int len = (int) tsfile.length();
    byte[] buf = new byte[len];
    long totalNs = 0;
    try (RandomAccessFile raf = new RandomAccessFile(tsfile, "r")) {
      for (int[] seg : segments) {
        long t0 = System.nanoTime();
        raf.seek(seg[0]);
        raf.readFully(buf, seg[0], seg[1]);
        totalNs += System.nanoTime() - t0;
      }
    }
    return new Pair<>(buf, totalNs);
  }

  /**
   * Sort segments, drop empty spans, insert gap segments for any hole, append tail to EOF, and
   * verify full coverage. Fixes missing header/page slices when metadata or page parsing is
   * slightly inconsistent.
   */
  private static List<int[]> mergeGapsAndVerify(List<int[]> segments, int fileLen) throws IOException {
    segments.removeIf(s -> s[1] <= 0);
    segments.sort(Comparator.comparingInt(a -> a[0]));
    List<int[]> merged = new ArrayList<>();
    int expect = 0;
    for (int[] s : segments) {
      if (s[0] < expect) {
        throw new IOException("Overlapping segments at " + s[0] + ", expected end " + expect);
      }
      if (s[0] > expect) {
        merged.add(new int[] {expect, s[0] - expect});
      }
      merged.add(s);
      expect = s[0] + s[1];
    }
    if (expect < fileLen) {
      merged.add(new int[] {expect, fileLen - expect});
    }
    if (expect > fileLen) {
      throw new IOException("Segments extend past EOF: end " + expect + " fileLen " + fileLen);
    }
    return merged;
  }
}
