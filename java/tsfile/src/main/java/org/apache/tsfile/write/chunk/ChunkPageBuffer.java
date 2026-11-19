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
package org.apache.tsfile.write.chunk;

import org.apache.tsfile.utils.PublicBAOS;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Chunk page storage backed by a slab stream.
 *
 * <p>The first page is kept in a scratch buffer so its statistics can still be inserted later.
 * Subsequent page bytes are appended into slabs; slabs are filled before a new one is taken and are
 * recycled through size-class LIFO pools. This keeps the retained memory close to the page bytes
 * and avoids both chunk-level doubling and per-page block slack.
 */
final class ChunkPageBuffer extends PublicBAOS {

  private static final int MIN_SLAB = 4 * 1024;
  private static final int MAX_POOLED_SLAB = 64 * 1024;
  private static final int[] SLAB_SIZES = {
    MIN_SLAB, 8 * 1024, 16 * 1024, 32 * 1024, MAX_POOLED_SLAB
  };

  private final List<byte[]> slabs = new ArrayList<>();
  private final ArrayDeque<byte[]>[] freeSlabs;

  private byte[] currentSlab;
  private int slabPosition;
  private long totalBytes;
  private long activeCapacity;

  private PublicBAOS firstPage = new PublicBAOS();
  private boolean firstPageFlushed;
  private int firstPageLength;

  private int slabSize = MIN_SLAB;
  private long poolBytes;
  private long poolLimitBytes = MAX_POOLED_SLAB;

  private int pageIndex = -1;

  @SuppressWarnings("unchecked")
  ChunkPageBuffer() {
    freeSlabs = new ArrayDeque[SLAB_SIZES.length];
    for (int i = 0; i < freeSlabs.length; i++) {
      freeSlabs[i] = new ArrayDeque<>();
    }
  }

  void beginPage() {
    pageIndex++;
    if (pageIndex == 0) {
      firstPage.reset();
      firstPageFlushed = false;
      firstPageLength = 0;
    }
  }

  void endPage() {
    // Bytes are appended to the slab stream as they arrive; no per-page finalization is needed.
  }

  @Override
  public void write(int b) {
    if (pageIndex <= 0) {
      firstPage.write(b);
      return;
    }
    flushFirstPageIfNeeded();
    ensureSlab(1);
    currentSlab[slabPosition++] = (byte) b;
    totalBytes++;
  }

  @Override
  public void write(byte[] b, int off, int len) {
    if (pageIndex <= 0) {
      firstPage.write(b, off, len);
    } else {
      flushFirstPageIfNeeded();
      appendToSlabs(b, off, len);
    }
  }

  private void flushFirstPageIfNeeded() {
    if (firstPageFlushed) {
      return;
    }
    firstPageLength = firstPage.size();
    slabSize = clampSlabSize(nextPowerOfTwo(firstPageLength));
    appendToSlabs(firstPage.getBuf(), 0, firstPageLength);
    firstPage.reset();
    firstPageFlushed = true;
  }

  private void appendToSlabs(byte[] b, int off, int len) {
    int remaining = len;
    int position = off;
    while (remaining > 0) {
      ensureSlab(remaining);
      int copyLength = Math.min(remaining, currentSlab.length - slabPosition);
      System.arraycopy(b, position, currentSlab, slabPosition, copyLength);
      slabPosition += copyLength;
      position += copyLength;
      remaining -= copyLength;
    }
    totalBytes += len;
  }

  private void ensureSlab(int required) {
    if (currentSlab != null && currentSlab.length - slabPosition > 0) {
      return;
    }
    int size = Math.max(slabSize, required);
    if (size > MAX_POOLED_SLAB) {
      currentSlab = new byte[size];
    } else {
      int sizeClass = sizeClass(size);
      currentSlab = freeSlabs[sizeClass].pollFirst();
      if (currentSlab == null) {
        currentSlab = new byte[SLAB_SIZES[sizeClass]];
      } else {
        poolBytes -= currentSlab.length;
      }
    }
    slabs.add(currentSlab);
    activeCapacity += currentSlab.length;
    slabPosition = 0;
  }

  private void returnSlab(byte[] slab) {
    if (slab.length > MAX_POOLED_SLAB || poolBytes + slab.length > poolLimitBytes) {
      return;
    }
    freeSlabs[sizeClass(slab.length)].addFirst(slab);
    poolBytes += slab.length;
  }

  private static int nextPowerOfTwo(int value) {
    if (value <= MIN_SLAB) {
      return MIN_SLAB;
    }
    int highest = Integer.highestOneBit(value - 1);
    return highest >= MAX_POOLED_SLAB ? MAX_POOLED_SLAB : highest << 1;
  }

  private static int clampSlabSize(int size) {
    return Math.max(MIN_SLAB, Math.min(MAX_POOLED_SLAB, size));
  }

  private static int sizeClass(int size) {
    for (int i = 0; i < SLAB_SIZES.length; i++) {
      if (size <= SLAB_SIZES[i]) {
        return i;
      }
    }
    return SLAB_SIZES.length - 1;
  }

  @Override
  public int size() {
    return (int) Math.min(Integer.MAX_VALUE, firstPageFlushed ? totalBytes : firstPage.size());
  }

  int getPageCount() {
    return Math.max(0, pageIndex + 1);
  }

  byte[] getFirstPage() {
    if (!firstPageFlushed) {
      return firstPage.toByteArray();
    }
    byte[] first = new byte[firstPageLength];
    int position = 0;
    for (byte[] slab : slabs) {
      int copyLength = Math.min(slab.length, firstPageLength - position);
      if (copyLength <= 0) {
        break;
      }
      System.arraycopy(slab, 0, first, position, copyLength);
      position += copyLength;
      if (position == firstPageLength) {
        break;
      }
    }
    return first;
  }

  void replaceFirstPage(byte[] page) {
    if (firstPageFlushed) {
      throw new IllegalStateException("first page has already been flushed");
    }
    firstPage.reset();
    firstPage.write(page, 0, page.length);
  }

  void truncateFirstPage(int newSize) {
    if (firstPageFlushed) {
      throw new IllegalStateException("first page has already been flushed");
    }
    byte[] page = firstPage.toByteArray();
    if (newSize < 0 || newSize > page.length) {
      throw new IllegalArgumentException("newSize=" + newSize + ", pageSize=" + page.length);
    }
    firstPage.reset();
    firstPage.write(page, 0, newSize);
  }

  @Override
  public byte[] toByteArray() {
    if (!firstPageFlushed) {
      return firstPage.toByteArray();
    }
    byte[] result = new byte[size()];
    int position = 0;
    for (byte[] slab : slabs) {
      int copyLength = Math.min(slab.length, result.length - position);
      if (copyLength <= 0) {
        break;
      }
      System.arraycopy(slab, 0, result, position, copyLength);
      position += copyLength;
      if (position == result.length) {
        break;
      }
    }
    return result;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    if (!firstPageFlushed) {
      out.write(firstPage.getBuf(), 0, firstPage.size());
      return;
    }
    int remaining = size();
    for (byte[] slab : slabs) {
      int writeLength = Math.min(slab.length, remaining);
      if (writeLength <= 0) {
        break;
      }
      out.write(slab, 0, writeLength);
      remaining -= writeLength;
      if (remaining == 0) {
        break;
      }
    }
  }

  @Override
  public void reset() {
    poolLimitBytes = Math.max(MIN_SLAB, totalBytes);
    for (byte[] slab : slabs) {
      returnSlab(slab);
    }
    clearState();
  }

  long retainedBytes() {
    return activeCapacity + poolBytes + firstPage.getBuf().length;
  }

  void release() {
    for (ArrayDeque<byte[]> free : freeSlabs) {
      free.clear();
    }
    poolBytes = 0;
    firstPage = new PublicBAOS();
    clearState();
  }

  private void clearState() {
    slabs.clear();
    currentSlab = null;
    slabPosition = 0;
    totalBytes = 0;
    activeCapacity = 0;
    firstPage.reset();
    firstPageFlushed = false;
    firstPageLength = 0;
    slabSize = MIN_SLAB;
    pageIndex = -1;
  }
}
