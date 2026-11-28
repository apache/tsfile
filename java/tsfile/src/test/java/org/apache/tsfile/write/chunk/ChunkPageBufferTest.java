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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

public class ChunkPageBufferTest {

  @Test
  public void testPagesAreStoredExactlyAndWrittenInOrder() throws Exception {
    ChunkPageBuffer buffer = new ChunkPageBuffer();
    byte[] page1 = "abc".getBytes("UTF-8");
    byte[] page2 = new byte[1000];
    for (int i = 0; i < page2.length; i++) {
      page2[i] = (byte) i;
    }

    buffer.beginPage();
    buffer.write(page1, 0, page1.length);
    buffer.endPage();

    buffer.beginPage();
    buffer.write(page2, 0, page2.length);
    buffer.endPage();

    Assert.assertEquals(page1.length + page2.length, buffer.size());
    Assert.assertEquals(2, buffer.getPageCount());
    Assert.assertArrayEquals(page1, buffer.getFirstPage());

    byte[] expected = new byte[page1.length + page2.length];
    System.arraycopy(page1, 0, expected, 0, page1.length);
    System.arraycopy(page2, 0, expected, page1.length, page2.length);
    Assert.assertArrayEquals(expected, buffer.toByteArray());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    buffer.writeTo(out);
    Assert.assertArrayEquals(expected, out.toByteArray());
  }

  @Test
  public void testReplaceAndTruncateFirstPage() {
    ChunkPageBuffer buffer = new ChunkPageBuffer();
    buffer.beginPage();
    buffer.write(new byte[] {1, 2, 3, 4, 5}, 0, 5);
    buffer.endPage();

    buffer.replaceFirstPage(new byte[] {1, 2, 3});
    Assert.assertEquals(3, buffer.size());
    Assert.assertArrayEquals(new byte[] {1, 2, 3}, buffer.getFirstPage());

    buffer.truncateFirstPage(2);
    Assert.assertEquals(2, buffer.size());
    Assert.assertArrayEquals(new byte[] {1, 2}, buffer.toByteArray());
  }

  @Test
  public void testResetKeepsScratchButReleaseDropsIt() {
    ChunkPageBuffer buffer = new ChunkPageBuffer();
    buffer.beginPage();
    buffer.write(new byte[4096], 0, 4096);
    buffer.endPage();
    long retainedAfterPage = buffer.retainedBytes();

    buffer.reset();
    Assert.assertEquals(0, buffer.size());
    Assert.assertEquals(0, buffer.getPageCount());
    // reset keeps the page scratch for reuse
    Assert.assertTrue(buffer.retainedBytes() <= retainedAfterPage);

    buffer.release();
    Assert.assertEquals(0, buffer.size());
    Assert.assertEquals(0, buffer.getPageCount());
    Assert.assertTrue(buffer.retainedBytes() < 4096);
  }

  @Test
  public void testResetPoolsSlabsForReuse() {
    ChunkPageBuffer buffer = new ChunkPageBuffer();
    writeTwoPages(buffer, 10_000);
    buffer.reset();
    long retainedAfterFirstChunk = buffer.retainedBytes();

    writeTwoPages(buffer, 10_000);
    buffer.reset();
    long retainedAfterSecondChunk = buffer.retainedBytes();

    Assert.assertEquals(0, buffer.size());
    Assert.assertTrue(retainedAfterFirstChunk > 0);
    Assert.assertTrue(retainedAfterSecondChunk <= retainedAfterFirstChunk + 4096);
  }

  private static void writeTwoPages(ChunkPageBuffer buffer, int pageSize) {
    byte[] page = new byte[pageSize];
    for (int i = 0; i < page.length; i++) {
      page[i] = (byte) i;
    }
    buffer.beginPage();
    buffer.write(page, 0, page.length);
    buffer.endPage();
    buffer.beginPage();
    buffer.write(page, 0, page.length);
    buffer.endPage();
  }

  @Test
  public void testRetainedBytesEqualsPagesPlusCurrentScratch() {
    ChunkPageBuffer buffer = new ChunkPageBuffer();
    buffer.beginPage();
    buffer.write(new byte[300], 0, 300);
    buffer.endPage();
    buffer.beginPage();
    buffer.write(new byte[100], 0, 100);

    // 300 stored page bytes + the first page scratch; blocks have at most one small block of slack
    Assert.assertTrue(buffer.retainedBytes() >= 400);
    Assert.assertTrue(buffer.retainedBytes() < 16 * 1024);
    Assert.assertEquals(400, buffer.size());
    Assert.assertTrue(Arrays.equals(buffer.toByteArray(), buffer.toByteArray()));
  }
}
