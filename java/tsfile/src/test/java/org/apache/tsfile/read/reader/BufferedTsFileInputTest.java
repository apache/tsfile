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

package org.apache.tsfile.read.reader;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class BufferedTsFileInputTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File file;
  private byte[] data;

  @Before
  public void setUp() throws Exception {
    file = temporaryFolder.newFile("buffered-input.tsfile");
    data = new byte[32];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }
    Files.write(file.toPath(), data);
  }

  @Test
  public void testSequentialAndPositionedRead() throws Exception {
    BufferedTsFileInput input = new BufferedTsFileInput(file.toPath(), 8);
    try {
      assertEquals(data.length, input.size());
      assertEquals(file.toPath().toString(), input.getFilePath());
      assertEquals(0, input.position());

      ByteBuffer firstRead = ByteBuffer.allocate(5);
      assertEquals(5, input.read(firstRead));
      assertBufferEquals(new byte[] {0, 1, 2, 3, 4}, firstRead);
      assertEquals(5, input.position());

      ByteBuffer positionedRead = ByteBuffer.allocate(6);
      assertEquals(6, input.read(positionedRead, 2));
      assertBufferEquals(new byte[] {2, 3, 4, 5, 6, 7}, positionedRead);
      assertEquals(5, input.position());

      ByteBuffer crossBufferRead = ByteBuffer.allocate(10);
      assertEquals(10, input.read(crossBufferRead));
      assertBufferEquals(new byte[] {5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, crossBufferRead);
      assertEquals(15, input.position());

      input.position(29);
      ByteBuffer endRead = ByteBuffer.allocate(8);
      assertEquals(3, input.read(endRead));
      assertBufferEquals(new byte[] {29, 30, 31}, endRead);
      assertEquals(32, input.position());
      assertEquals(-1, input.read(ByteBuffer.allocate(1)));
      assertEquals(0, input.read(ByteBuffer.allocate(0)));
    } finally {
      input.close();
    }
  }

  @Test
  public void testReadLargerThanBuffer() throws Exception {
    BufferedTsFileInput input = new BufferedTsFileInput(file.toPath(), 4);
    try {
      ByteBuffer destination = ByteBuffer.allocate(20);
      assertEquals(20, input.read(destination, 3));
      byte[] expected = new byte[20];
      System.arraycopy(data, 3, expected, 0, expected.length);
      assertBufferEquals(expected, destination);
      assertEquals(0, input.position());
    } finally {
      input.close();
    }
  }

  @Test
  public void testInputStreamSharesPosition() throws Exception {
    BufferedTsFileInput input = new BufferedTsFileInput(file.toPath(), 4);
    input.position(3);
    try (InputStream stream = input.wrapAsInputStream()) {
      assertEquals(3, stream.read());
      assertEquals(4, input.position());

      byte[] bytes = new byte[7];
      assertEquals(7, stream.read(bytes));
      assertArrayEquals(new byte[] {4, 5, 6, 7, 8, 9, 10}, bytes);
      assertEquals(11, input.position());

      assertEquals(5, stream.skip(5));
      assertEquals(16, input.position());
      assertEquals(16, stream.available());
      assertEquals(16, stream.read());
    }
    assertThrows(ClosedChannelException.class, input::position);
  }

  @Test
  public void testInvalidArguments() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> new BufferedTsFileInput(file.toPath(), 0));
    assertThrows(IllegalArgumentException.class, () -> new BufferedTsFileInput(file.toPath(), -1));

    BufferedTsFileInput input = new BufferedTsFileInput(file.toPath());
    try {
      assertThrows(IllegalArgumentException.class, () -> input.position(-1));
      assertThrows(IllegalArgumentException.class, () -> input.read(ByteBuffer.allocate(1), -1));
    } finally {
      input.close();
    }
  }

  private void assertBufferEquals(byte[] expected, ByteBuffer actual) {
    actual.flip();
    byte[] actualBytes = new byte[actual.remaining()];
    actual.get(actualBytes);
    assertArrayEquals(expected, actualBytes);
  }
}
