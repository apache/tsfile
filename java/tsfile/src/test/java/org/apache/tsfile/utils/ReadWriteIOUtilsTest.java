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
package org.apache.tsfile.utils;

import org.apache.tsfile.read.reader.TsFileInput;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class ReadWriteIOUtilsTest {

  protected static final int DEFAULT_BUFFER_SIZE = 4096;

  @Test
  public void testReadLongFromFragmentedStream() throws IOException {
    long expected = 0x8123456789ABCDEFL;
    byte[] encoded = ByteBuffer.allocate(Long.BYTES).putLong(expected).array();
    Assert.assertEquals(expected, ReadWriteIOUtils.readLong(new FragmentedInputStream(encoded, 2)));
  }

  @Test
  public void testReadLongRejectsEveryTruncatedLength() {
    for (int length = 0; length < Long.BYTES; length++) {
      final byte[] truncated = new byte[length];
      Assert.assertThrows(
          EOFException.class, () -> ReadWriteIOUtils.readLong(new ByteArrayInputStream(truncated)));
    }
  }

  @Test
  public void stringSerdeTest() {
    // 1. not null value
    String str = "string";
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    DataOutputStream stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(str, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    String result =
        ReadWriteIOUtils.readString(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(str, result);

    // 2. null value
    str = null;
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(str, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readString(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNull(result);
    Assert.assertEquals(str, result);
  }

  @Test
  public void mapSerdeTest() {
    // 1. key: not null; value: not null
    String key = "string";
    String value = "string";
    Map<String, String> map = new HashMap<>();
    map.put(key, value);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    DataOutputStream stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    Map<String, String> result =
        ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 2. key: not null; value: null
    key = "string";
    value = null;
    map.clear();
    map.put(key, value);
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 3. key: null; value: not null
    key = null;
    value = "string";
    map.clear();
    map.put(key, value);
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 4. key: null; value: null
    key = null;
    value = null;
    map.clear();
    map.put(key, value);
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 5. empty map
    map = Collections.emptyMap();
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }
    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());

    // 6. null
    map = null;
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }
    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNull(result);
  }

  @Test
  public void skipInputStreamTest() {
    int expectedSkipNum = 9000;
    int totalNum = 10000;
    try (BufferedInputStream inputStream =
        new BufferedInputStream(new ByteArrayInputStream(new byte[totalNum]))) {
      ReadWriteIOUtils.readByte(inputStream);
      long skipN = inputStream.skip(expectedSkipNum);
      Assert.assertNotEquals(expectedSkipNum, skipN);
      Assert.assertNotEquals(totalNum - expectedSkipNum - 1, inputStream.available());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    try (BufferedInputStream inputStream =
        new BufferedInputStream(new ByteArrayInputStream(new byte[totalNum]))) {
      ReadWriteIOUtils.readByte(inputStream);
      ReadWriteIOUtils.skip(inputStream, expectedSkipNum);
      Assert.assertEquals(totalNum - expectedSkipNum - 1, inputStream.available());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testFixedWidthReadersFromFragmentedStream() throws IOException {
    byte[] shortBytes = ByteBuffer.allocate(Short.BYTES).putShort((short) 0x1234).array();
    Assert.assertEquals(
        (short) 0x1234, ReadWriteIOUtils.readShort(new FragmentedInputStream(shortBytes, 2)));

    byte[] intBytes = ByteBuffer.allocate(Integer.BYTES).putInt(0x12345678).array();
    Assert.assertEquals(
        0x12345678, ReadWriteIOUtils.readInt(new FragmentedInputStream(intBytes, 3)));

    byte[] floatBytes = ByteBuffer.allocate(Float.BYTES).putFloat(1.25F).array();
    Assert.assertEquals(
        1.25F, ReadWriteIOUtils.readFloat(new FragmentedInputStream(floatBytes, 3)), 0.0F);

    byte[] doubleBytes = ByteBuffer.allocate(Double.BYTES).putDouble(1.25D).array();
    Assert.assertEquals(
        1.25D, ReadWriteIOUtils.readDouble(new FragmentedInputStream(doubleBytes, 3)), 0.0D);
  }

  @Test
  public void testStringReadersFromFragmentedStream() throws IOException {
    String expected = "fragmented-value";

    ByteArrayOutputStream fixedLengthBytes = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(expected, fixedLengthBytes);
    Assert.assertEquals(
        expected,
        ReadWriteIOUtils.readString(new FragmentedInputStream(fixedLengthBytes.toByteArray(), 2)));

    ByteArrayOutputStream varIntBytes = new ByteArrayOutputStream();
    ReadWriteIOUtils.writeVar(expected, varIntBytes);
    Assert.assertEquals(
        expected,
        ReadWriteIOUtils.readVarIntString(new FragmentedInputStream(varIntBytes.toByteArray(), 2)));
  }

  @Test
  public void testReadBytesFromFragmentedStream() throws IOException {
    byte[] expected = new byte[] {1, 2, 3, 4};
    Assert.assertArrayEquals(
        expected, ReadWriteIOUtils.readBytes(new FragmentedInputStream(expected, 2), 4));
  }

  @Test
  public void testReadAsPossibleDoesNotPerformExtraRead() throws IOException {
    TsFileInput input = Mockito.mock(TsFileInput.class);
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    Mockito.when(input.read(buffer, 0L))
        .thenAnswer(
            invocation -> {
              buffer.put(new byte[] {1, 2, 3, 4});
              return Integer.BYTES;
            });

    Assert.assertEquals(Integer.BYTES, ReadWriteIOUtils.readAsPossible(input, 0L, buffer));
    Mockito.verify(input, Mockito.times(1)).read(buffer, 0L);
  }

  @Test
  public void testReadAsPossibleRetriesZeroAtPosition() throws IOException {
    TsFileInput input = Mockito.mock(TsFileInput.class);
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    Mockito.when(input.read(buffer, 7L))
        .thenReturn(0)
        .thenAnswer(
            invocation -> {
              buffer.put(new byte[] {1, 2, 3, 4});
              return Integer.BYTES;
            });

    Assert.assertEquals(Integer.BYTES, ReadWriteIOUtils.readAsPossible(input, 7L, buffer));
    Mockito.verify(input, Mockito.times(2)).read(buffer, 7L);
  }

  @Test
  public void testReadAsPossibleRetriesZeroAtCurrentPosition() throws IOException {
    TsFileInput input = Mockito.mock(TsFileInput.class);
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    Mockito.when(input.read(buffer))
        .thenReturn(0)
        .thenAnswer(
            invocation -> {
              buffer.put(new byte[] {1, 2, 3, 4});
              return Integer.BYTES;
            });

    Assert.assertEquals(Integer.BYTES, ReadWriteIOUtils.readAsPossible(input, buffer));
    Mockito.verify(input, Mockito.times(2)).read(buffer);
  }

  @Test
  public void testReadAsPossibleRetriesZeroForLimitedRead() throws IOException {
    TsFileInput input = Mockito.mock(TsFileInput.class);
    ByteBuffer buffer = ByteBuffer.allocate(2 * Integer.BYTES);
    Mockito.when(input.read(buffer, 11L))
        .thenReturn(0)
        .thenAnswer(
            invocation -> {
              buffer.put(new byte[] {1, 2, 3, 4});
              return Integer.BYTES;
            });

    Assert.assertEquals(Integer.BYTES, ReadWriteIOUtils.readAsPossible(input, buffer, 11L, 4));
    Assert.assertEquals(2 * Integer.BYTES, buffer.limit());
    Mockito.verify(input, Mockito.times(2)).read(buffer, 11L);
  }

  @Test
  public void testFixedWidthReadersRejectEveryTruncatedLength() {
    for (int length = 0; length < Integer.BYTES; length++) {
      final byte[] truncated = new byte[length];
      Assert.assertThrows(
          IOException.class, () -> ReadWriteIOUtils.readInt(new ByteArrayInputStream(truncated)));
    }
    for (int length = 0; length < Short.BYTES; length++) {
      final byte[] truncated = new byte[length];
      Assert.assertThrows(
          IOException.class, () -> ReadWriteIOUtils.readShort(new ByteArrayInputStream(truncated)));
    }
    for (int length = 0; length < Float.BYTES; length++) {
      final byte[] truncated = new byte[length];
      Assert.assertThrows(
          IOException.class, () -> ReadWriteIOUtils.readFloat(new ByteArrayInputStream(truncated)));
    }
    for (int length = 0; length < Double.BYTES; length++) {
      final byte[] truncated = new byte[length];
      Assert.assertThrows(
          IOException.class,
          () -> ReadWriteIOUtils.readDouble(new ByteArrayInputStream(truncated)));
    }
  }

  @Test
  public void testSingleByteReadersRejectEof() {
    InputStream input = new ByteArrayInputStream(new byte[0]);
    Assert.assertThrows(EOFException.class, () -> ReadWriteIOUtils.readByte(input));
    Assert.assertThrows(EOFException.class, () -> ReadWriteIOUtils.readBool(input));
    Assert.assertThrows(EOFException.class, () -> ReadWriteIOUtils.readBoolean(input));
  }

  private static final class FragmentedInputStream extends ByteArrayInputStream {
    private final int maxReadLength;
    private boolean returnZeroOnce = true;

    private FragmentedInputStream(byte[] bytes, int maxReadLength) {
      super(bytes);
      this.maxReadLength = maxReadLength;
    }

    @Override
    public synchronized int read(byte[] bytes, int offset, int length) {
      if (returnZeroOnce) {
        returnZeroOnce = false;
        return 0;
      }
      return super.read(bytes, offset, Math.min(length, maxReadLength));
    }
  }
}
