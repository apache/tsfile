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

import org.apache.tsfile.common.conf.TSFileConfig;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class ReadWriteIOUtilsTest {

  protected static final int DEFAULT_BUFFER_SIZE = 4096;

  @Test
  public void testReadStringRejectsOversizedLength() {
    for (int length : new int[] {2, Integer.MAX_VALUE}) {
      ByteBuffer buffer = ByteBuffer.allocate(5).putInt(length).put((byte) 'a');
      buffer.flip();
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readString(buffer));
    }
  }

  @Test
  public void testReadBinaryRejectsOversizedLength() {
    for (int length : new int[] {2, Integer.MAX_VALUE}) {
      ByteBuffer buffer = ByteBuffer.allocate(5).putInt(length).put((byte) 1);
      buffer.flip();
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readBinary(buffer));
    }
  }

  @Test
  public void testLengthPrefixedValuesRejectTruncatedLength() {
    for (int size = 0; size < Integer.BYTES; size++) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readString(buffer.duplicate()));
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readBinary(buffer.duplicate()));
    }
  }

  @Test
  public void testLengthPrefixedValuesRespectBufferLimit() {
    for (boolean direct : new boolean[] {false, true}) {
      ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(32) : ByteBuffer.allocate(32);
      buffer.putInt(7).putInt(5).put((byte) 'a').flip();
      buffer.position(Integer.BYTES);
      for (ByteBuffer view : new ByteBuffer[] {buffer, buffer.asReadOnlyBuffer()}) {
        Assert.assertThrows(
            BufferUnderflowException.class, () -> ReadWriteIOUtils.readString(view.duplicate()));
        Assert.assertThrows(
            BufferUnderflowException.class, () -> ReadWriteIOUtils.readBinary(view.duplicate()));
        Assert.assertEquals(Integer.BYTES, view.position());
        Assert.assertEquals(9, view.limit());
      }
    }
  }

  @Test
  public void testExplicitLengthsRespectRemainingBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(32);
    buffer.putInt(7).put((byte) 'a').flip();
    buffer.position(Integer.BYTES);
    for (int length : new int[] {2, Integer.MAX_VALUE}) {
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readBytes(buffer, length));
      Assert.assertThrows(
          BufferUnderflowException.class,
          () -> ReadWriteIOUtils.readStringWithLength(buffer, length));
      Assert.assertEquals(Integer.BYTES, buffer.position());
    }
  }

  @Test
  public void testReadVarIntStringRejectsOversizedLength() {
    for (int length : new int[] {2, Integer.MAX_VALUE}) {
      ByteBuffer buffer = ByteBuffer.allocate(32);
      ReadWriteForEncodingUtils.writeVarInt(length, buffer);
      buffer.put((byte) 'a').flip();
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readVarIntString(buffer));
    }
  }

  @Test
  public void testReadSelfDescribedBytesRejectsOversizedLength() {
    for (int length : new int[] {2, Integer.MAX_VALUE}) {
      ByteBuffer buffer = ByteBuffer.allocate(32);
      ReadWriteForEncodingUtils.writeUnsignedVarInt(length, buffer);
      buffer.put((byte) 1).flip();
      Assert.assertThrows(
          BufferUnderflowException.class,
          () -> ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer));
    }
  }

  @Test
  public void testMapReadersRejectInvalidEntryCount() {
    for (int count : new int[] {-2, 2, Integer.MAX_VALUE}) {
      ByteBuffer buffer = ByteBuffer.allocate(12).putInt(count).putInt(-1).putInt(-1);
      buffer.flip();
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readMap(buffer.duplicate()));
      Assert.assertThrows(
          BufferUnderflowException.class,
          () -> ReadWriteIOUtils.readLinkedHashMap(buffer.duplicate()));
    }
  }

  @Test
  public void testMapReadersRejectOversizedStrings() {
    for (ByteBuffer buffer :
        new ByteBuffer[] {
          ByteBuffer.allocate(12).putInt(1).putInt(Integer.MAX_VALUE).putInt(0),
          ByteBuffer.allocate(12).putInt(1).putInt(0).putInt(Integer.MAX_VALUE)
        }) {
      buffer.flip();
      Assert.assertThrows(
          BufferUnderflowException.class, () -> ReadWriteIOUtils.readMap(buffer.duplicate()));
      Assert.assertThrows(
          BufferUnderflowException.class,
          () -> ReadWriteIOUtils.readLinkedHashMap(buffer.duplicate()));
    }
  }

  @Test
  public void testStringBufferReadersPreserveValuesAndPositions() {
    for (boolean varInt : new boolean[] {false, true}) {
      for (boolean direct : new boolean[] {false, true}) {
        ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(64) : ByteBuffer.allocate(64);
        buffer.putInt(7);
        for (String value : new String[] {null, "", "中文"}) {
          if (varInt) {
            ReadWriteIOUtils.writeVar(value, buffer);
          } else {
            ReadWriteIOUtils.write(value, buffer);
          }
        }
        int end = buffer.position();
        buffer.putInt(77).flip();
        buffer.position(Integer.BYTES);
        for (ByteBuffer view : new ByteBuffer[] {buffer.duplicate(), buffer.asReadOnlyBuffer()}) {
          for (String value : new String[] {null, "", "中文"}) {
            Assert.assertEquals(
                value,
                varInt
                    ? ReadWriteIOUtils.readVarIntString(view)
                    : ReadWriteIOUtils.readString(view));
          }
          Assert.assertEquals(end, view.position());
          Assert.assertEquals(77, view.getInt());
        }
      }
    }
  }

  @Test
  public void testReadStringWithLengthPreservesNullEmptyAndUnicode() {
    byte[] bytes = "中文".getBytes(TSFileConfig.STRING_CHARSET);
    ByteBuffer buffer = ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    Assert.assertNull(ReadWriteIOUtils.readStringWithLength(buffer, -1));
    Assert.assertNull(ReadWriteIOUtils.readStringWithLength(buffer, -2));
    Assert.assertEquals("", ReadWriteIOUtils.readStringWithLength(buffer, 0));
    Assert.assertEquals(0, buffer.position());
    Assert.assertEquals("中文", ReadWriteIOUtils.readStringWithLength(buffer, bytes.length));
    Assert.assertFalse(buffer.hasRemaining());
  }

  @Test
  public void testBinaryBufferReaderPreservesValuesAndPositions() {
    for (boolean direct : new boolean[] {false, true}) {
      ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(32) : ByteBuffer.allocate(32);
      buffer.putInt(7);
      ReadWriteIOUtils.write(new Binary(new byte[0]), buffer);
      ReadWriteIOUtils.write(new Binary(new byte[] {1, 2, 3}), buffer);
      int end = buffer.position();
      buffer.putInt(77).flip();
      buffer.position(Integer.BYTES);
      for (ByteBuffer view : new ByteBuffer[] {buffer.duplicate(), buffer.asReadOnlyBuffer()}) {
        Assert.assertArrayEquals(new byte[0], ReadWriteIOUtils.readBinary(view).getValues());
        Assert.assertArrayEquals(
            new byte[] {1, 2, 3}, ReadWriteIOUtils.readBinary(view).getValues());
        Assert.assertEquals(end, view.position());
        Assert.assertEquals(77, view.getInt());
      }
    }
  }

  @Test
  public void testReadSelfDescribedBytesPreservesValuesAndPositions() {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(0, buffer);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(3, buffer);
    buffer.put(new byte[] {1, 2, 3});
    int end = buffer.position();
    buffer.putInt(77).flip();
    Assert.assertArrayEquals(
        new byte[0], ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer));
    Assert.assertArrayEquals(
        new byte[] {1, 2, 3}, ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer));
    Assert.assertEquals(end, buffer.position());
    Assert.assertEquals(77, buffer.getInt());
  }

  @Test
  public void testNegativeBinaryLengthsRemainRejected() {
    Assert.assertThrows(
        NegativeArraySizeException.class,
        () -> ReadWriteIOUtils.readBinary(ByteBuffer.allocate(4).putInt(0, -1)));
    Assert.assertThrows(
        NegativeArraySizeException.class,
        () -> ReadWriteIOUtils.readBytes(ByteBuffer.allocate(0), -1));
  }

  @Test
  public void testMapReadersPreserveNullEmptyAndOrder() throws IOException {
    Map<String, String> map = new LinkedHashMap<>();
    map.put(null, null);
    map.put("", "");
    map.put("key", "中文");
    for (Map<String, String> value :
        Arrays.asList(null, Collections.<String, String>emptyMap(), map)) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      ReadWriteIOUtils.write(value, stream);
      byte[] bytes = stream.toByteArray();
      ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length + 8);
      buffer.putInt(7).put(bytes).putInt(77).flip();
      buffer.position(Integer.BYTES);
      for (ByteBuffer view : new ByteBuffer[] {buffer.duplicate(), buffer.asReadOnlyBuffer()}) {
        Assert.assertEquals(value, ReadWriteIOUtils.readMap(view.duplicate()));
        Map<String, String> linked = ReadWriteIOUtils.readLinkedHashMap(view);
        Assert.assertEquals(value, linked);
        if (value != null) {
          Assert.assertEquals(new ArrayList<>(value.keySet()), new ArrayList<>(linked.keySet()));
        }
        Assert.assertEquals(Integer.BYTES + bytes.length, view.position());
        Assert.assertEquals(77, view.getInt());
      }
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

    // 2. Chinese value
    str = "中文";
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(str, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readString(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(str, result);

    // 3. null value
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

    // 6. key: chinese; value: chinese
    key = "中文";
    value = "中文";
    map = new HashMap<>();
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

    ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    ReadWriteIOUtils.write(map, buffer);
    buffer.flip();
    result = ReadWriteIOUtils.readMap(buffer);
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 7. null
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
}
