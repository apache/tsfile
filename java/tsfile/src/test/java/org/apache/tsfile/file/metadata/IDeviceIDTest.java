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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.file.metadata.IDeviceID.Deserializer;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IDeviceIDTest {

  @Test
  public void testStartWith() {
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.a.b.c.d");
    assertTrue(deviceID.startWith("root.a"));
    assertTrue(deviceID.startWith("root.a."));
    assertTrue(deviceID.startWith("root.a.b"));
    assertTrue(deviceID.startWith("root.a.b."));
    assertTrue(deviceID.startWith("root.a.b.c"));
    assertTrue(deviceID.startWith("root.a.b.c.d"));

    assertFalse(deviceID.startWith("root.b"));
    assertFalse(deviceID.startWith("root.a.b.d"));
    assertFalse(deviceID.startWith("root.a.b.c.e"));

    assertFalse(deviceID.startWith("root.a.bb"));
    assertFalse(deviceID.startWith("root.a.b.cc"));
    assertFalse(deviceID.startWith("root.a.b.c.dd"));

    assertFalse(deviceID.startWith("root.a.b.c.."));
    assertFalse(deviceID.startWith("root.a.b.c.d."));
    assertFalse(deviceID.startWith("root.a.b.c.d.e"));
    assertFalse(deviceID.startWith("root.a..b.c"));

    deviceID = Factory.DEFAULT_FACTORY.create("root.aaaa.b.c.d");
    assertTrue(deviceID.startWith("root.a"));
  }

  @Test
  public void testMatchDatabaseName() {
    IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.a.b.c.d");
    assertTrue(deviceID.matchDatabaseName("root.a"));
    assertFalse(deviceID.matchDatabaseName("root.a."));
    assertTrue(deviceID.matchDatabaseName("root.a.b"));
    assertFalse(deviceID.matchDatabaseName("root.a.b."));
    assertTrue(deviceID.matchDatabaseName("root.a.b.c"));
    assertTrue(deviceID.matchDatabaseName("root.a.b.c.d"));

    assertFalse(deviceID.matchDatabaseName("root.b"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.d"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.e"));

    assertFalse(deviceID.matchDatabaseName("root.a.bb"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.cc"));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.dd"));

    assertFalse(deviceID.matchDatabaseName("root.a.b.c.."));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.d."));
    assertFalse(deviceID.matchDatabaseName("root.a.b.c.d.e"));
    assertFalse(deviceID.matchDatabaseName("root.a..b.c"));

    deviceID = Factory.DEFAULT_FACTORY.create("root.aaaa.b.c.d");
    assertFalse(deviceID.matchDatabaseName("root.a"));
  }

  @Test
  public void testSerialize() throws IOException {
    testSerialize(Factory.DEFAULT_FACTORY.create("root"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a.b"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a.b.c"));
    testSerialize(Factory.DEFAULT_FACTORY.create("root.a.b.c.d"));
    testSerialize(Factory.DEFAULT_FACTORY.create(new String[] {"root", "a", null, "c", "d"}));
  }

  private void testSerialize(IDeviceID deviceID) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(deviceID.serializedSize());
    deviceID.serialize(buffer);
    buffer.flip();
    IDeviceID deserialized = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
    assertEquals(deserialized, deviceID);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    deviceID.serialize(byteArrayOutputStream);
    assertEquals(deviceID.serializedSize(), byteArrayOutputStream.size());
    buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    deserialized = Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(buffer);
    assertEquals(deserialized, deviceID);
  }
}
