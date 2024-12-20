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

import org.apache.tsfile.file.metadata.idcolumn.FourOrHigherLevelDBExtractor;
import org.apache.tsfile.file.metadata.idcolumn.ThreeLevelDBExtractor;
import org.apache.tsfile.file.metadata.idcolumn.TwoLevelDBExtractor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TreeDeviceIdColumnValueExtractorTest {

  @Test
  public void testTwoLevelDBExtractor() {
    // db name is root.db whose name length is 7
    TwoLevelDBExtractor extractor = new TwoLevelDBExtractor(7);
    // case 1: device is root.db whose DeviceId is {root, db}
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.db");
    assertNull(extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // case 2: device is root.db.d1 whose DeviceId is {root.db, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.d1");
    assertEquals("d1", extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // case 3: device is root.db.a.d1 whose DeviceId is {root.db.a, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.a.d1");
    assertEquals("a", extractor.extract(deviceID, 0));
    assertEquals("d1", extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // case 3: device is root.db.a.b.d1 whose DeviceId is {root.db.a, b, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.db.a.b.d1");
    assertEquals("a", extractor.extract(deviceID, 0));
    assertEquals("b", extractor.extract(deviceID, 1));
    assertEquals("d1", extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
  }

  @Test
  public void testThreeLevelDBExtractor() {
    // db name is root.a.db whose name length is 9
    ThreeLevelDBExtractor extractor = new ThreeLevelDBExtractor(9);
    // case 1: device is root.a.db whose DeviceId is {root.a, db}
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.db");
    assertNull(extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // case 2: device is root.a.db.d1 whose DeviceId is {root.a.db, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.db.d1");
    assertEquals("d1", extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // case 2: device is root.a.db.b.d1 whose DeviceId is {root.a.db, b, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.db.b.d1");
    assertEquals("b", extractor.extract(deviceID, 0));
    assertEquals("d1", extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // case 2: device is root.a.db.b.c.d1 whose DeviceId is {root.a.db, b, c, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.db.b.c.d1");
    assertEquals("b", extractor.extract(deviceID, 0));
    assertEquals("c", extractor.extract(deviceID, 1));
    assertEquals("d1", extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
  }

  @Test
  public void testFourOrHigherLevelDBExtractor() {
    // db name is root.a.b.db whose level number is 4
    FourOrHigherLevelDBExtractor extractor = new FourOrHigherLevelDBExtractor(4);
    // device is root.a.b.db whose DeviceId is {root.a.b, db}
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.db");
    assertNull(extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
    // device is root.a.b.db.d1 whose DeviceId is {root.a.b, db, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.db.d1");
    assertEquals("d1", extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
    // device is root.a.b.db.b.d1 whose DeviceId is {root.a.b, db, b, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.db.b.d1");
    assertEquals("b", extractor.extract(deviceID, 0));
    assertEquals("d1", extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
    // device is root.a.b.db.b.c.d1 whose DeviceId is {root.a.b, db, b, c, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.db.b.c.d1");
    assertEquals("b", extractor.extract(deviceID, 0));
    assertEquals("c", extractor.extract(deviceID, 1));
    assertEquals("d1", extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // db name is root.a.b.c.db whose level number is 5
    extractor = new FourOrHigherLevelDBExtractor(5);
    // device is root.a.b.c.db whose DeviceId is {root.a.b, c, db}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.c.db");
    assertNull(extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
    // device is root.a.b.c.db.d1 whose DeviceId is {root.a.b, c, db, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.c.db.d1");
    assertEquals("d1", extractor.extract(deviceID, 0));
    assertNull(extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
    // device is root.a.b.c.db.b.d1 whose DeviceId is {root.a.b, c, db, b, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.c.db.b.d1");
    assertEquals("b", extractor.extract(deviceID, 0));
    assertEquals("d1", extractor.extract(deviceID, 1));
    assertNull(extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));
    // device is root.a.b.c.db.b.c.d1 whose DeviceId is {root.a.b, c, db, b, c, d1}
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.a.b.c.db.b.c.d1");
    assertEquals("b", extractor.extract(deviceID, 0));
    assertEquals("c", extractor.extract(deviceID, 1));
    assertEquals("d1", extractor.extract(deviceID, 2));
    assertNull(extractor.extract(deviceID, 3));

    // db name is root.db whose level number is 2, should throw exception
    try {
      extractor = new FourOrHigherLevelDBExtractor(2);
      fail("expected exception");
    } catch (IllegalArgumentException e) {
      assertEquals("db level number must be >= 4", e.getMessage());
    }
    try {
      extractor = new FourOrHigherLevelDBExtractor(3);
      fail("expected exception");
    } catch (IllegalArgumentException e) {
      assertEquals("db level number must be >= 4", e.getMessage());
    }
  }
}
