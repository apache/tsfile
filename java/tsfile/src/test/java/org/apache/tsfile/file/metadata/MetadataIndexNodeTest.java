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

import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MetadataIndexNodeTest {

  @Test
  public void testBinarySearchInChildren() {
    List<IMetadataIndexEntry> list = new ArrayList<>();
    list.add(new MeasurementMetadataIndexEntry("a", -1L));
    list.add(new MeasurementMetadataIndexEntry("c", -1L));
    list.add(new MeasurementMetadataIndexEntry("f", -1L));
    list.add(new MeasurementMetadataIndexEntry("m", -1L));
    list.add(new MeasurementMetadataIndexEntry("z", -1L));

    MetadataIndexNode metadataIndexNode =
        new MetadataIndexNode(list, -1L, MetadataIndexNodeType.LEAF_MEASUREMENT);
    Assert.assertEquals(0, metadataIndexNode.binarySearchInChildren("a", false));
    Assert.assertEquals(2, metadataIndexNode.binarySearchInChildren("f", false));
    Assert.assertEquals(2, metadataIndexNode.binarySearchInChildren("g", false));
    Assert.assertEquals(4, metadataIndexNode.binarySearchInChildren("zz", false));
    Assert.assertEquals(-1, metadataIndexNode.binarySearchInChildren("g", true));
    Assert.assertEquals(-1, metadataIndexNode.binarySearchInChildren("zz", true));
  }
}
