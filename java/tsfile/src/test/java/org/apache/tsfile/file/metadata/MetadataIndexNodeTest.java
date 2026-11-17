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
import org.apache.tsfile.utils.Pair;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MetadataIndexNodeTest {

  @Test
  public void testBinarySearchInChildren() {
    List<IMetadataIndexEntry> list = new ArrayList<>();
    list.add(new MeasurementMetadataIndexEntry("s0", -1L));
    list.add(new MeasurementMetadataIndexEntry("s5", -1L));
    list.add(new MeasurementMetadataIndexEntry("s10", -1L));
    list.add(new MeasurementMetadataIndexEntry("s15", -1L));
    list.add(new MeasurementMetadataIndexEntry("s20", -1L));

    MetadataIndexNode metadataIndexNode =
        new MetadataIndexNode(list, -1L, MetadataIndexNodeType.LEAF_MEASUREMENT);
    Assert.assertEquals(0, metadataIndexNode.binarySearchInChildren("s0", false));
    Assert.assertEquals(2, metadataIndexNode.binarySearchInChildren("s10", false));
    Assert.assertEquals(2, metadataIndexNode.binarySearchInChildren("s13", false));
    Assert.assertEquals(4, metadataIndexNode.binarySearchInChildren("s23", false));
    Assert.assertEquals(-1, metadataIndexNode.binarySearchInChildren("s13", true));
    Assert.assertEquals(-1, metadataIndexNode.binarySearchInChildren("s23", true));
  }

  @Test
  public void testGetChildIndexEntries() {
    List<IMetadataIndexEntry> list = new ArrayList<>();
    list.add(new MeasurementMetadataIndexEntry("s0", 0L));
    list.add(new MeasurementMetadataIndexEntry("s5", 10L));
    list.add(new MeasurementMetadataIndexEntry("s10", 20L));
    list.add(new MeasurementMetadataIndexEntry("s15", 30L));
    list.add(new MeasurementMetadataIndexEntry("s20", 40L));
    MetadataIndexNode node =
        new MetadataIndexNode(list, 50L, MetadataIndexNodeType.LEAF_MEASUREMENT);

    // few keys: monotonic binary search, each key gets the floor child and its end offset
    List<Pair<IMetadataIndexEntry, Long>> pairs =
        node.getChildIndexEntries(Arrays.asList("s1", "s12"), false);
    Assert.assertEquals(2, pairs.size());
    Assert.assertEquals(0L, pairs.get(0).getLeft().getOffset());
    Assert.assertEquals(10L, pairs.get(0).getRight().longValue());
    Assert.assertEquals(20L, pairs.get(1).getLeft().getOffset());
    Assert.assertEquals(30L, pairs.get(1).getRight().longValue());

    // many keys: merge search, consecutive keys of one child share the same pair instance
    List<Pair<IMetadataIndexEntry, Long>> merged =
        node.getChildIndexEntries(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5", "s6"), false);
    Assert.assertEquals(7, merged.size());
    Assert.assertSame(merged.get(0), merged.get(4));
    Assert.assertNotSame(merged.get(0), merged.get(5));
    Assert.assertEquals(10L, merged.get(5).getLeft().getOffset());
    Assert.assertEquals(20L, merged.get(5).getRight().longValue());

    // a key before the first child has no floor, and the last child ends at the node end offset
    Assert.assertNull(node.getChildIndexEntries(Collections.singletonList("a"), false).get(0));
    Assert.assertEquals(
        50L,
        node.getChildIndexEntries(Collections.singletonList("zz"), false)
            .get(0)
            .getRight()
            .longValue());

    // exact search returns null for a missing key
    Assert.assertNull(node.getChildIndexEntries(Collections.singletonList("s13"), true).get(0));

    // an empty node has no floor entry
    MetadataIndexNode empty =
        new MetadataIndexNode(new ArrayList<>(), 0L, MetadataIndexNodeType.LEAF_MEASUREMENT);
    Assert.assertNull(empty.getChildIndexEntries(Collections.singletonList("s1"), false).get(0));
  }
}
