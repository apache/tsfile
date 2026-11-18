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

  @Test
  public void testGetChildIndexEntries() {
    List<IMetadataIndexEntry> list = new ArrayList<>();
    list.add(new MeasurementMetadataIndexEntry("a", 0L));
    list.add(new MeasurementMetadataIndexEntry("c", 10L));
    list.add(new MeasurementMetadataIndexEntry("f", 20L));
    list.add(new MeasurementMetadataIndexEntry("m", 30L));
    list.add(new MeasurementMetadataIndexEntry("z", 40L));
    MetadataIndexNode node =
        new MetadataIndexNode(list, 50L, MetadataIndexNodeType.LEAF_MEASUREMENT);

    // few keys: monotonic binary search, each key gets the floor child and its end offset
    List<Pair<IMetadataIndexEntry, Long>> pairs =
        node.getChildIndexEntries(Arrays.asList("b", "d"), false);
    Assert.assertEquals(2, pairs.size());
    Assert.assertEquals(0L, pairs.get(0).getLeft().getOffset());
    Assert.assertEquals(10L, pairs.get(0).getRight().longValue());
    Assert.assertEquals(10L, pairs.get(1).getLeft().getOffset());
    Assert.assertEquals(20L, pairs.get(1).getRight().longValue());

    // many keys: merge search, consecutive keys of one child share the same pair instance
    List<Pair<IMetadataIndexEntry, Long>> merged =
        node.getChildIndexEntries(Arrays.asList("a", "b", "c", "d", "e"), false);
    Assert.assertEquals(5, merged.size());
    Assert.assertSame(merged.get(0), merged.get(1));
    Assert.assertSame(merged.get(2), merged.get(4));
    Assert.assertNotSame(merged.get(1), merged.get(2));
    Assert.assertEquals(10L, merged.get(2).getLeft().getOffset());
    Assert.assertEquals(20L, merged.get(4).getRight().longValue());

    // a key before the first child has no floor, and the last child ends at the node end offset
    Assert.assertNull(node.getChildIndexEntries(Collections.singletonList("0"), false).get(0));
    Assert.assertEquals(
        50L,
        node.getChildIndexEntries(Collections.singletonList("zz"), false)
            .get(0)
            .getRight()
            .longValue());

    // exact search returns null for a missing key
    Assert.assertNull(node.getChildIndexEntries(Collections.singletonList("e"), true).get(0));

    // duplicate keys stay eligible in both search strategies
    int[] duplicates =
        node.getChildIndexEntries(Arrays.asList("c", "c"), true).stream()
            .mapToInt(pair -> node.getChildren().indexOf(pair.getLeft()))
            .toArray();
    Assert.assertArrayEquals(new int[] {1, 1}, duplicates);
    int[] mergedDuplicates =
        node.getChildIndexEntries(Arrays.asList("c", "c", "f", "f", "m"), true).stream()
            .mapToInt(pair -> node.getChildren().indexOf(pair.getLeft()))
            .toArray();
    Assert.assertArrayEquals(new int[] {1, 1, 2, 2, 3}, mergedDuplicates);

    // an empty node has no floor entry
    MetadataIndexNode empty =
        new MetadataIndexNode(new ArrayList<>(), 0L, MetadataIndexNodeType.LEAF_MEASUREMENT);
    Assert.assertNull(empty.getChildIndexEntries(Collections.singletonList("s1"), false).get(0));
  }

  @Test
  public void testGetChildIndexEntriesComparesChildrenWithMixedDeviceIds() {
    // plain device ids cannot be compared to a string-array child in this direction, so the
    // batch search must compare the child to the key, like the binary searches do
    List<IMetadataIndexEntry> children = new ArrayList<>();
    children.add(new DeviceMetadataIndexEntry(new StringArrayDeviceID("d1"), 0L));
    children.add(new DeviceMetadataIndexEntry(new StringArrayDeviceID("d2"), 10L));
    children.add(new DeviceMetadataIndexEntry(new StringArrayDeviceID("d3"), 20L));
    MetadataIndexNode node =
        new MetadataIndexNode(children, 30L, MetadataIndexNodeType.LEAF_MEASUREMENT);

    List<PlainDeviceID> keys =
        Arrays.asList(new PlainDeviceID("d1"), new PlainDeviceID("d2"), new PlainDeviceID("d4"));
    List<Pair<IMetadataIndexEntry, Long>> pairs = node.getChildIndexEntries(keys, false);
    Assert.assertEquals(keys.size(), pairs.size());
    for (int i = 0; i < keys.size(); i++) {
      Pair<IMetadataIndexEntry, Long> expected = node.getChildIndexEntry(keys.get(i), false);
      Assert.assertEquals(expected.getLeft().getOffset(), pairs.get(i).getLeft().getOffset());
      Assert.assertEquals(expected.getRight(), pairs.get(i).getRight());
    }
  }
}
