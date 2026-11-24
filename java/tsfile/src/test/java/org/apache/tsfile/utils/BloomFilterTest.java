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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.FullPath;
import org.apache.tsfile.read.common.Path;

import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class BloomFilterTest {

  @Test
  public void testIn() {
    String value1 = "device1.s1";
    String value2 = "device1.s2";
    String value3 = "device1.s3";
    BloomFilter filter = BloomFilter.getEmptyBloomFilter(0.05, 3);
    filter.add(value1);
    filter.add(value2);
    filter.add(value3);

    assertTrue(filter.contains(value1));
    assertTrue(filter.contains(value2));
    assertTrue(filter.contains(value3));
    System.out.println(filter.contains("12iuedyauydua"));
    System.out.println(filter.contains("device_1.s1"));
    System.out.println(filter.contains("device1.s_2"));
    System.out.println(filter.contains("device2.s1"));
    System.out.println(filter.contains("device3.s2"));
    System.out.println(filter.contains("device4.s2"));
    System.out.println(filter.contains("device1.s4"));
  }

  @Test
  public void testSerialize() {
    String value1 = "device1.s1";
    String value2 = "device1.s2";
    String value3 = "device1.s3";
    BloomFilter filter = BloomFilter.getEmptyBloomFilter(0.05, 3);
    filter.add(value1);
    filter.add(value2);
    filter.add(value3);

    BloomFilter filter1 =
        BloomFilter.buildBloomFilter(
            filter.serialize(), filter.getSize(), filter.getHashFunctionSize());
    assertTrue(filter1.contains(value1));
    assertTrue(filter1.contains(value2));
    assertTrue(filter1.contains(value3));
  }

  @Test
  public void testBatchMatchesIndividualPaths() {
    List<String> measurements =
        Arrays.asList("传感器.温度", null, "absent", "", "s".repeat(80), "x", "传感器.温度");
    IDeviceID.Factory devices = IDeviceID.Factory.DEFAULT_FACTORY;
    for (IDeviceID device :
        Arrays.asList(
            devices.create("d"),
            devices.create("root.区域." + "a".repeat(70)),
            devices.create(new String[] {"table", null, "region.1", "传感器"}))) {
      BloomFilter filter = BloomFilter.getEmptyBloomFilter(0.05, 100);
      filter.add(new FullPath(device, ""));
      filter.add(new FullPath(device, "传感器.温度"));
      filter.add(new FullPath(device, "x"));
      assertBatchMatchesSingles(filter, device, measurements);
      assertBatchMatchesSingles(
          filter, device, Collections.unmodifiableSet(new LinkedHashSet<>(measurements)));
      assertBatchMatchesSingles(
          filter, IDeviceID.Factory.DEFAULT_FACTORY.create("other"), measurements);
      assertTrue(filter.mightContainBatch(device, Collections.emptySet()).isEmpty());
      assertTrue(
          BloomFilter.getEmptyBloomFilter(0.05, 1)
              .mightContainBatch(device, Collections.singleton(""))
              .isEmpty());
    }
  }

  @Test
  public void testBatchChecksBothEmptyDevicePathForms() {
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("");
    List<String> measurements = Arrays.asList("s1", "", null, "missing", "s1");
    BitSet expected = new BitSet();
    expected.set(0);
    expected.set(1);
    expected.set(4);
    for (boolean useSeparator : new boolean[] {false, true}) {
      BloomFilter filter = BloomFilter.getEmptyBloomFilter(0.01, 100);
      for (String measurement : Arrays.asList("s1", "")) {
        Path path =
            useSeparator ? new FullPath(device, measurement) : new Path("", measurement, true);
        filter.add(path);
      }
      assertEquals(expected, filter.mightContainBatch(device, measurements));
    }
  }

  private static void assertBatchMatchesSingles(
      BloomFilter filter, IDeviceID device, Collection<String> measurements) {
    BitSet expected = new BitSet();
    int index = 0;
    for (String measurement : measurements) {
      expected.set(
          index++, measurement != null && filter.contains(new FullPath(device, measurement)));
    }
    assertEquals(expected, filter.mightContainBatch(device, measurements));
  }

  @Test
  public void testPrefixHashAtBlockBoundaries() {
    // The optimized reader is used when the scalar API encodes paths as UTF-8.
    assumeTrue(StandardCharsets.UTF_8.equals(Charset.defaultCharset()));
    int[] seeds = {5, 7, 11, 19, 31, 37, 43, 59};
    for (int i = 0; i < 48; i++) {
      String prefix = "区域." + "p".repeat(i) + ".";
      Murmur128Hash.PrefixHasher hasher =
          new Murmur128Hash.PrefixHasher(
              prefix.getBytes(StandardCharsets.UTF_8), seeds, seeds.length);
      for (int j = 31; j >= 0; j--) {
        String measurement = "é".repeat(j) + "温度";
        hasher.setSuffix(measurement.getBytes(StandardCharsets.UTF_8));
        for (int k = 0; k < seeds.length; k++) {
          assertEquals(Murmur128Hash.hash(prefix + measurement, seeds[k]), hasher.hash(k));
        }
      }
    }
  }

  @Test
  public void testGrowingSuffixesWithLateSeedLookup() {
    assumeTrue(StandardCharsets.UTF_8.equals(Charset.defaultCharset()));
    String prefix = "p".repeat(159) + ".";
    int[] seeds = {5, 7};
    Murmur128Hash.PrefixHasher hasher =
        new Murmur128Hash.PrefixHasher(
            prefix.getBytes(StandardCharsets.UTF_8), seeds, seeds.length);
    for (int length = 1; length <= 64; length++) {
      String suffix = "s".repeat(length);
      hasher.setSuffix(suffix.getBytes(StandardCharsets.UTF_8));
      assertEquals(Murmur128Hash.hash(prefix + suffix, seeds[0]), hasher.hash(0));
    }
    hasher.setSuffix(new byte[0]);
    assertEquals(Murmur128Hash.hash(prefix, seeds[1]), hasher.hash(1));
    assertEquals(Murmur128Hash.hash(prefix, seeds[0]), hasher.hash(0));
  }

  @Test
  public void testBatchAgainstExistingFilter() {
    // Generated by the snapshot writer for root.d., root.d.s1 and root.d.temp.
    assumeTrue(Arrays.equals("root.d.".getBytes(), "root.d.".getBytes(StandardCharsets.US_ASCII)));
    BloomFilter filter =
        snapshotFilter(256, 4, 8, 51, 90, 96, 128, 157, 192, 193, 209, 229, 234, 237, 242, 245);
    BitSet expected = new BitSet();
    expected.set(0, 3);
    assertEquals(
        expected,
        filter.mightContainBatch(
            IDeviceID.Factory.DEFAULT_FACTORY.create("root.d"),
            Arrays.asList("", "s1", "temp", "absent")));
  }

  @Test
  public void testBatchAgainstExistingMultiBlockFilter() {
    // Snapshot bits for the time column, 温度 and load under the same multi-block UTF-8 prefix.
    assumeTrue(StandardCharsets.UTF_8.equals(Charset.defaultCharset()));
    BloomFilter filter =
        snapshotFilter(
            1024, 9, 26, 228, 298, 337, 362, 384, 425, 440, 457, 470, 705, 836, 837, 938);
    IDeviceID device =
        IDeviceID.Factory.DEFAULT_FACTORY.create("root.factory.production_line_009.区域.station_43");
    BitSet expected = new BitSet();
    expected.set(0, 3);
    assertEquals(
        expected, filter.mightContainBatch(device, Arrays.asList("温度", "load", "", "absent")));
  }

  private static BloomFilter snapshotFilter(int size, int... setBits) {
    BitSet bits = new BitSet(size);
    for (int bit : setBits) {
      bits.set(bit);
    }
    return BloomFilter.buildBloomFilter(bits.toByteArray(), size, 5);
  }
}
