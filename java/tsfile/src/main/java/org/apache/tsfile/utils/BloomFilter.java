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
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.common.FullPath;
import org.apache.tsfile.read.common.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfLongArray;

public class BloomFilter {

  private static final int INSTANCE_SIZE =
      (int)
          (RamUsageEstimator.shallowSizeOfInstance(BloomFilter.class)
              + RamUsageEstimator.shallowSizeOfInstance(BitSet.class));

  private static final int MINIMAL_SIZE = 256;
  private static final int MAXIMAL_HASH_FUNCTION_SIZE = 8;
  private static final int[] SEEDS = new int[] {5, 7, 11, 19, 31, 37, 43, 59};

  private final int size;
  private final int hashFunctionSize;
  private final BitSet bits;
  private final boolean useCheapHash;

  // do not try to initialize the filter by construction method
  private BloomFilter(byte[] bytes, int size, int hashFunctionSize) {
    this.size = size;
    this.hashFunctionSize = hashFunctionSize;
    bits = BitSet.valueOf(bytes);
    useCheapHash = false;
  }

  private BloomFilter(int size, int hashFunctionSize) {
    this.size = size;
    this.hashFunctionSize = hashFunctionSize;
    bits = new BitSet(size);
    useCheapHash = false;
  }

  private BloomFilter(byte[] bytes, int size, int hashFunctionSize, boolean useCheapHash) {
    this.size = size;
    this.hashFunctionSize = hashFunctionSize;
    bits = BitSet.valueOf(bytes);
    this.useCheapHash = useCheapHash;
  }

  private BloomFilter(int size, int hashFunctionSize, boolean useCheapHash) {
    this.size = size;
    this.hashFunctionSize = hashFunctionSize;
    bits = new BitSet(size);
    this.useCheapHash = useCheapHash;
  }

  /**
   * get empty bloom filter
   *
   * @param errorPercent the tolerant percent of error of the bloom filter
   * @param numOfString the number of string want to store in the bloom filter
   * @return empty bloom
   */
  public static BloomFilter getEmptyBloomFilter(double errorPercent, int numOfString) {
    errorPercent = Math.max(errorPercent, TSFileConfig.MIN_BLOOM_FILTER_ERROR_RATE);
    errorPercent = Math.min(errorPercent, TSFileConfig.MAX_BLOOM_FILTER_ERROR_RATE);

    double ln2 = Math.log(2);
    int size = (int) (-numOfString * Math.log(errorPercent) / ln2 / ln2) + 1;
    int hashFunctionSize = (int) (-Math.log(errorPercent) / ln2) + 1;
    return new BloomFilter(
        Math.max(MINIMAL_SIZE, size), Math.min(MAXIMAL_HASH_FUNCTION_SIZE, hashFunctionSize));
  }

  /**
   * get empty bloom filter
   *
   * @param errorPercent the tolerant percent of error of the bloom filter
   * @param numOfString the number of string want to store in the bloom filter
   * @return empty bloom
   */
  public static BloomFilter getEmptyBloomFilterWithCheapHash(double errorPercent, int numOfString) {
    errorPercent = Math.max(errorPercent, TSFileConfig.MIN_BLOOM_FILTER_ERROR_RATE);
    errorPercent = Math.min(errorPercent, TSFileConfig.MAX_BLOOM_FILTER_ERROR_RATE);

    double ln2 = Math.log(2);
    int size = (int) (-numOfString * Math.log(errorPercent) / ln2 / ln2) + 1;
    int hashFunctionSize = (int) (-Math.log(errorPercent) / ln2) + 1;
    return new BloomFilter(
        Math.max(MINIMAL_SIZE, size), Math.min(MAXIMAL_HASH_FUNCTION_SIZE, hashFunctionSize), true);
  }

  /**
   * build bloom filter by bytes
   *
   * @param bytes bytes of bits
   * @return bloom filter
   */
  public static BloomFilter buildBloomFilter(byte[] bytes, int size, int hashFunctionSize) {
    return new BloomFilter(bytes, size, Math.min(MAXIMAL_HASH_FUNCTION_SIZE, hashFunctionSize));
  }

  /**
   * build bloom filter by bytes
   *
   * @param bytes bytes of bits
   * @return bloom filter
   */
  public static BloomFilter buildBloomFilterWithCheapHash(
      byte[] bytes, int size, int hashFunctionSize) {
    return new BloomFilter(
        bytes, size, Math.min(MAXIMAL_HASH_FUNCTION_SIZE, hashFunctionSize), true);
  }

  public int getHashFunctionSize() {
    return hashFunctionSize;
  }

  public int getSize() {
    return size;
  }

  public void add(Path path) {
    if (!useCheapHash) {
      add(path.getFullPath());
    } else {
      for (int i = 0; i < hashFunctionSize; i++) {
        bits.set(hash(path, size, SEEDS[i]), true);
      }
    }
  }

  public void add(String value) {
    for (int i = 0; i < hashFunctionSize; i++) {
      bits.set(hash(value, size, SEEDS[i]), true);
    }
  }

  public boolean contains(Path value) {
    if (value == null) {
      return false;
    }
    boolean ret = true;
    int index = 0;
    while (ret && index < hashFunctionSize) {
      if (!useCheapHash) {
        ret = bits.get(hash(value.getFullPath(), size, SEEDS[index++]));
      } else {
        if (!(value instanceof FullPath)) {
          value = new FullPath(value.getIDeviceID(), value.getMeasurement());
        }
        ret = bits.get(hash(value, size, SEEDS[index++]));
      }
    }

    return ret;
  }

  @Deprecated
  public boolean contains(String value) {
    if (value == null) {
      return false;
    }
    boolean ret = true;
    int index = 0;
    while (ret && index < hashFunctionSize) {
      ret = bits.get(hash(value, size, SEEDS[index++]));
    }

    return ret;
  }

  public byte[] serialize() {
    return bits.toByteArray();
  }

  public long getRetainedSizeInBytes() {
    // calculated according to the implementation of BitSet, bits.size() returns words.length *
    // BITS_PER_WORD and BITS_PER_WORD = (1 << 6) which is 64
    return INSTANCE_SIZE + sizeOfLongArray(bits.size() / 64);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BloomFilter that = (BloomFilter) o;
    return size == that.size
        && hashFunctionSize == that.hashFunctionSize
        && Objects.equals(bits, that.bits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, hashFunctionSize, bits);
  }

  private static int hash(String value, int cap, int seed) {
    int res = Murmur128Hash.hash(value, seed);
    if (res == Integer.MIN_VALUE) {
      res = 0;
    }

    return Math.abs(res) % cap;
  }

  private static int hash(Path value, int cap, int seed) {
    int res = Objects.hash(value, seed);
    if (res == Integer.MIN_VALUE) {
      res = 0;
    }

    return Math.abs(res) % cap;
  }

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byte[] bytes = serialize();
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, outputStream);
    if (bytes.length > 0) {
      outputStream.write(bytes);
      byteLen += bytes.length;
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(getSize(), outputStream);
      if (!useCheapHash) {
        byteLen +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(getHashFunctionSize(), outputStream);
      } else {
        byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(Integer.MAX_VALUE, outputStream);
        byteLen +=
            ReadWriteForEncodingUtils.writeUnsignedVarInt(getHashFunctionSize(), outputStream);
      }
    }
    return byteLen;
  }

  public static BloomFilter deserialize(ByteBuffer buffer) {
    byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
    if (bytes.length != 0) {
      int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      if (hashFunctionSize != Integer.MAX_VALUE) {
        return BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize);
      } else {
        hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        return BloomFilter.buildBloomFilterWithCheapHash(bytes, filterSize, hashFunctionSize);
      }
    }
    return null;
  }

  public static void main(String[] args) {
    int deviceNum = 100;
    int measurementNum = 10000;
    List<Path> pathList = new ArrayList<>();
    for (int i = 0; i < deviceNum; i++) {
      IDeviceID deviceID = Factory.DEFAULT_FACTORY.create("root.db1.d" + i);
      for (int j = 0; j < measurementNum; j++) {
        String measurement = "s" + j;
        pathList.add(new FullPath(deviceID, measurement));
      }
    }

    int repeat = 100;
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < repeat; i++) {
      //      BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(
      //          TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(), deviceNum *
      // measurementNum);
      BloomFilter bloomFilter =
          BloomFilter.getEmptyBloomFilterWithCheapHash(
              TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate(),
              deviceNum * measurementNum);
      pathList.forEach(bloomFilter::add);
    }
    System.out.println(System.currentTimeMillis() - startTime);
  }
}
