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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.encoding.fire.Fire;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EncoderMemoryEstimateTest {

  @Test
  public void testRetainedSizeIncludesBoxedValuesAndBuffersAfterFlush() throws IOException {
    assertRetainedSize(new IntRleEncoder(), false);
    assertRetainedSize(new LongRleEncoder(), true);
    assertRetainedSize(new IntZigzagEncoder(), false);
    assertRetainedSize(new LongZigzagEncoder(), true);
  }

  @Test
  public void testRleRetainedSizeTracksValuesAfterReuse() throws IOException {
    assertRleRetainedSizeTracksReuse(new IntRleEncoder(), false);
    assertRleRetainedSizeTracksReuse(new LongRleEncoder(), true);
  }

  @Test
  public void testSprintzRetainedSizeTracksValuesAfterReuse() throws IOException {
    assertSprintzRetainedSizeTracksReuse(new IntSprintzEncoder(), TSDataType.INT32);
    assertSprintzRetainedSizeTracksReuse(new LongSprintzEncoder(), TSDataType.INT64);
    assertSprintzRetainedSizeTracksReuse(new FloatSprintzEncoder(), TSDataType.FLOAT);
    assertSprintzRetainedSizeTracksReuse(new DoubleSprintzEncoder(), TSDataType.DOUBLE);
  }

  @Test
  public void testRetainedSizeIsAvailableWithoutMaxByteSize() {
    Encoder encoder =
        new Encoder(TSEncoding.PLAIN) {
          @Override
          public void flush(ByteArrayOutputStream output) {}
        };

    assertEquals(RamUsageEstimator.shallowSizeOf(encoder), encoder.getRetainedSizeInBytes());
  }

  @Test
  public void testSprintzRetainedSizeSupportsByteArrayOutputStreamCache()
      throws ReflectiveOperationException {
    IntSprintzEncoder encoder = new IntSprintzEncoder();
    encoder.byteCache = new ByteArrayOutputStream();
    encoder.byteCache.write(1);

    assertByteArrayOutputStreamCapacity(encoder, SprintzEncoder.class);
  }

  @Test
  public void testRleRetainedSizeSupportsByteArrayOutputStreamCache()
      throws ReflectiveOperationException {
    IntRleEncoder encoder = new IntRleEncoder();
    encoder.byteCache = new ByteArrayOutputStream();
    encoder.byteCache.write(1);

    assertByteArrayOutputStreamCapacity(encoder, RleEncoder.class);
  }

  @Test
  public void testRleFlushPreservesByteArrayOutputStreamSubclassHooks() throws IOException {
    IntRleEncoder encoder = new IntRleEncoder();
    TrackingByteArrayOutputStream byteCache = new TrackingByteArrayOutputStream();
    encoder.byteCache = byteCache;
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    for (int value = 0; value < 16; value++) {
      encoder.encode(value, output);
    }
    for (int value = 0; value < 8; value++) {
      encoder.encode(7, output);
    }
    encoder.flush(output);

    IntRleEncoder baselineEncoder = new IntRleEncoder();
    ByteArrayOutputStream baselineOutput = new ByteArrayOutputStream();
    for (int value = 0; value < 16; value++) {
      baselineEncoder.encode(value, baselineOutput);
    }
    for (int value = 0; value < 8; value++) {
      baselineEncoder.encode(7, baselineOutput);
    }
    baselineEncoder.flush(baselineOutput);

    assertTrue(byteCache.singleByteWrites > 0);
    assertTrue(byteCache.arrayWrites > 0);
    assertEquals(1, byteCache.writeToCalls);
    assertEquals(1, byteCache.resetCalls);
    assertArrayEquals(baselineOutput.toByteArray(), output.toByteArray());
  }

  @Test
  public void testSprintzFlushPreservesByteArrayOutputStreamSubclassHooks() throws IOException {
    IntSprintzEncoder encoder = new IntSprintzEncoder();
    TrackingByteArrayOutputStream byteCache = new TrackingByteArrayOutputStream();
    encoder.byteCache = byteCache;
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    for (int value = 0; value < 9; value++) {
      encoder.encode(value, output);
    }
    encoder.flush(output);

    IntSprintzEncoder baselineEncoder = new IntSprintzEncoder();
    ByteArrayOutputStream baselineOutput = new ByteArrayOutputStream();
    for (int value = 0; value < 9; value++) {
      baselineEncoder.encode(value, baselineOutput);
    }
    baselineEncoder.flush(baselineOutput);

    assertTrue(byteCache.singleByteWrites > 0);
    assertTrue(byteCache.arrayWrites > 0);
    assertTrue(byteCache.writeToCalls > 0);
    assertEquals(1, byteCache.resetCalls);
    assertArrayEquals(baselineOutput.toByteArray(), output.toByteArray());
  }

  @Test
  public void testRleRetainedSizeIncludesPackerObjects()
      throws IOException, ReflectiveOperationException {
    assertRlePackerRetainedSize(new IntRleEncoder(), IntRleEncoder.class);
    assertRlePackerRetainedSize(new LongRleEncoder(), LongRleEncoder.class);
  }

  @Test
  public void testChimpRetainedSizeIncludesLookupArrays() {
    IntChimpEncoder intEncoder = new IntChimpEncoder();
    assertTrue(
        intEncoder.getRetainedSizeInBytes()
            >= RamUsageEstimator.sizeOfIntArray(4096) + RamUsageEstimator.sizeOfIntArray(64));

    LongChimpEncoder encoder = new LongChimpEncoder();

    assertTrue(
        encoder.getRetainedSizeInBytes()
            >= RamUsageEstimator.sizeOfIntArray(16_384) + RamUsageEstimator.sizeOfLongArray(128));
  }

  @Test
  public void testDictionaryRetainedSizeIncludesBinaryPayload() {
    DictionaryEncoder encoder = new DictionaryEncoder();
    Binary value = new Binary(new byte[64 * 1024]);

    encoder.encode(value, new ByteArrayOutputStream());

    assertTrue(encoder.getRetainedSizeInBytes() >= value.ramBytesUsed());
  }

  @Test
  public void testDictionaryRetainedSizeReleasesEntriesAfterFlush() {
    DictionaryEncoder encoder = new DictionaryEncoder();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] payload = new byte[64 * 1024];
    encoder.encode(new Binary(payload), output);
    long retainedSizeBeforeFlush = encoder.getRetainedSizeInBytes();

    encoder.flush(output);
    long retainedSizeAfterFlush = encoder.getRetainedSizeInBytes();

    DictionaryEncoder emptyEntryEncoder = new DictionaryEncoder();
    emptyEntryEncoder.encode(new Binary(new byte[0]), new ByteArrayOutputStream());
    emptyEntryEncoder.flush(new ByteArrayOutputStream());

    assertTrue(retainedSizeBeforeFlush > retainedSizeAfterFlush);
    assertEquals(emptyEntryEncoder.getRetainedSizeInBytes(), retainedSizeAfterFlush);
  }

  @Test
  public void testDictionaryRetainedSizeAddsOnlyActualEntryMemory() {
    DictionaryEncoder encoder = new DictionaryEncoder();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Binary first = new Binary(new byte[32]);
    byte[] secondPayload = new byte[32];
    secondPayload[0] = 1;
    Binary second = new Binary(secondPayload);

    encoder.encode(first, output);
    long firstEstimate = encoder.getRetainedSizeInBytes();
    encoder.encode(second, output);

    assertEquals(
        first.ramBytesUsed() + RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY,
        encoder.getRetainedSizeInBytes() - firstEstimate);
  }

  @Test
  public void testSprintzRetainedSizeSkipsCachedBoxedValues() {
    IntSprintzEncoder cachedIntEncoder = new IntSprintzEncoder();
    IntSprintzEncoder uncachedIntEncoder = new IntSprintzEncoder();
    cachedIntEncoder.encode(1, new ByteArrayOutputStream());
    uncachedIntEncoder.encode(1000, new ByteArrayOutputStream());
    assertEquals(
        RamUsageEstimator.shallowSizeOfInstance(Integer.class),
        uncachedIntEncoder.getRetainedSizeInBytes() - cachedIntEncoder.getRetainedSizeInBytes());

    LongSprintzEncoder cachedLongEncoder = new LongSprintzEncoder();
    LongSprintzEncoder uncachedLongEncoder = new LongSprintzEncoder();
    cachedLongEncoder.encode(1L, new ByteArrayOutputStream());
    uncachedLongEncoder.encode(1000L, new ByteArrayOutputStream());
    assertEquals(
        RamUsageEstimator.sizeOf(Long.valueOf(1000L)),
        uncachedLongEncoder.getRetainedSizeInBytes() - cachedLongEncoder.getRetainedSizeInBytes());
  }

  @Test
  public void testRetainedSizeIncludesRegularEncoderBlockBuffer() {
    RegularDataEncoder.IntRegularEncoder encoder = new RegularDataEncoder.IntRegularEncoder();

    encoder.encode(1, new ByteArrayOutputStream());

    assertTrue(encoder.getRetainedSizeInBytes() >= RamUsageEstimator.sizeOfIntArray(128));
  }

  @Test
  public void testRetainedSizeIncludesDeltaEncoderBlockBuffers() {
    DeltaBinaryEncoder.IntDeltaEncoder encoder = new DeltaBinaryEncoder.IntDeltaEncoder();

    encoder.encode(1, new ByteArrayOutputStream());

    assertTrue(
        encoder.getRetainedSizeInBytes()
            >= RamUsageEstimator.sizeOfIntArray(128)
                + RamUsageEstimator.sizeOfByteArray(128 * Integer.BYTES));
  }

  @Test
  public void testRetainedSizeIncludesSprintzStateAfterFlush() throws IOException {
    assertSprintzRetainedSize(new IntSprintzEncoder(), TSDataType.INT32);
    assertSprintzRetainedSize(new LongSprintzEncoder(), TSDataType.INT64);
    assertSprintzRetainedSize(new FloatSprintzEncoder(), TSDataType.FLOAT);
    assertSprintzRetainedSize(new DoubleSprintzEncoder(), TSDataType.DOUBLE);
  }

  @Test
  public void testSprintzRetainedSizeIncludesFirePredictorDelta()
      throws ReflectiveOperationException {
    assertFirePredictorDeltaRetainedSize(new IntSprintzEncoder(), Integer.valueOf(1000));
    assertFirePredictorDeltaRetainedSize(new LongSprintzEncoder(), Long.valueOf(1000));
    assertFirePredictorDeltaRetainedSize(new FloatSprintzEncoder(), Integer.valueOf(1000));
    assertFirePredictorDeltaRetainedSize(new DoubleSprintzEncoder(), Long.valueOf(1000));
  }

  @Test
  public void testFloatRetainedSizeIncludesUnderflowFlags()
      throws IOException, ReflectiveOperationException {
    FloatEncoder encoder = new FloatEncoder(TSEncoding.RLE, TSDataType.FLOAT, 2);
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    for (int value = 0; value < 1000; value++) {
      encoder.encode((float) value, output);
    }
    encoder.flush(output);

    java.lang.reflect.Field delegateField = FloatEncoder.class.getDeclaredField("encoder");
    delegateField.setAccessible(true);
    Encoder delegate = (Encoder) delegateField.get(encoder);
    java.lang.reflect.Field maximumSizeField =
        FloatEncoder.class.getDeclaredField("maximumUnderflowFlagsSize");
    maximumSizeField.setAccessible(true);

    assertEquals(1000, maximumSizeField.getInt(encoder));
    assertEquals(
        RamUsageEstimator.shallowSizeOf(encoder)
            + delegate.getRetainedSizeInBytes()
            + Encoder.estimateArrayListRetainedSize(1000, 0),
        encoder.getRetainedSizeInBytes());
  }

  private void assertRetainedSize(Encoder encoder, boolean useLongValues) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    long initialRetainedSize = encoder.getRetainedSizeInBytes();

    for (int value = 1000; value < 5000; value++) {
      if (useLongValues) {
        encoder.encode((long) value, output);
      } else {
        encoder.encode(value, output);
      }
    }

    long bufferedRetainedSize = encoder.getRetainedSizeInBytes();
    assertTrue(bufferedRetainedSize > encoder.getMaxByteSize());

    encoder.flush(output);

    assertTrue(encoder.getRetainedSizeInBytes() > initialRetainedSize);
  }

  private void assertRleRetainedSizeTracksReuse(Encoder encoder, boolean useLongValues)
      throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (int value = 1000; value < 5000; value++) {
      if (useLongValues) {
        encoder.encode((long) value, output);
      } else {
        encoder.encode(value, output);
      }
    }
    encoder.flush(output);
    long retainedSizeAfterFlush = encoder.getRetainedSizeInBytes();

    if (useLongValues) {
      encoder.encode(9000L, output);
      assertEquals(
          RamUsageEstimator.sizeOf(Long.valueOf(9000L)),
          encoder.getRetainedSizeInBytes() - retainedSizeAfterFlush);
    } else {
      encoder.encode(9000, output);
      assertEquals(
          RamUsageEstimator.shallowSizeOfInstance(Integer.class),
          encoder.getRetainedSizeInBytes() - retainedSizeAfterFlush);
    }
  }

  private void assertSprintzRetainedSizeTracksReuse(Encoder encoder, TSDataType dataType)
      throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (int value = 1000; value < 1009; value++) {
      switch (dataType) {
        case INT32:
          encoder.encode(value, output);
          break;
        case INT64:
          encoder.encode((long) value, output);
          break;
        case FLOAT:
          encoder.encode((float) value, output);
          break;
        case DOUBLE:
          encoder.encode((double) value, output);
          break;
        default:
          throw new IllegalArgumentException(dataType.name());
      }
    }
    encoder.flush(output);
    long retainedSizeAfterFlush = encoder.getRetainedSizeInBytes();

    long expectedValueSize;
    switch (dataType) {
      case INT32:
        encoder.encode(9000, output);
        expectedValueSize = RamUsageEstimator.shallowSizeOfInstance(Integer.class);
        break;
      case INT64:
        encoder.encode(9000L, output);
        expectedValueSize = RamUsageEstimator.sizeOf(Long.valueOf(9000L));
        break;
      case FLOAT:
        encoder.encode(9000.0F, output);
        expectedValueSize = RamUsageEstimator.shallowSizeOfInstance(Float.class);
        break;
      case DOUBLE:
        encoder.encode(9000.0D, output);
        expectedValueSize = RamUsageEstimator.shallowSizeOfInstance(Double.class);
        break;
      default:
        throw new IllegalArgumentException(dataType.name());
    }

    assertEquals(expectedValueSize, encoder.getRetainedSizeInBytes() - retainedSizeAfterFlush);
  }

  private void assertSprintzRetainedSize(Encoder encoder, TSDataType dataType) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    long initialRetainedSize = encoder.getRetainedSizeInBytes();

    for (int value = 1000; value < 1009; value++) {
      switch (dataType) {
        case INT32:
          encoder.encode(value, output);
          break;
        case INT64:
          encoder.encode((long) value, output);
          break;
        case FLOAT:
          encoder.encode((float) value, output);
          break;
        case DOUBLE:
          encoder.encode((double) value, output);
          break;
        default:
          throw new IllegalArgumentException(dataType.name());
      }
    }

    encoder.flush(output);

    assertTrue(encoder.getRetainedSizeInBytes() > initialRetainedSize);
  }

  private void assertFirePredictorDeltaRetainedSize(Encoder encoder, Object delta)
      throws ReflectiveOperationException {
    Field predictorField = encoder.getClass().getDeclaredField("firePred");
    predictorField.setAccessible(true);
    Object predictor = predictorField.get(encoder);
    Field deltaField = Fire.class.getDeclaredField("delta");
    deltaField.setAccessible(true);
    long retainedSizeWithCachedDelta = encoder.getRetainedSizeInBytes();

    deltaField.set(predictor, delta);

    assertEquals(
        RamUsageEstimator.shallowSizeOf(delta),
        encoder.getRetainedSizeInBytes() - retainedSizeWithCachedDelta);
  }

  private static class TrackingByteArrayOutputStream extends ByteArrayOutputStream {
    private int singleByteWrites;
    private int arrayWrites;
    private int writeToCalls;
    private int resetCalls;

    @Override
    public synchronized void write(int value) {
      singleByteWrites++;
      super.write(value);
    }

    @Override
    public synchronized void write(byte[] values, int offset, int length) {
      arrayWrites++;
      super.write(values, offset, length);
    }

    @Override
    public synchronized void reset() {
      resetCalls++;
      super.reset();
    }

    @Override
    public synchronized void writeTo(OutputStream output) throws IOException {
      writeToCalls++;
      super.writeTo(output);
    }
  }

  private void assertRlePackerRetainedSize(Encoder encoder, Class<?> encoderType)
      throws IOException, ReflectiveOperationException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    if (encoder instanceof LongRleEncoder) {
      encoder.encode(1L, output);
    } else {
      encoder.encode(1, output);
    }
    encoder.flush(output);

    Field packerField = encoderType.getDeclaredField("packer");
    packerField.setAccessible(true);
    Object packer = packerField.get(encoder);
    long retainedSizeWithPacker = encoder.getRetainedSizeInBytes();

    packerField.set(encoder, null);

    assertEquals(
        RamUsageEstimator.shallowSizeOf(packer),
        retainedSizeWithPacker - encoder.getRetainedSizeInBytes());
  }

  private void assertByteArrayOutputStreamCapacity(Encoder encoder, Class<?> cacheOwner)
      throws ReflectiveOperationException {
    Field byteCacheField = cacheOwner.getDeclaredField("byteCache");
    byteCacheField.setAccessible(true);
    ByteArrayOutputStream byteCache = (ByteArrayOutputStream) byteCacheField.get(encoder);
    long retainedSizeWithCache = encoder.getRetainedSizeInBytes();

    byteCacheField.set(encoder, null);

    assertEquals(
        RamUsageEstimator.shallowSizeOf(byteCache) + RamUsageEstimator.sizeOfByteArray(32),
        retainedSizeWithCache - encoder.getRetainedSizeInBytes());
  }
}
