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
package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * This class is the parent class of all Encoders. Every encoder has a specific {@code
 * <encoderType>} which represents the type of this encoder
 */
public abstract class Encoder {

  public static final String MAX_STRING_LENGTH = "max_string_length";
  public static final String MAX_POINT_NUMBER = "max_point_number";

  private TSEncoding type;

  public void setType(TSEncoding type) {
    this.type = type;
  }

  public TSEncoding getType() {
    return type;
  }

  public Encoder(TSEncoding type) {
    this.type = type;
  }

  public void encode(boolean value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "boolean"));
  }

  public void encode(short value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "short"));
  }

  public void encode(int value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "int"));
  }

  public void encode(long value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "long"));
  }

  public void encode(float value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "float"));
  }

  public void encode(double value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "double"));
  }

  public void encode(Binary value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "Binary"));
  }

  public void encode(BigDecimal value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        Messages.format("error.encoding.encoder_method_not_supported", "BigDecimal"));
  }

  /**
   * Write all values buffered in memory cache to OutputStream.
   *
   * @param out - ByteArrayOutputStream
   * @throws IOException cannot flush to OutputStream
   */
  public abstract void flush(ByteArrayOutputStream out) throws IOException;

  /**
   * When encoder accepts a new incoming data point, the maximal possible size in byte it takes to
   * store in memory.
   *
   * @return the maximal possible size of one data item encoded by this encoder
   */
  public int getOneItemMaxSize() {
    throw new UnsupportedOperationException();
  }

  /**
   * The maximal possible memory size occupied by current Encoder. This statistic value doesn't
   * involve OutputStream.
   *
   * @return the maximal size of possible memory occupied by current encoder
   */
  public long getMaxByteSize() {
    throw new UnsupportedOperationException();
  }

  public long getRetainedSizeInBytes() {
    long retainedSize = RamUsageEstimator.shallowSizeOf(this);
    try {
      return retainedSize + getMaxByteSize();
    } catch (UnsupportedOperationException e) {
      return retainedSize;
    }
  }

  protected static long estimateArrayListRetainedSize(int maximumSize, long retainedValuesSize) {
    long estimatedCapacity = 0;
    if (maximumSize > 0) {
      estimatedCapacity = 10;
      while (estimatedCapacity < maximumSize) {
        estimatedCapacity =
            Math.min(Integer.MAX_VALUE, estimatedCapacity + Math.max(1, estimatedCapacity >> 1));
      }
    }
    return RamUsageEstimator.ARRAY_LIST_INSTANCE_SIZE
        + (estimatedCapacity == 0
            ? 0
            : RamUsageEstimator.sizeOfObjectArray((int) estimatedCapacity))
        + retainedValuesSize;
  }

  protected static long estimateByteArrayOutputStreamRetainedSize(
      ByteArrayOutputStream output, int maximumObservedSize) {
    if (output == null) {
      return 0;
    }
    long retainedSize = RamUsageEstimator.shallowSizeOf(output);
    if (output instanceof PublicBAOS publicOutput) {
      return retainedSize + RamUsageEstimator.sizeOf(publicOutput.getBuf());
    }

    int minimumCapacity = Math.max(output.size(), maximumObservedSize);
    int estimatedCapacity = 32;
    while (estimatedCapacity < minimumCapacity) {
      int nextCapacity = estimatedCapacity << 1;
      if (nextCapacity <= 0) {
        estimatedCapacity = minimumCapacity;
        break;
      }
      estimatedCapacity = Math.max(nextCapacity, minimumCapacity);
    }
    return retainedSize + RamUsageEstimator.sizeOfByteArray(estimatedCapacity);
  }
}
