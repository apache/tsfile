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

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;
import java.util.function.LongPredicate;

public final class TypeServices {

  public static final TypeService<PageDataValueReader> READ_PAGE_VALUE_TO_BATCHDATA_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (decoder, buffer, filter, pageData, timestamp, allSatisfy, isDeleted) -> {
                  boolean value = decoder.readBoolean(buffer);
                  if (!isDeleted.test(timestamp)
                      && (allSatisfy || filter.satisfyBoolean(timestamp, value))) {
                    pageData.putBoolean(timestamp, value);
                  }
                };
            case INT32, DATE ->
                (decoder, buffer, filter, pageData, timestamp, allSatisfy, isDeleted) -> {
                  int value = decoder.readInt(buffer);
                  if (!isDeleted.test(timestamp)
                      && (allSatisfy || filter.satisfyInteger(timestamp, value))) {
                    pageData.putInt(timestamp, value);
                  }
                };
            case INT64, TIMESTAMP ->
                (decoder, buffer, filter, pageData, timestamp, allSatisfy, isDeleted) -> {
                  long value = decoder.readLong(buffer);
                  if (!isDeleted.test(timestamp)
                      && (allSatisfy || filter.satisfyLong(timestamp, value))) {
                    pageData.putLong(timestamp, value);
                  }
                };
            case FLOAT ->
                (decoder, buffer, filter, pageData, timestamp, allSatisfy, isDeleted) -> {
                  float value = decoder.readFloat(buffer);
                  if (!isDeleted.test(timestamp)
                      && (allSatisfy || filter.satisfyFloat(timestamp, value))) {
                    pageData.putFloat(timestamp, value);
                  }
                };
            case DOUBLE ->
                (decoder, buffer, filter, pageData, timestamp, allSatisfy, isDeleted) -> {
                  double value = decoder.readDouble(buffer);
                  if (!isDeleted.test(timestamp)
                      && (allSatisfy || filter.satisfyDouble(timestamp, value))) {
                    pageData.putDouble(timestamp, value);
                  }
                };
            case TEXT, BLOB, STRING, OBJECT ->
                (decoder, buffer, filter, pageData, timestamp, allSatisfy, isDeleted) -> {
                  Binary value = decoder.readBinary(buffer);
                  if (!isDeleted.test(timestamp)
                      && (allSatisfy || filter.satisfyBinary(timestamp, value))) {
                    pageData.putBinary(timestamp, value);
                  }
                };
            case ROW, UNKNOWN, VECTOR ->
                (decoder, buffer, filter, pageData, timestamp, allSatisfy, isDeleted) -> {
                  throw new UnSupportedDataTypeException(String.valueOf(type.getTypeEnum()));
                };
          };

  private TypeServices() {}

  @FunctionalInterface
  public interface PageDataValueReader {

    void read(
        Decoder decoder,
        ByteBuffer buffer,
        Filter filter,
        BatchData pageData,
        long timestamp,
        boolean allSatisfy,
        LongPredicate isDeleted);
  }
}
