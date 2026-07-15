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

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.tsfile.encoding.encoder.DoublePrecisionEncoderV1;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.FloatEncoder;
import org.apache.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.tsfile.encoding.encoder.LongRleEncoder;
import org.apache.tsfile.encoding.encoder.SinglePrecisionEncoderV1;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ValueChunkWriter;

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

  public static final TypeService<PageDataBlockValueReader> READ_PAGE_VALUE_TO_TSBLOCK_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (decoder,
                    buffer,
                    filter,
                    builder,
                    timestamp,
                    allSatisfy,
                    isDeleted,
                    paginationController) -> {
                  boolean value = decoder.readBoolean(buffer);
                  if (isDeleted.test(timestamp)) {
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!allSatisfy && !filter.satisfyBoolean(timestamp, value)) {
                    return PageDataReadStatus.FILTERED;
                  }
                  if (paginationController.hasCurOffset()) {
                    paginationController.consumeOffset();
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!paginationController.hasCurLimit()) {
                    return PageDataReadStatus.STOP;
                  }
                  builder.getTimeColumnBuilder().writeLong(timestamp);
                  builder.getColumnBuilder(0).writeBoolean(value);
                  builder.declarePosition();
                  paginationController.consumeLimit();
                  return PageDataReadStatus.CONTINUE;
                };
            case INT32, DATE ->
                (decoder,
                    buffer,
                    filter,
                    builder,
                    timestamp,
                    allSatisfy,
                    isDeleted,
                    paginationController) -> {
                  int value = decoder.readInt(buffer);
                  if (isDeleted.test(timestamp)) {
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!allSatisfy && !filter.satisfyInteger(timestamp, value)) {
                    return PageDataReadStatus.FILTERED;
                  }
                  if (paginationController.hasCurOffset()) {
                    paginationController.consumeOffset();
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!paginationController.hasCurLimit()) {
                    return PageDataReadStatus.STOP;
                  }
                  builder.getTimeColumnBuilder().writeLong(timestamp);
                  builder.getColumnBuilder(0).writeInt(value);
                  builder.declarePosition();
                  paginationController.consumeLimit();
                  return PageDataReadStatus.CONTINUE;
                };
            case INT64, TIMESTAMP ->
                (decoder,
                    buffer,
                    filter,
                    builder,
                    timestamp,
                    allSatisfy,
                    isDeleted,
                    paginationController) -> {
                  long value = decoder.readLong(buffer);
                  if (isDeleted.test(timestamp)) {
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!allSatisfy && !filter.satisfyLong(timestamp, value)) {
                    return PageDataReadStatus.FILTERED;
                  }
                  if (paginationController.hasCurOffset()) {
                    paginationController.consumeOffset();
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!paginationController.hasCurLimit()) {
                    return PageDataReadStatus.STOP;
                  }
                  builder.getTimeColumnBuilder().writeLong(timestamp);
                  builder.getColumnBuilder(0).writeLong(value);
                  builder.declarePosition();
                  paginationController.consumeLimit();
                  return PageDataReadStatus.CONTINUE;
                };
            case FLOAT ->
                (decoder,
                    buffer,
                    filter,
                    builder,
                    timestamp,
                    allSatisfy,
                    isDeleted,
                    paginationController) -> {
                  float value = decoder.readFloat(buffer);
                  if (isDeleted.test(timestamp)) {
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!allSatisfy && !filter.satisfyFloat(timestamp, value)) {
                    return PageDataReadStatus.FILTERED;
                  }
                  if (paginationController.hasCurOffset()) {
                    paginationController.consumeOffset();
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!paginationController.hasCurLimit()) {
                    return PageDataReadStatus.STOP;
                  }
                  builder.getTimeColumnBuilder().writeLong(timestamp);
                  builder.getColumnBuilder(0).writeFloat(value);
                  builder.declarePosition();
                  paginationController.consumeLimit();
                  return PageDataReadStatus.CONTINUE;
                };
            case DOUBLE ->
                (decoder,
                    buffer,
                    filter,
                    builder,
                    timestamp,
                    allSatisfy,
                    isDeleted,
                    paginationController) -> {
                  double value = decoder.readDouble(buffer);
                  if (isDeleted.test(timestamp)) {
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!allSatisfy && !filter.satisfyDouble(timestamp, value)) {
                    return PageDataReadStatus.FILTERED;
                  }
                  if (paginationController.hasCurOffset()) {
                    paginationController.consumeOffset();
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!paginationController.hasCurLimit()) {
                    return PageDataReadStatus.STOP;
                  }
                  builder.getTimeColumnBuilder().writeLong(timestamp);
                  builder.getColumnBuilder(0).writeDouble(value);
                  builder.declarePosition();
                  paginationController.consumeLimit();
                  return PageDataReadStatus.CONTINUE;
                };
            case TEXT, BLOB, STRING, OBJECT ->
                (decoder,
                    buffer,
                    filter,
                    builder,
                    timestamp,
                    allSatisfy,
                    isDeleted,
                    paginationController) -> {
                  Binary value = decoder.readBinary(buffer);
                  if (isDeleted.test(timestamp)) {
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!allSatisfy && !filter.satisfyBinary(timestamp, value)) {
                    return PageDataReadStatus.FILTERED;
                  }
                  if (paginationController.hasCurOffset()) {
                    paginationController.consumeOffset();
                    return PageDataReadStatus.CONTINUE;
                  }
                  if (!paginationController.hasCurLimit()) {
                    return PageDataReadStatus.STOP;
                  }
                  builder.getTimeColumnBuilder().writeLong(timestamp);
                  builder.getColumnBuilder(0).writeBinary(value);
                  builder.declarePosition();
                  paginationController.consumeLimit();
                  return PageDataReadStatus.CONTINUE;
                };
            case ROW, UNKNOWN, VECTOR ->
                (decoder,
                    buffer,
                    filter,
                    builder,
                    timestamp,
                    allSatisfy,
                    isDeleted,
                    paginationController) -> {
                  throw new UnSupportedDataTypeException(String.valueOf(type.getTypeEnum()));
                };
          };

  public static final TypeService<PageDataTsPrimitiveValueReader>
      READ_PAGE_VALUE_TO_TSPRIMITIVETYPE_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      boolean value = decoder.readBoolean(buffer);
                      return isDeleted.test(timestamp)
                          ? null
                          : new TsPrimitiveType.TsBoolean(value);
                    };
                case INT32 ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      int value = decoder.readInt(buffer);
                      return isDeleted.test(timestamp) ? null : new TsPrimitiveType.TsInt(value);
                    };
                case DATE ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      int value = decoder.readInt(buffer);
                      return isDeleted.test(timestamp)
                          ? null
                          : new TsPrimitiveType.TsInt(value, TSDataType.DATE);
                    };
                case INT64, TIMESTAMP ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      long value = decoder.readLong(buffer);
                      return isDeleted.test(timestamp) ? null : new TsPrimitiveType.TsLong(value);
                    };
                case FLOAT ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      float value = decoder.readFloat(buffer);
                      return isDeleted.test(timestamp) ? null : new TsPrimitiveType.TsFloat(value);
                    };
                case DOUBLE ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      double value = decoder.readDouble(buffer);
                      return isDeleted.test(timestamp) ? null : new TsPrimitiveType.TsDouble(value);
                    };
                case TEXT, BLOB, STRING, OBJECT ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      Binary value = decoder.readBinary(buffer);
                      return isDeleted.test(timestamp) ? null : new TsPrimitiveType.TsBinary(value);
                    };
                case ROW, UNKNOWN, VECTOR ->
                    (decoder, buffer, timestamp, isDeleted) -> {
                      throw new UnSupportedDataTypeException(String.valueOf(type.getTypeEnum()));
                    };
              };

  public static final TypeService<PageDataColumnBuilderValueReader>
      READ_PAGE_VALUE_TO_COLUMNBUILDER_SERVICE =
          type ->
              switch (type.getTypeEnum()) {
                case BOOLEAN ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      boolean value = decoder.readBoolean(buffer);
                      if (keepCurrentRow) {
                        if (isDeleted) {
                          builder.appendNull();
                        } else {
                          builder.writeBoolean(value);
                        }
                      }
                    };
                case INT32 ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      int value = decoder.readInt(buffer);
                      if (keepCurrentRow) {
                        if (isDeleted) {
                          builder.appendNull();
                        } else {
                          builder.writeInt(value);
                        }
                      }
                    };
                case DATE ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      int value = decoder.readInt(buffer);
                      if (keepCurrentRow) {
                        if (isDeleted) {
                          builder.appendNull();
                        } else if (builder instanceof BinaryColumnBuilder) {
                          ((BinaryColumnBuilder) builder).writeDate(value);
                        } else {
                          builder.writeInt(value);
                        }
                      }
                    };
                case INT64, TIMESTAMP ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      long value = decoder.readLong(buffer);
                      if (keepCurrentRow) {
                        if (isDeleted) {
                          builder.appendNull();
                        } else {
                          builder.writeLong(value);
                        }
                      }
                    };
                case FLOAT ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      float value = decoder.readFloat(buffer);
                      if (keepCurrentRow) {
                        if (isDeleted) {
                          builder.appendNull();
                        } else {
                          builder.writeFloat(value);
                        }
                      }
                    };
                case DOUBLE ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      double value = decoder.readDouble(buffer);
                      if (keepCurrentRow) {
                        if (isDeleted) {
                          builder.appendNull();
                        } else {
                          builder.writeDouble(value);
                        }
                      }
                    };
                case TEXT, BLOB, STRING, OBJECT ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      Binary value = decoder.readBinary(buffer);
                      if (keepCurrentRow) {
                        if (isDeleted) {
                          builder.appendNull();
                        } else {
                          builder.writeBinary(value);
                        }
                      }
                    };
                case ROW, UNKNOWN, VECTOR ->
                    (decoder, buffer, builder, keepCurrentRow, isDeleted) -> {
                      throw new UnSupportedDataTypeException(String.valueOf(type.getTypeEnum()));
                    };
              };

  public static final TypeService<EmptyValueChunkWriter> WRITE_EMPTY_VALUE_TO_CHUNK_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> writer -> writer.write(-1, false, true);
            case INT32, DATE -> writer -> writer.write(-1, 0, true);
            case INT64, TIMESTAMP -> writer -> writer.write(-1, 0L, true);
            case FLOAT -> writer -> writer.write(-1, 0.0F, true);
            case DOUBLE -> writer -> writer.write(-1, 0.0D, true);
            case TEXT, BLOB, STRING, OBJECT -> writer -> writer.write(-1, null, true);
            case ROW, UNKNOWN, VECTOR ->
                writer -> {
                  throw new UnSupportedDataTypeException(
                      Messages.format("error.write.type_not_supported", type.getTypeEnum()));
                };
          };

  public static final TypeService<EncoderBuilder> RLE_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE, BOOLEAN -> maxPointNumber -> new IntRleEncoder();
            case INT64, TIMESTAMP -> maxPointNumber -> new LongRleEncoder();
            case FLOAT ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.RLE, TSDataType.FLOAT, maxPointNumber);
            case DOUBLE ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.RLE, TSDataType.DOUBLE, maxPointNumber);
            case TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                maxPointNumber -> {
                  throw new UnSupportedDataTypeException(
                      Messages.format(
                          "error.encoding.ts_encoding_builder_unsupported_type",
                          TSEncoding.RLE,
                          type.getTypeEnum()));
                };
          };

  public static final TypeService<EncoderBuilder> TS_2DIFF_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> maxPointNumber -> new DeltaBinaryEncoder.IntDeltaEncoder();
            case INT64, TIMESTAMP -> maxPointNumber -> new DeltaBinaryEncoder.LongDeltaEncoder();
            case FLOAT ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.TS_2DIFF, TSDataType.FLOAT, maxPointNumber);
            case DOUBLE ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.TS_2DIFF, TSDataType.DOUBLE, maxPointNumber);
            case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                maxPointNumber -> {
                  throw new UnSupportedDataTypeException(
                      Messages.format(
                          "error.encoding.ts_encoding_builder_unsupported_type",
                          TSEncoding.TS_2DIFF,
                          type.getTypeEnum()));
                };
          };

  public static final TypeService<Encoder> GORILLA_V1_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case FLOAT -> new SinglePrecisionEncoderV1();
            case DOUBLE -> new DoublePrecisionEncoderV1();
            case BOOLEAN,
                INT32,
                DATE,
                INT64,
                TIMESTAMP,
                TEXT,
                BLOB,
                STRING,
                OBJECT,
                ROW,
                UNKNOWN,
                VECTOR ->
                throw new UnSupportedDataTypeException(
                    Messages.format(
                        "error.encoding.ts_encoding_builder_unsupported_type",
                        TSEncoding.GORILLA_V1,
                        type.getTypeEnum()));
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

  @FunctionalInterface
  public interface PageDataBlockValueReader {

    PageDataReadStatus read(
        Decoder decoder,
        ByteBuffer buffer,
        Filter filter,
        TsBlockBuilder builder,
        long timestamp,
        boolean allSatisfy,
        LongPredicate isDeleted,
        PaginationController paginationController);
  }

  @FunctionalInterface
  public interface PageDataTsPrimitiveValueReader {

    TsPrimitiveType read(
        Decoder decoder, ByteBuffer buffer, long timestamp, LongPredicate isDeleted);
  }

  @FunctionalInterface
  public interface PageDataColumnBuilderValueReader {

    void read(
        Decoder decoder,
        ByteBuffer buffer,
        ColumnBuilder builder,
        boolean keepCurrentRow,
        boolean isDeleted);
  }

  @FunctionalInterface
  public interface EmptyValueChunkWriter {

    void write(ValueChunkWriter writer);
  }

  @FunctionalInterface
  public interface EncoderBuilder {

    Encoder build(int maxPointNumber);
  }

  public enum PageDataReadStatus {
    CONTINUE,
    FILTERED,
    STOP
  }
}
