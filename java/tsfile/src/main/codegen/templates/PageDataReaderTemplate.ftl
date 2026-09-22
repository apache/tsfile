<@pp.dropOutputFile />
<#list [
  ["Boolean", "boolean", "Boolean", "Boolean", "TsBoolean"],
  ["Int", "int", "Int", "Integer", "TsInt"],
  ["Date", "int", "Int", "Integer", "TsInt"],
  ["Long", "long", "Long", "Long", "TsLong"],
  ["Float", "float", "Float", "Float", "TsFloat"],
  ["Double", "double", "Double", "Double", "TsDouble"],
  ["Binary", "Binary", "Binary", "Binary", "TsBinary"]
] as t>
<#assign name=t[0] primitive=t[1] suffix=t[2] filterSuffix=t[3] wrapper=t[4]>
<@pp.changeOutputFile name="/org/apache/tsfile/read/common/type/service/${name}PageDataReader.java" />
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
package org.apache.tsfile.read.common.type.service;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.encoding.decoder.Decoder;
<#if name == "Date">
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
</#if>
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.series.PaginationController;
<#if name == "Binary">
import org.apache.tsfile.utils.Binary;
</#if>
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.TypeServices.PageDataBatchReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongPredicate;

/** Generated from PageDataReaderTemplate.ftl. Dispatch once per batch, not once per value. */
public final class ${name}PageDataReader implements PageDataBatchReader {
  public static final ${name}PageDataReader INSTANCE = new ${name}PageDataReader();

  private ${name}PageDataReader() {}

  @Override
  public void readBatch(Decoder timeDecoder, ByteBuffer timeBuffer, Decoder decoder,
      ByteBuffer buffer, Filter filter, BatchData output, boolean allSatisfy,
      LongPredicate isDeleted) throws IOException {
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      ${primitive} value = decoder.read${suffix}(buffer);
      if (!isDeleted.test(timestamp)
          && (allSatisfy || filter.satisfy${filterSuffix}(timestamp, value))) {
        output.put${suffix}(timestamp, value);
      }
    }
  }

  @Override
  public void readAlignedBatch(long[] timestamps, byte[] bitmap, Decoder decoder,
      ByteBuffer buffer, Filter filter, BatchData output, LongPredicate isDeleted) {
    boolean allSatisfy = filter == null;
    for (int i = 0; i < timestamps.length; i++) {
      if ((bitmap[i / 8] & (0x80 >>> (i % 8))) == 0) {
        continue;
      }
      long timestamp = timestamps[i];
      ${primitive} value = decoder.read${suffix}(buffer);
      if (!isDeleted.test(timestamp)
          && (allSatisfy || filter.satisfy${filterSuffix}(timestamp, value))) {
        output.put${suffix}(timestamp, value);
      }
    }
  }

  @Override
  public long readBlock(Decoder timeDecoder, ByteBuffer timeBuffer, Decoder decoder,
      ByteBuffer buffer, Filter filter, TsBlockBuilder builder, boolean allSatisfy,
      LongPredicate isDeleted, PaginationController pagination) throws IOException {
    ColumnBuilder times = builder.getTimeColumnBuilder();
    ColumnBuilder values = builder.getColumnBuilder(0);
    long filtered = 0;
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      // Consume both streams before deletion/filter/pagination checks, including the stop row.
      ${primitive} value = decoder.read${suffix}(buffer);
      if (isDeleted.test(timestamp)) {
        continue;
      }
      if (!allSatisfy && !filter.satisfy${filterSuffix}(timestamp, value)) {
        filtered++;
        continue;
      }
      if (pagination.hasCurOffset()) {
        pagination.consumeOffset();
        continue;
      }
      if (!pagination.hasCurLimit()) {
        break;
      }
      times.writeLong(timestamp);
      values.write${suffix}(value);
      builder.declarePosition();
      pagination.consumeLimit();
    }
    return filtered;
  }

  @Override
  public void readValues(long[] timestamps, byte[] bitmap, Decoder decoder, ByteBuffer buffer,
      TsPrimitiveType[] output, LongPredicate isDeleted) {
    for (int i = 0; i < output.length; i++) {
      if ((bitmap[i / 8] & (0x80 >>> (i % 8))) == 0) {
        continue;
      }
      ${primitive} value = decoder.read${suffix}(buffer);
      if (!isDeleted.test(timestamps[i])) {
        output[i] = new TsPrimitiveType.${wrapper}(value<#if name == "Date">, TSDataType.DATE</#if>);
      }
    }
  }

  @Override
  public void readColumn(int end, byte[] bitmap, Decoder decoder, ByteBuffer buffer,
      ColumnBuilder builder, boolean[] keep, boolean[] deleted) {
    <#if name == "Date">
    if (builder instanceof BinaryColumnBuilder binaryBuilder) {
      <@selectedColumn dateBinary=true />
      return;
    }
    </#if>
    <@selectedColumn dateBinary=false />
  }

  @Override
  public void readColumn(int start, int end, byte[] bitmap, Decoder decoder, ByteBuffer buffer,
      ColumnBuilder builder) {
    // The encoded stream contains only present values; skipped rows still consume their values.
    for (int i = 0; i < start; i++) {
      if ((bitmap[i / 8] & (0x80 >>> (i % 8))) != 0) {
        decoder.read${suffix}(buffer);
      }
    }
    <#if name == "Date">
    if (builder instanceof BinaryColumnBuilder binaryBuilder) {
      <@rangeColumn dateBinary=true />
      return;
    }
    </#if>
    <@rangeColumn dateBinary=false />
  }
}
</#list>

<#macro selectedColumn dateBinary>
    for (int i = 0; i < end; i++) {
      if ((bitmap[i / 8] & (0x80 >>> (i % 8))) == 0) {
        if (keep[i]) {
          builder.appendNull();
        }
        continue;
      }
      // Decode even rows discarded by selection or deletion to keep the stream aligned.
      ${primitive} value = decoder.read${suffix}(buffer);
      if (keep[i]) {
        if (deleted != null && deleted[i]) {
          builder.appendNull();
        } else {
          <#if dateBinary>
          binaryBuilder.writeDate(value);
          <#else>
          builder.write${suffix}(value);
          </#if>
        }
      }
    }
</#macro>

<#macro rangeColumn dateBinary>
    for (int i = start; i < end; i++) {
      if ((bitmap[i / 8] & (0x80 >>> (i % 8))) == 0) {
        builder.appendNull();
      } else {
        <#if dateBinary>
        binaryBuilder.writeDate(decoder.readInt(buffer));
        <#else>
        builder.write${suffix}(decoder.read${suffix}(buffer));
        </#if>
      }
    }
</#macro>
