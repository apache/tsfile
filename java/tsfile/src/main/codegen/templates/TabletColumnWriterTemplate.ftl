<@pp.dropOutputFile />
<#list [["Boolean", "boolean"], ["Int", "int"], ["Long", "long"],
        ["Float", "float"], ["Double", "double"], ["Binary", "Binary"],
        ["Date", "LocalDate"]] as t>
<#assign name=t[0] primitive=t[1]>
<@pp.changeOutputFile name="/org/apache/tsfile/read/common/type/service/${name}TabletColumnWriter.java" />
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

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.BitMap;
<#if name == "Binary">
import org.apache.tsfile.utils.Binary;
</#if>
<#if name == "Date">
import org.apache.tsfile.utils.DateUtils;
import java.time.LocalDate;
</#if>
import org.apache.tsfile.utils.TypeServices.TabletColumnWriter;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.TabletWriteContext;

/** Generated typed loop; retain scalar encoding to preserve page boundaries and SDT state. */
public final class ${name}TabletColumnWriter implements TabletColumnWriter {
  public static final ${name}TabletColumnWriter INSTANCE = new ${name}TabletColumnWriter();

  private ${name}TabletColumnWriter() {}

  @Override
  public void write(ChunkWriterImpl writer, long[] times, Object values, BitMap nulls,
      int start, int end, TabletWriteContext context) throws WriteProcessException {
<#if name == "Date">
    if (values instanceof int[]) {
      IntTabletColumnWriter.INSTANCE.write(writer, times, values, nulls, start, end, context);
      return;
    }
</#if>
    ${primitive}[] column = (${primitive}[]) values;
    for (int row = start; row < end; row++) {
      if (nulls != null && nulls.isMarked(row)) {
        continue;
      }
      long time = times[row];
      context.checkTime(time);
<#if name == "Date">
      writer.write(time, DateUtils.parseDateExpressionToInt(column[row]));
<#else>
      writer.write(time, column[row]);
</#if>
      context.recordTime(time);
    }
  }
}
</#list>
