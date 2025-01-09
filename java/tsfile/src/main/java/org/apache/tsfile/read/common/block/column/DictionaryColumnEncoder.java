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

package org.apache.tsfile.read.common.block.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DictionaryColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {
    // dictionary
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(ColumnEncoding.deserializeFrom(input));
    Column dictionary = columnEncoder.readColumn(input, dataType, ReadWriteIOUtils.readInt(input));

    // ids
    int[] ids = ReadWriteIOUtils.readInts(input);

    // flatten the dictionary
    return dictionary.copyPositions(ids, 0, positionCount);
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {
    DictionaryColumn dictionaryColumn = (DictionaryColumn) column;
    // compact before serialize
    dictionaryColumn = dictionaryColumn.compact();

    Column dictionary = dictionaryColumn.getDictionary();

    // dictionary
    dictionary.getEncoding().serializeTo(output);
    int positionCount = dictionary.getPositionCount();
    ReadWriteIOUtils.write(positionCount, output);
    ColumnEncoder columnEncoder = ColumnEncoderFactory.get(dictionary.getEncoding());
    columnEncoder.writeColumn(output, dictionary);

    // ids
    ReadWriteIOUtils.writeInts(
        dictionaryColumn.getRawIds(),
        dictionaryColumn.getRawIdsOffset(),
        dictionaryColumn.getPositionCount(),
        output);
  }
}
