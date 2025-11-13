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
package org.apache.tsfile.file.metadata.evolution;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.junit.Test;

public class TsFileBackupProcessorTest {

  @Test
  public void testBackupAndRecover() throws IOException, WriteProcessException {
    File file = new File("target" + File.separator + "test.tsfile");
    TableSchema tableSchema = new TableSchema(
        "test_table",
        Arrays.asList(
            new ColumnSchema("s1", TSDataType.INT64, ColumnCategory.FIELD)
        )
    );
    try (ITsFileWriter tsFileWriter = new TsFileWriterBuilder().file(file).tableSchema(tableSchema).build()) {
      Tablet tablet = new Tablet(tableSchema);
      tablet.addTimestamp(0, 1L);
      tablet.addValue("s1", 0, 100L);
      tsFileWriter.write(tablet);
    }

    long backupPosition = file.length() / 2;
    TsFileBackupProcessor.writeBackup(file, backupPosition);
    try (FileChannel fileChannel = new FileOutputStream(file, true).getChannel()) {
      fileChannel.truncate(backupPosition);
    }

    TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath());
    assertEquals(tableSchema, reader.getTableSchema("test_table").get());
  }
}