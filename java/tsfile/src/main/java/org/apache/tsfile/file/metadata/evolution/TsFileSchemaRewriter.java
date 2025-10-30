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

import java.nio.charset.StandardCharsets;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Map;

/**
 * A utility class to rewrite the schema of an existing TsFile by appending new properties to its
 * TsFileMetadata.
 */
public class TsFileSchemaRewriter {
  private final String filePath;

  public TsFileSchemaRewriter(String tsfilePath) {
    this.filePath = tsfilePath;
  }

  /**
   * Append new properties to the TsFileMetadata of the TsFile.
   *
   * @param newProperties the new properties to append
   * @throws IOException if an I/O error occurs
   */
  public void appendProperties(Map<String, String> newProperties) throws IOException {
    // read TsFileMetadata and its position
    TsFileMetadata tsFileMetadata;
    long metadataOffset;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      tsFileMetadata = reader.readFileMetadata();
      metadataOffset = reader.getFileMetadataPos();
    }

    // calculate the position of properties and write new properties to a buffer
    int propertiesOffset = tsFileMetadata.getPropertiesOffset();
    Map<String, String> mergedProperties = tsFileMetadata.getTsFileProperties();
    mergedProperties.putAll(newProperties);
    PublicBAOS newPropertiesBuffer = new PublicBAOS(4096);
    DataOutputStream dataOutputStream = new DataOutputStream(newPropertiesBuffer);
    ReadWriteIOUtils.writeVar(mergedProperties, dataOutputStream);
    byte[] newPropertiesBuf = newPropertiesBuffer.getBuf();
    int newPropertiesSize = newPropertiesBuffer.size();
    // calculate the new metadata size
    int newMetadataSize = propertiesOffset + newPropertiesSize;

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw")) {
      // write the new properties and update the metadata size
      randomAccessFile.seek(metadataOffset + propertiesOffset);
      randomAccessFile.write(newPropertiesBuf, 0, newPropertiesSize);
      randomAccessFile.writeInt(newMetadataSize);
      randomAccessFile.write(TSFileConfig.MAGIC_STRING.getBytes(StandardCharsets.UTF_8));
    }
  }

  public static void main(String[] args) throws IOException, WriteProcessException {
    String tsfilePath = "test.tsfile";
    try (TsFileWriter writer = new TsFileWriter(new File(tsfilePath))) {
      writer.registerTimeseries("d1", new MeasurementSchema("s1", TSDataType.INT32));
      TSRecord record = new TSRecord("d1", 0);
      record.addPoint("s1", 100);
      writer.writeRecord(record);
    }

    TsFileSchemaRewriter rewriter = new TsFileSchemaRewriter(tsfilePath);
    rewriter.appendProperties(Collections.singletonMap("new_property_key", "new_property_value"));

    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsfilePath)) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      System.out.println("Updated TsFile Properties: " + tsFileMetadata.getTsFileProperties());
    }
  }
}
