/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.compatibility;

import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry.Deserializer;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

public class CompatibilityUtils {

  private CompatibilityUtils() {
    // util class
  }

  public static TsFileMetadata deserializeTsFileMetadataFromV3(ByteBuffer buffer) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    // metadataIndex
    MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer, true);
    fileMetaData.setTableMetadataIndexNodeMap(Collections.singletonMap("", metadataIndexNode));

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
      int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      fileMetaData.setBloomFilter(
          BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize));
    }

    return fileMetaData;
  }

  public static DeviceMetadataIndexEntry.Deserializer v3DeviceMetadataIndexEntryDeserializer =
      new Deserializer() {
        @Override
        public DeviceMetadataIndexEntry deserializeFrom(ByteBuffer buffer) {
          return deserializeFromV3(buffer);
        }

        @Override
        public DeviceMetadataIndexEntry deserializeFrom(InputStream inputStream)
            throws IOException {
          return deserializeFromV3(inputStream);
        }
      };

  public static DeviceMetadataIndexEntry deserializeFromV3(ByteBuffer buffer) {
    IDeviceID device = PlainDeviceID.DESERIALIZER.deserializeFrom(buffer);
    long offset = ReadWriteIOUtils.readLong(buffer);
    return new DeviceMetadataIndexEntry(device, offset);
  }

  public static DeviceMetadataIndexEntry deserializeFromV3(InputStream inputStream)
      throws IOException {
    IDeviceID device = PlainDeviceID.DESERIALIZER.deserializeFrom(inputStream);
    long offset = ReadWriteIOUtils.readLong(inputStream);
    return new DeviceMetadataIndexEntry(device, offset);
  }
}
