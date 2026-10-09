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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/** TSFileMetaData collects all metadata info and saves in its data structure. */
public class TsFileMetadata {

  // bloom filter
  private BloomFilter bloomFilter;

  // List of <name, offset, childMetadataIndexType>
  private Map<String, MetadataIndexNode> tableMetadataIndexNodeMap;
  private Map<String, TableSchema> tableSchemaMap;
  private boolean hasTableSchemaMapCache;
  private Map<String, byte[]> tsFileProperties;

  // offset of MetaMarker.SEPARATOR
  private long metaOffset;
  // offset from MetaMarker.SEPARATOR (exclusive) to tsFileProperties
  private int propertiesOffset;

  private int encryptLevel;

  private byte[] secondKey;

  private String encryptType;

  public static TsFileMetadata deserializeAndCacheTableSchemaMap(
      ByteBuffer buffer, DeserializeConfig context) {
    return deserializeFrom(buffer, context, true);
  }

  public static TsFileMetadata deserializeWithoutCacheTableSchemaMap(
      ByteBuffer buffer, DeserializeConfig context) {
    return deserializeFrom(buffer, context, false);
  }

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -an instance of TsFileMetaData
   */
  public static TsFileMetadata deserializeFrom(
      ByteBuffer buffer, DeserializeConfig context, boolean needTableSchemaMap) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    int startPos = buffer.position();
    // metadataIndex
    int tableIndexNodeNum = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    Map<String, MetadataIndexNode> tableIndexNodeMap = new TreeMap<>();
    for (int i = 0; i < tableIndexNodeNum; i++) {
      String tableName = ReadWriteIOUtils.readVarIntString(buffer);
      MetadataIndexNode metadataIndexNode =
          context.deviceMetadataIndexNodeBufferDeserializer.deserialize(buffer, context);
      tableIndexNodeMap.put(tableName, metadataIndexNode);
    }
    fileMetaData.setTableMetadataIndexNodeMap(tableIndexNodeMap);

    // tableSchemas
    int tableSchemaNum = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    Map<String, TableSchema> tableSchemaMap = new HashMap<>();
    for (int i = 0; i < tableSchemaNum; i++) {
      String tableName = ReadWriteIOUtils.readVarIntString(buffer);
      TableSchema tableSchema = context.tableSchemaBufferDeserializer.deserialize(buffer, context);
      if (needTableSchemaMap) {
        tableSchema.setTableName(tableName);
        tableSchemaMap.put(tableName, tableSchema);
      }
    }
    fileMetaData.setTableSchemaMap(tableSchemaMap);
    fileMetaData.hasTableSchemaMapCache = needTableSchemaMap;

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      fileMetaData.bloomFilter = BloomFilter.deserialize(buffer);
    }

    fileMetaData.propertiesOffset = buffer.position() - startPos;

    if (buffer.hasRemaining()) {
      int propertiesSize = ReadWriteForEncodingUtils.readVarInt(buffer);
      Map<String, byte[]> propertiesMap = new HashMap<>();
      for (int i = 0; i < propertiesSize; i++) {
        String key = ReadWriteIOUtils.readVarIntString(buffer);
        int valueSize = ReadWriteForEncodingUtils.readVarInt(buffer);
        if ("encryptKey".equals(key) && valueSize >= 0) {
          EncryptUtils.validateSecondKeyStringLength(valueSize);
        }
        byte[] value = null;
        if (valueSize >= 0) {
          value = new byte[valueSize];
          buffer.get(value);
        }
        propertiesMap.put(key, value);
      }
      String encryptLevel = getPropertyAsString(propertiesMap, "encryptLevel");
      // if the file is not encrypted, set the default value(for compatible reason)
      if (!propertiesMap.containsKey("encryptLevel") || encryptLevel == null) {
        propertiesMap.put("encryptLevel", stringToBytes("0"));
        propertiesMap.put("encryptType", stringToBytes("org.apache.tsfile.encrypt.UNENCRYPTED"));
        propertiesMap.put("encryptKey", stringToBytes(""));
      } else if (encryptLevel.equals("0")) {
        propertiesMap.put("encryptType", stringToBytes("org.apache.tsfile.encrypt.UNENCRYPTED"));
        propertiesMap.put("encryptKey", stringToBytes(""));
      } else if (encryptLevel.equals("1")) {
        if (!propertiesMap.containsKey("encryptType")) {
          throw new EncryptException(
              Messages.format("error.file.tsfile_metadata_no_encrypt_type", 1));
        }
        if (!propertiesMap.containsKey("encryptKey")) {
          throw new EncryptException(
              Messages.format("error.file.tsfile_metadata_no_encrypt_key", 1));
        }
        String encryptKey = getPropertyAsString(propertiesMap, "encryptKey");
        if (encryptKey == null || encryptKey.isEmpty()) {
          throw new EncryptException(
              Messages.format("error.file.tsfile_metadata_null_encrypt_key", 1));
        }
        fileMetaData.encryptLevel = 1;
        fileMetaData.secondKey = EncryptUtils.getSecondKeyFromStr(encryptKey);
        fileMetaData.encryptType = getPropertyAsString(propertiesMap, "encryptType");
      } else if (encryptLevel.equals("2")) {
        if (!propertiesMap.containsKey("encryptType")) {
          throw new EncryptException(
              Messages.format("error.file.tsfile_metadata_no_encrypt_type", 2));
        }
        if (!propertiesMap.containsKey("encryptKey")) {
          throw new EncryptException(
              Messages.format("error.file.tsfile_metadata_no_encrypt_key", 2));
        }
        String encryptKey = getPropertyAsString(propertiesMap, "encryptKey");
        if (encryptKey == null || encryptKey.isEmpty()) {
          throw new EncryptException(
              Messages.format("error.file.tsfile_metadata_null_encrypt_key", 2));
        }
        fileMetaData.encryptLevel = 2;
        fileMetaData.secondKey = EncryptUtils.getSecondKeyFromStr(encryptKey);
        fileMetaData.encryptType = getPropertyAsString(propertiesMap, "encryptType");
      } else {
        throw new EncryptException(
            Messages.format("error.file.tsfile_metadata_unsupported_encrypt_level", encryptLevel));
      }
      fileMetaData.tsFileProperties = propertiesMap;
    }

    return fileMetaData;
  }

  public void addProperty(String key, byte[] value) {
    if (tsFileProperties == null) {
      tsFileProperties = new HashMap<>();
    }
    tsFileProperties.put(key, value);
  }

  public String getEncryptType() {
    return encryptType;
  }

  public byte[] getSecondKey() {
    return secondKey;
  }

  public int getEncryptLevel() {
    return encryptLevel;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public void setBloomFilter(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  /**
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   * @throws IOException error when operating outputStream
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    if (tableMetadataIndexNodeMap != null) {
      byteLen +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(
              tableMetadataIndexNodeMap.size(), outputStream);
      for (Entry<String, MetadataIndexNode> entry : tableMetadataIndexNodeMap.entrySet()) {
        byteLen += ReadWriteIOUtils.writeVar(entry.getKey(), outputStream);
        byteLen += entry.getValue().serializeTo(outputStream);
      }
    } else {
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(0, outputStream);
    }

    if (tableSchemaMap != null) {
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(tableSchemaMap.size(), outputStream);
      for (Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
        byteLen += ReadWriteIOUtils.writeVar(entry.getKey(), outputStream);
        byteLen += entry.getValue().serialize(outputStream);
      }
    } else {
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(0, outputStream);
    }

    // metaOffset
    byteLen += ReadWriteIOUtils.write(metaOffset, outputStream);
    if (bloomFilter != null) {
      byteLen += bloomFilter.serialize(outputStream);
    } else {
      byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(0, outputStream);
    }

    byteLen +=
        ReadWriteForEncodingUtils.writeVarInt(
            tsFileProperties != null ? tsFileProperties.size() : 0, outputStream);
    if (tsFileProperties != null) {
      for (Entry<String, byte[]> entry : tsFileProperties.entrySet()) {
        byteLen += ReadWriteIOUtils.writeVar(entry.getKey(), outputStream);
        byte[] value = entry.getValue();
        byteLen +=
            ReadWriteForEncodingUtils.writeVarInt(value == null ? -1 : value.length, outputStream);
        if (value != null) {
          outputStream.write(value);
          byteLen += value.length;
        }
      }
    }

    return byteLen;
  }

  public long getMetaOffset() {
    return metaOffset;
  }

  public void setMetaOffset(long metaOffset) {
    this.metaOffset = metaOffset;
  }

  public void setTableMetadataIndexNodeMap(
      Map<String, MetadataIndexNode> tableMetadataIndexNodeMap) {
    this.tableMetadataIndexNodeMap = tableMetadataIndexNodeMap;
  }

  public void setTableSchemaMap(Map<String, TableSchema> tableSchemaMap) {
    this.tableSchemaMap = tableSchemaMap;
    this.hasTableSchemaMapCache = true;
  }

  public Map<String, MetadataIndexNode> getTableMetadataIndexNodeMap() {
    return tableMetadataIndexNodeMap;
  }

  public MetadataIndexNode getTableMetadataIndexNode(String tableName) {
    MetadataIndexNode metadataIndexNode = tableMetadataIndexNodeMap.get(tableName);
    if (metadataIndexNode == null) {
      metadataIndexNode = tableMetadataIndexNodeMap.get("");
    }
    return metadataIndexNode;
  }

  public boolean hasTableSchemaMapCache() {
    return hasTableSchemaMapCache;
  }

  public Map<String, TableSchema> getTableSchemaMap() {
    return tableSchemaMap;
  }

  public Map<String, byte[]> getTsFileProperties() {
    return tsFileProperties;
  }

  private static String getPropertyAsString(Map<String, byte[]> properties, String key) {
    byte[] value = properties.get(key);
    return value == null ? null : new String(value, TSFileConfig.STRING_CHARSET);
  }

  private static byte[] stringToBytes(String value) {
    return value.getBytes(TSFileConfig.STRING_CHARSET);
  }
}
