package org.apache.tsfile.compatibility;

import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;

public class DeserializeContext {
  public Deserializer<TsFileMetadata> tsFileMetadataDeserializer = TsFileMetadata::deserializeFrom;

  public Deserializer<MetadataIndexNode> deviceMetadataIndexNodeDeserializer =
      (buffer, context) -> MetadataIndexNode.deserializeFrom(buffer, true, context);
  public Deserializer<MetadataIndexNode> measurementMetadataIndexNodeDeserializer =
      (buffer, context) -> MetadataIndexNode.deserializeFrom(buffer, false, context);
  public Deserializer<IMetadataIndexEntry> deviceMetadataIndexEntryDeserializer =
      DeviceMetadataIndexEntry::deserializeFrom;
  public Deserializer<IMetadataIndexEntry> measurementMetadataIndexEntryDeserializer =
      ((buffer, context) -> MeasurementMetadataIndexEntry.deserializeFrom(buffer));

  public Deserializer<TableSchema> tableSchemaDeserializer = TableSchema::deserialize;
  public Deserializer<MeasurementSchema> measurementSchemaDeserializer =
      ((buffer, context) -> MeasurementSchema.deserializeFrom(buffer));

  public Deserializer<IDeviceID> deviceIDDeserializer =
      ((buffer, context) -> StringArrayDeviceID.deserialize(buffer));

  public MetadataIndexNode deserilizeMetadataIndexNode(ByteBuffer buffer, boolean isDeviceLevel) {
    if (isDeviceLevel) {
      return deviceMetadataIndexNodeDeserializer.deserialize(buffer, this);
    } else {
      return measurementMetadataIndexNodeDeserializer.deserialize(buffer, this);
    }
  }
}
