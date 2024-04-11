package org.apache.tsfile.read.query.executor.task;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.query.executor.TableQueryExecutor.ColumnMapping;
import org.apache.tsfile.utils.Pair;

import java.util.Iterator;
import java.util.List;

public class DeviceTaskIterator implements Iterator<DeviceQueryTask> {
  private List<String> columnNames;
  private ColumnMapping columnMapping;
  private TableSchema tableSchema;
  private Iterator<Pair<IDeviceID, MetadataIndexNode>> deviceMetaIterator;

  public DeviceTaskIterator(
      List<String> columnNames,
      MetadataIndexNode indexRoot,
      ColumnMapping columnMapping,
      IMetadataQuerier metadataQuerier,
      ExpressionTree idFilter,
      TableSchema tableSchema) {
    this.columnNames = columnNames;
    this.columnMapping = columnMapping;
    this.deviceMetaIterator = metadataQuerier.deviceIterator(indexRoot, idFilter);
    this.tableSchema = tableSchema;
  }

  @Override
  public boolean hasNext() {
    return deviceMetaIterator.hasNext();
  }

  @Override
  public DeviceQueryTask next() {
    final Pair<IDeviceID, MetadataIndexNode> next = deviceMetaIterator.next();
    return new DeviceQueryTask(next.left, columnNames, columnMapping, next.right, tableSchema);
  }
}
