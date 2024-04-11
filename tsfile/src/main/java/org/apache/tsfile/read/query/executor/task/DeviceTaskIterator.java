package org.apache.tsfile.read.query.executor.task;

import java.util.Iterator;
import java.util.List;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.query.executor.TableQueryExecutor.ColumnMapping;
import org.apache.tsfile.utils.Pair;

public class DeviceTaskIterator implements Iterator<DeviceQueryTask> {
  private List<String> columnNames;
  private ColumnMapping columnMapping;
  private Iterator<Pair<IDeviceID, MetadataIndexNode>> deviceMetaIterator;

  public DeviceTaskIterator(List<String> columnNames, MetadataIndexNode indexRoot,
      ColumnMapping columnMapping, IMetadataQuerier metadataQuerier, ExpressionTree idFilter) {
    this.columnNames = columnNames;
    this.columnMapping = columnMapping;
    this.deviceMetaIterator = metadataQuerier.deviceIterator(indexRoot, idFilter);
  }

  @Override
  public boolean hasNext() {
    return deviceMetaIterator.hasNext();
  }

  @Override
  public DeviceQueryTask next() {
    final Pair<IDeviceID, MetadataIndexNode> next = deviceMetaIterator.next();
    return new DeviceQueryTask(next.left, columnNames, columnMapping, next.right);
  }
}
