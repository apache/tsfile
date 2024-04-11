package org.apache.tsfile.read.controller;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceMetaIterator implements Iterator<Pair<IDeviceID, MetadataIndexNode>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceMetaIterator.class);
  private final TsFileSequenceReader tsFileSequenceReader;
  private final Queue<MetadataIndexNode> metadataIndexNodes = new ArrayDeque<>();
  private final Queue<Pair<IDeviceID, MetadataIndexNode>> resultCache = new ArrayDeque<>();

  public DeviceMetaIterator(TsFileSequenceReader tsFileSequenceReader, MetadataIndexNode metadataIndexNode) {
    this.tsFileSequenceReader = tsFileSequenceReader;
    this.metadataIndexNodes.add(metadataIndexNode);
  }

  @Override
  public boolean hasNext() {
    if (!resultCache.isEmpty()) {
      return true;
    }
    try {
      loadResults();
    } catch (IOException e) {
      LOGGER.error("Failed to load device meta data", e);
      return false;
    }

    return !resultCache.isEmpty();
  }

  private void loadResults() throws IOException {
    while (!metadataIndexNodes.isEmpty()) {
      final MetadataIndexNode currentNode = metadataIndexNodes.poll();
      final MetadataIndexNodeType nodeType = currentNode.getNodeType();
      switch (nodeType) {
        case LEAF_DEVICE:
          List<IMetadataIndexEntry> leafChildren = currentNode.getChildren();
          for (int i = 0; i < leafChildren.size(); i++) {
            IMetadataIndexEntry child = leafChildren.get(i);
            final IDeviceID deviceID = ((DeviceMetadataIndexEntry) child).getDeviceID();
            long startOffset = child.getOffset();
            long endOffset = i < leafChildren.size() - 1 ? leafChildren.get(i + 1).getOffset() :
                currentNode.getEndOffset();
            final MetadataIndexNode childNode = tsFileSequenceReader.readMetadataIndexNode(
                startOffset, endOffset, false);
            resultCache.add(new Pair<>(deviceID, childNode));
          }
          return;
        case INTERNAL_DEVICE:
          List<IMetadataIndexEntry> internalChildren = currentNode.getChildren();
          for (int i = 0; i < internalChildren.size(); i++) {
            IMetadataIndexEntry child = internalChildren.get(i);
            long startOffset = child.getOffset();
            long endOffset = i < internalChildren.size() - 1 ? internalChildren.get(i + 1).getOffset() :
                currentNode.getEndOffset();
            final MetadataIndexNode childNode = tsFileSequenceReader.readMetadataIndexNode(
                startOffset, endOffset, true);
            metadataIndexNodes.add(childNode);
          }
          break;
        default:
          throw new IOException("A non-device node detected: " + currentNode);
      }
    }
  }

  @Override
  public Pair<IDeviceID, MetadataIndexNode> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return resultCache.poll();
  }
}
