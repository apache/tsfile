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
package org.apache.tsfile.write.writer;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.TsFileSequenceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ForceAppendTsFileWriter opens a COMPLETE TsFile, reads and truncate its metadata to support
 * appending new data.
 */
public class ForceAppendTsFileWriter extends TsFileIOWriter {

  private long truncatePosition;
  private static Logger logger = LoggerFactory.getLogger(ForceAppendTsFileWriter.class);
  private EncryptParameter ownedEncryptParameter;

  public ForceAppendTsFileWriter(File file) throws IOException {
    this(
        file,
        new EncryptParameter(
            TSFileDescriptor.getInstance().getConfig().getEncryptType(),
            TSFileDescriptor.getInstance().getConfig().getEncryptKey()));
  }

  public ForceAppendTsFileWriter(File file, EncryptParameter param) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug(Messages.get("log.write.writer_opened"), file.getName());
    }
    this.file = file;

    // file doesn't exist
    if (file.length() == 0 || !file.exists()) {
      throw new TsFileNotCompleteException(
          Messages.format("error.write.force_append_not_complete", file.getPath()));
    }
    this.out = FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), true);
    setEncryptParam(param);

    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(file.getAbsolutePath(), param, true)) {

      EncryptParameter recoveredParameter = reader.getEncryptParam();
      if (recoveredParameter != null && recoveredParameter.isTdePageAead()) {
        ownedEncryptParameter = recoveredParameter.copy();
        setEncryptParam(ownedEncryptParameter);
      }
      markExistingFileStarted(recoveredParameter != null && recoveredParameter.isTdePageAead());

      // this tsfile is not complete
      if (!reader.isComplete()) {
        throw new TsFileNotCompleteException(
            Messages.format("error.write.force_append_not_complete", file.getPath()));
      }
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      // truncate metadata and marker
      truncatePosition = tsFileMetadata.getMetaOffset();

      canWrite = true;
      List<IDeviceID> devices = reader.getAllDevices();
      for (IDeviceID device : devices) {
        List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
        reader.readChunkMetadataInDevice(device).values().forEach(chunkMetadataList::addAll);
        ChunkGroupMetadata chunkGroupMetadata = new ChunkGroupMetadata(device, chunkMetadataList);
        chunkGroupMetadataList.add(chunkGroupMetadata);
      }
    } catch (IOException | RuntimeException e) {
      closeAfterFailedInitialization(e);
      throw e;
    }
  }

  public void doTruncate() throws IOException {
    out.truncate(truncatePosition);
  }

  public long getTruncatePosition() {
    return truncatePosition;
  }

  @Override
  public void endFile() throws IOException {
    try {
      super.endFile();
    } finally {
      closeOwnedEncryptParameter();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      closeOwnedEncryptParameter();
    }
  }

  private void closeOwnedEncryptParameter() {
    if (ownedEncryptParameter != null) {
      ownedEncryptParameter.close();
    }
  }

  private void closeAfterFailedInitialization(Exception exception) {
    try {
      out.close();
    } catch (IOException e) {
      exception.addSuppressed(e);
    } finally {
      closeOwnedEncryptParameter();
    }
  }
}
