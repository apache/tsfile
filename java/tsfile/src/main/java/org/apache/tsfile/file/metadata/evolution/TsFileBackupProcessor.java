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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * TsFileBackupWriter is responsible for writing a backup of a suffix of a TsFile and recover a
 * TsFile from its backup file.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TsFileBackupProcessor {

  public static final String BACKUP_FILE_SUFFIX = ".backup";

  public static void writeBackup(File tsFile, long backupPosition) throws IOException {
    // Implementation for writing backup files goes here.
    File backupFile = new File(tsFile.getAbsolutePath() + BACKUP_FILE_SUFFIX);
    long backupLength = tsFile.length() - backupPosition;
    try (FileChannel backupChannel = new FileOutputStream(backupFile).getChannel();
         FileChannel originalChannel = new FileInputStream(tsFile).getChannel()) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(backupPosition);
      buffer.flip();
      backupChannel.write(buffer);
      buffer.flip();
      buffer.putLong(backupLength);
      buffer.flip();
      backupChannel.write(buffer);

      originalChannel.transferTo(backupPosition, backupLength, backupChannel);
    }
  }

  public static boolean hasBackup(File tsFile) {
    File backupFile = new File(tsFile.getAbsolutePath() + BACKUP_FILE_SUFFIX);
    return backupFile.exists();
  }

  public static void removeBackup(File tsFile) {
    File backupFile = new File(tsFile.getAbsolutePath() + BACKUP_FILE_SUFFIX);
    backupFile.delete();
  }

  public static void recoverFromBackup(File tsFile) throws IOException {
    // Implementation for recovering from backup files goes here.
    File backupFile = new File(tsFile.getAbsolutePath() + BACKUP_FILE_SUFFIX);
    if (!backupFile.exists()) {
      return;
    }

    try (FileChannel backupChannel = new FileInputStream(backupFile).getChannel();
         FileChannel originalChannel = new FileOutputStream(tsFile, true).getChannel()) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
      int read = backupChannel.read(buffer);
      if (read != Long.BYTES * 2) {
        // backup file is not complete, no need to recover
        return;
      }
      buffer.flip();
      long backupPosition = buffer.getLong();
      long backupLength = buffer.getLong();

      if (backupFile.length() < Long.BYTES * 2 + backupLength) {
        // backup file is not complete, no need to recover
        return;
      }

      originalChannel.position(backupPosition);
      originalChannel.truncate(backupPosition);
      backupChannel.transferTo(Long.BYTES * 2, backupLength, originalChannel);
    } finally {
      backupFile.delete();
    }
  }
}
