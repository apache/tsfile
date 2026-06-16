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

package org.apache.tsfile.spark;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class TsFileTableBatchWrite implements BatchWrite {

  private final TsFileTableWriteContext context;
  private final boolean truncate;
  private final String queryId;

  public TsFileTableBatchWrite(TsFileTableWriteContext context, boolean truncate, String queryId) {
    this.context = context;
    this.truncate = truncate;
    this.queryId = queryId;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new TsFileTableDataWriterFactory(context, queryId);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    try {
      Path outputPath = context.outputPath();
      if (truncate) {
        deleteVisibleOutput(outputPath);
      }
      Files.createDirectories(outputPath);
      for (WriterCommitMessage message : messages) {
        if (!(message instanceof TsFileTableWriterCommitMessage)) {
          continue;
        }
        TsFileTableWriterCommitMessage tsfileMessage = (TsFileTableWriterCommitMessage) message;
        if (tsfileMessage.tempFile() == null) {
          continue;
        }
        Files.move(
            Path.of(tsfileMessage.tempFile()),
            Path.of(tsfileMessage.finalFile()),
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
      }
      Path temporaryPath = outputPath.resolve("_temporary");
      deleteRecursively(temporaryPath.resolve(TsFileTableDataWriterFactory.safeId(queryId)));
      deleteIfEmpty(temporaryPath);
    } catch (IOException e) {
      throw new TsFileSparkException("Failed to commit TsFile Spark write", e);
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    if (messages != null) {
      for (WriterCommitMessage message : messages) {
        if (message instanceof TsFileTableWriterCommitMessage) {
          TsFileTableWriterCommitMessage tsfileMessage = (TsFileTableWriterCommitMessage) message;
          deleteIfPresent(tsfileMessage.tempFile());
          deleteIfPresent(tsfileMessage.finalFile());
        }
      }
    }
    Path temporaryPath = context.outputPath().resolve("_temporary");
    deleteRecursively(temporaryPath.resolve(TsFileTableDataWriterFactory.safeId(queryId)));
    deleteIfEmpty(temporaryPath);
  }

  private void deleteVisibleOutput(Path outputPath) throws IOException {
    if (!Files.exists(outputPath)) {
      return;
    }
    if (!Files.isDirectory(outputPath)) {
      Files.delete(outputPath);
      return;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(outputPath)) {
      for (Path child : stream) {
        if (!"_temporary".equals(child.getFileName().toString())) {
          deleteRecursively(child);
        }
      }
    }
  }

  private static void deleteIfPresent(String path) {
    if (path != null) {
      deleteRecursively(Path.of(path));
    }
  }

  private static void deleteIfEmpty(Path path) {
    if (path == null || !Files.isDirectory(path)) {
      return;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      if (!stream.iterator().hasNext()) {
        Files.deleteIfExists(path);
      }
    } catch (IOException e) {
      throw new TsFileSparkException(
          "Failed to delete empty path during TsFile write cleanup: " + path, e);
    }
  }

  private static void deleteRecursively(Path path) {
    if (path == null || !Files.exists(path)) {
      return;
    }
    try {
      if (Files.isDirectory(path)) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
          for (Path child : stream) {
            deleteRecursively(child);
          }
        }
      }
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new TsFileSparkException(
          "Failed to delete path during TsFile write cleanup: " + path, e);
    }
  }
}
