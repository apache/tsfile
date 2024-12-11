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

package org.apache.tsfile.write.v4;

import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.file.metadata.TableSchema;

import java.io.File;
import java.io.IOException;

public class TsFileWriterBuilder {

  private static final long defaultMemoryThresholdInByte = 32 * 1024 * 1024;
  private File file;
  private TableSchema tableSchema;
  private long memoryThresholdInByte = defaultMemoryThresholdInByte;

  @TsFileApi
  public ITsFileWriter build() throws IOException {
    validateParameters();
    return new DeviceTableModelWriter(file, tableSchema, memoryThresholdInByte);
  }

  @TsFileApi
  public TsFileWriterBuilder file(File file) {
    this.file = file;
    return this;
  }

  @TsFileApi
  public TsFileWriterBuilder tableSchema(TableSchema schema) {
    this.tableSchema = schema;
    return this;
  }

  @TsFileApi
  public TsFileWriterBuilder memoryThreshold(long memoryThreshold) {
    this.memoryThresholdInByte = memoryThreshold;
    return this;
  }

  private void validateParameters() {
    if (file == null || file.isDirectory()) {
      throw new IllegalArgumentException("The file must be a non-null and non-directory File.");
    }
    if (this.tableSchema == null) {
      throw new IllegalArgumentException("TableSchema must not be null.");
    }
    if (this.memoryThresholdInByte <= 0) {
      throw new IllegalArgumentException("Memory threshold must be > 0 bytes.");
    }
  }
}
