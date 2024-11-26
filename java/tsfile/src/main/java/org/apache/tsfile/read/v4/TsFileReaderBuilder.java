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

package org.apache.tsfile.read.v4;

import org.apache.tsfile.common.TsFileApi;

import java.io.File;
import java.io.IOException;

public class TsFileReaderBuilder {

  private File file;

  @TsFileApi
  public ITsFileReader build() throws IOException {
    validateParameters();
    return new DeviceTableModelReader(file);
  }

  @TsFileApi
  public TsFileReaderBuilder file(File file) {
    this.file = file;
    return this;
  }

  @TsFileApi
  private void validateParameters() {
    if (file == null || !file.exists() || file.isDirectory()) {
      throw new IllegalArgumentException("The file must be a non-null and non-directory File.");
    }
  }
}
