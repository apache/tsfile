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

import org.apache.tsfile.annotations.TreeModel;
import org.apache.tsfile.annotations.TsFileApi;

import java.io.File;
import java.io.IOException;

/** Builder for TsFileTreeReader. */
public class TsFileTreeReaderBuilder {
  private File file;

  @TsFileApi
  @TreeModel
  public TsFileTreeReaderBuilder file(File file) {
    this.file = file;
    return this;
  }

  /**
   * Build an ITsFileTreeReader instance.
   *
   * @throws IllegalStateException if required fields are missing.
   */
  @TsFileApi
  @TreeModel
  public ITsFileTreeReader build() throws IOException {
    if (this.file == null) {
      throw new IllegalStateException("file must be set before build()");
    }
    return new TsFileTreeReader(this.file);
  }
}
