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

package org.apache.tsfile.interop;

import java.util.List;

/** Metadata describing a test file for interoperability testing. */
public class TestFileMetadata {
  public String fileName;
  public String dataType;
  public String encoding;
  public String compression;
  public String pattern;
  public int valueCount;
  public List<Object> expectedValues;

  public TestFileMetadata() {}

  public TestFileMetadata(
      String fileName,
      String dataType,
      String encoding,
      String compression,
      String pattern,
      int valueCount,
      List<Object> expectedValues) {
    this.fileName = fileName;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compression = compression;
    this.pattern = pattern;
    this.valueCount = valueCount;
    this.expectedValues = expectedValues;
  }
}
