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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class TSEncodingBuilderTest {

  private static final String ERROR_MSG = "Unsupported dataType: %s doesn't support data type: %s";

  @Test
  public void testTSEncodingBuilder() {
    Set<TSDataType> supportedDataTypes =
        Arrays.stream(TSDataType.values()).collect(Collectors.toSet());
    supportedDataTypes.remove(TSDataType.VECTOR);
    supportedDataTypes.remove(TSDataType.UNKNOWN);

    for (TSDataType dataType : supportedDataTypes) {
      for (TSEncoding encoding : TSEncoding.values()) {
        if (TSEncoding.isSupported(dataType, encoding)) {
          try {
            TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
          } catch (UnSupportedDataTypeException e) {
            Assert.fail(e.getMessage());
          }
        } else {
          try {
            TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
            Assert.fail(String.format(ERROR_MSG, encoding, dataType));
          } catch (UnsupportedOperationException e) {
            Assert.assertEquals("Unsupported encoding: " + encoding, e.getMessage());
          } catch (UnSupportedDataTypeException e) {
            Assert.assertEquals(String.format(ERROR_MSG, encoding, dataType), e.getMessage());
          }
        }
      }
    }
  }
}
