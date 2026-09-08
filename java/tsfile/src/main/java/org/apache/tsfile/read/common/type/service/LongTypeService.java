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
package org.apache.tsfile.read.common.type.service;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

/**
 * A LongTypeService wraps a switch(dataType) clause into a checked service class. Implementations
 * should cover all Types that can be instanced from TsDataTypes, and throw
 * UnsupportedDataTypeException when an unrecognized type is encountered.
 *
 * <p>It returns long only.
 */
public interface LongTypeService {

  long call(Type type);

  default void check() {
    for (TSDataType tsDataType : TSDataType.values()) {
      Type type = null;
      try {
        type = Type.fromTsDataType(tsDataType);
      } catch (UnsupportedOperationException e) {
        // ignore types that cannot be instanced
      }

      if (type != null) {
        call(type);
      }
    }
  }
}
