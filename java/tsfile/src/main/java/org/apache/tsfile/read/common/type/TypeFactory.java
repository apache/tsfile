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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.i18n.Messages;

public class TypeFactory {

  private TypeFactory() {
    // forbidding instantiation
  }

  public static Type getType(TSDataType tsDataType) {
    return switch (tsDataType) {
      case DATE -> DateType.getInstance();
      case INT32 -> IntType.getInstance();
      case INT64 -> LongType.getInstance();
      case TIMESTAMP -> TimestampType.getInstance();
      case FLOAT -> FloatType.getInstance();
      case DOUBLE -> DoubleType.getInstance();
      case BOOLEAN -> BooleanType.getInstance();
      case TEXT -> BinaryType.getInstance();
      case STRING -> StringType.getInstance();
      case BLOB -> BlobType.getInstance();
      case OBJECT -> ObjectType.getInstance();
      default ->
          throw new UnsupportedOperationException(
              Messages.format("error.read.typefactory_invalid_tsdata_type", tsDataType));
    };
  }

  public static Type getType(TypeEnum typeEnum) {
    return switch (typeEnum) {
      case INT32 -> IntType.getInstance();
      case INT64 -> LongType.getInstance();
      case FLOAT -> FloatType.getInstance();
      case DOUBLE -> DoubleType.getInstance();
      case BOOLEAN -> BooleanType.getInstance();
      case TEXT -> BinaryType.getInstance();
      case UNKNOWN -> UnknownType.getInstance();
      case DATE -> DateType.getInstance();
      case TIMESTAMP -> TimestampType.getInstance();
      case BLOB -> BlobType.getInstance();
      case STRING -> StringType.getInstance();
      default ->
          throw new UnsupportedOperationException(
              Messages.format("error.read.typefactory_invalid_type_enum", typeEnum));
    };
  }
}
