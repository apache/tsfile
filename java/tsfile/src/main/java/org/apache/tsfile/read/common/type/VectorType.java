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

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.tsfile.encoding.decoder.LongChimpDecoder;
import org.apache.tsfile.encoding.decoder.LongGorillaDecoder;
import org.apache.tsfile.encoding.decoder.LongRleDecoder;
import org.apache.tsfile.encoding.decoder.RegularDataDecoder;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

public class VectorType extends AbstractLongType {

  public static final VectorType VECTOR = new VectorType();

  private VectorType() {}

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN, DICTIONARY -> super.getDecoder(encoding);
      case RLE -> new LongRleDecoder();
      case TS_2DIFF -> new DeltaBinaryDecoder.LongDeltaDecoder();
      case REGULAR -> new RegularDataDecoder.LongRegularDecoder();
      case GORILLA -> new LongGorillaDecoder();
      case CHIMP -> new LongChimpDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.VECTOR;
  }

  @Override
  public String getDisplayName() {
    return "VECTOR";
  }

  public static VectorType getInstance() {
    return VECTOR;
  }
}
