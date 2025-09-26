/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#ifndef ENCODING_ENCODER_FACTORY_H
#define ENCODING_ENCODER_FACTORY_H

#include "common/global.h"
#include "encoder.h"
#include "gorilla_encoder.h"
#include "plain_encoder.h"
#include "ts2diff_encoder.h"

namespace storage {

#define ALLOC_AND_RETURN_ENCODER(EncoderType)                                \
    do {                                                                     \
        void *buf =                                                          \
            common::mem_alloc(sizeof(EncoderType), common::MOD_ENCODER_OBJ); \
        if (buf != nullptr) {                                                \
            EncoderType *encoder = new (buf) EncoderType;                    \
            return encoder;                                                  \
        } else {                                                             \
            return nullptr;                                                  \
        }                                                                    \
    } while (false)

class EncoderFactory {
   public:
    static Encoder *alloc_time_encoder() {
        if (common::g_config_value_.time_encoding_type_ == common::PLAIN) {
            ALLOC_AND_RETURN_ENCODER(PlainEncoder);
        } else if (common::g_config_value_.time_encoding_type_ ==
                   common::TS_2DIFF) {
            ALLOC_AND_RETURN_ENCODER(LongTS2DIFFEncoder);
        } else {
            // not support now
            ASSERT(false);
            return nullptr;
        }
    }

    static Encoder *alloc_value_encoder(common::TSEncoding encoding,
                                        common::TSDataType data_type) {
        if (encoding == common::PLAIN) {
            ALLOC_AND_RETURN_ENCODER(PlainEncoder);
        } else if (encoding == common::DICTIONARY) {
            return nullptr;
        } else if (encoding == common::RLE) {
            return nullptr;
        } else if (encoding == common::DIFF) {
            return nullptr;
        } else if (encoding == common::TS_2DIFF) {
            if (data_type == common::INT32) {
                ALLOC_AND_RETURN_ENCODER(IntTS2DIFFEncoder);
            } else if (data_type == common::INT64) {
                ALLOC_AND_RETURN_ENCODER(LongTS2DIFFEncoder);
            } else if (data_type == common::FLOAT) {
                ALLOC_AND_RETURN_ENCODER(FloatTS2DIFFEncoder);
            } else if (data_type == common::DOUBLE) {
                ALLOC_AND_RETURN_ENCODER(DoubleTS2DIFFEncoder);
            } else {
                ASSERT(false);
            }
        } else if (encoding == common::BITMAP) {
            return nullptr;
        } else if (encoding == common::GORILLA_V1) {
            return nullptr;
        } else if (encoding == common::REGULAR) {
            return nullptr;
        } else if (encoding == common::GORILLA) {
            if (data_type == common::INT32) {
                ALLOC_AND_RETURN_ENCODER(IntGorillaEncoder);
            } else if (data_type == common::INT64) {
                ALLOC_AND_RETURN_ENCODER(LongGorillaEncoder);
            } else if (data_type == common::FLOAT) {
                ALLOC_AND_RETURN_ENCODER(FloatGorillaEncoder);
            } else if (data_type == common::DOUBLE) {
                ALLOC_AND_RETURN_ENCODER(DoubleGorillaEncoder);
            } else {
                ASSERT(false);
            }
        } else if (encoding == common::ZIGZAG) {
            return nullptr;
        } else if (encoding == common::FREQ) {
            return nullptr;
        } else {
            // not support now
            ASSERT(false);
            return nullptr;
        }
        return nullptr;
    }

    static void free(Encoder *encoder) { common::mem_free(encoder); }
};

}  // end namespace storage
#endif  // ENCODING_ENCODER_FACTORY_H
