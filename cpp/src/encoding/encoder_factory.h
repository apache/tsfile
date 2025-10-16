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
#include "dictionary_encoder.h"
#include "double_sprintz_encoder.h"
#include "encoder.h"
#include "encoding/int32_rle_encoder.h"
#include "encoding/int64_rle_encoder.h"
#include "float_sprintz_encoder.h"
#include "gorilla_encoder.h"
#include "int32_sprintz_encoder.h"
#include "int64_sprintz_encoder.h"
#include "plain_encoder.h"
#include "ts2diff_encoder.h"
#include "zigzag_encoder.h"

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
            return nullptr;
        }
    }

    static Encoder *alloc_time_encoder(common::TSEncoding encoding) {
        if (encoding == common::PLAIN) {
            ALLOC_AND_RETURN_ENCODER(PlainEncoder);
        } else if (encoding == common::TS_2DIFF) {
            ALLOC_AND_RETURN_ENCODER(LongTS2DIFFEncoder);
        } else {
            // not support now
            return nullptr;
        }
    }

    static Encoder *alloc_value_encoder(common::TSEncoding encoding,
                                        common::TSDataType data_type) {
        using namespace common;

        switch (encoding) {
            case PLAIN:
                ALLOC_AND_RETURN_ENCODER(PlainEncoder);

            case DICTIONARY:
                switch (data_type) {
                    case STRING:
                    case TEXT:
                        ALLOC_AND_RETURN_ENCODER(DictionaryEncoder);
                    default:
                        return nullptr;
                }

            case RLE:
                switch (data_type) {
                    case INT32:
                    case DATE:
                        ALLOC_AND_RETURN_ENCODER(Int32RleEncoder);
                    case INT64:
                    case TIMESTAMP:
                        ALLOC_AND_RETURN_ENCODER(Int64RleEncoder);
                    default:
                        return nullptr;
                }

            case TS_2DIFF:
                switch (data_type) {
                    case INT32:
                    case DATE:
                        ALLOC_AND_RETURN_ENCODER(IntTS2DIFFEncoder);
                    case INT64:
                    case TIMESTAMP:
                        ALLOC_AND_RETURN_ENCODER(LongTS2DIFFEncoder);
                    case FLOAT:
                        ALLOC_AND_RETURN_ENCODER(FloatTS2DIFFEncoder);
                    case DOUBLE:
                        ALLOC_AND_RETURN_ENCODER(DoubleTS2DIFFEncoder);
                    default:
                        return nullptr;
                }

            case GORILLA:
                switch (data_type) {
                    case INT32:
                    case DATE:
                        ALLOC_AND_RETURN_ENCODER(IntGorillaEncoder);
                    case INT64:
                    case TIMESTAMP:
                        ALLOC_AND_RETURN_ENCODER(LongGorillaEncoder);
                    case FLOAT:
                        ALLOC_AND_RETURN_ENCODER(FloatGorillaEncoder);
                    case DOUBLE:
                        ALLOC_AND_RETURN_ENCODER(DoubleGorillaEncoder);
                    default:
                        return nullptr;
                }

            case ZIGZAG:
                switch (data_type) {
                    case INT32:
                        ALLOC_AND_RETURN_ENCODER(IntZigzagEncoder);
                    case INT64:
                        ALLOC_AND_RETURN_ENCODER(LongZigzagEncoder);
                    default:
                        return nullptr;
                }

            case SPRINTZ:
                switch (data_type) {
                    case INT32:
                        ALLOC_AND_RETURN_ENCODER(Int32SprintzEncoder);
                    case INT64:
                        ALLOC_AND_RETURN_ENCODER(Int64SprintzEncoder);
                    case FLOAT:
                        ALLOC_AND_RETURN_ENCODER(FloatSprintzEncoder);
                    case DOUBLE:
                        ALLOC_AND_RETURN_ENCODER(DoubleSprintzEncoder);
                    default:
                        return nullptr;
                }

            case DIFF:
            case BITMAP:
            case GORILLA_V1:
            case REGULAR:
            case FREQ:
                return nullptr;

            default:
                return nullptr;
        }
        return nullptr;
    }

    static void free(Encoder *encoder) {
        encoder->~Encoder();
        common::mem_free(encoder);
    }
};

}  // end namespace storage
#endif  // ENCODING_ENCODER_FACTORY_H
