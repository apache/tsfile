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

#ifndef ENCODING_PLAIN_DECODER_H
#define ENCODING_PLAIN_DECODER_H

#include "encoding/decoder.h"

namespace storage {

class PlainDecoder : public Decoder {
   public:
    FORCE_INLINE void reset() { /* do nothing */
    }
    FORCE_INLINE bool has_remaining() { return false; }
    FORCE_INLINE int read_boolean(bool &ret_bool, common::ByteStream &in) {
        return common::SerializationUtil::read_ui8((uint8_t &)ret_bool, in);
    }

    FORCE_INLINE int read_int32(int32_t &ret_int32, common::ByteStream &in) {
        return common::SerializationUtil::read_var_int(ret_int32, in);
    }

    FORCE_INLINE int read_int64(int64_t &ret_int64, common::ByteStream &in) {
        return common::SerializationUtil::read_i64(ret_int64, in);
    }

    FORCE_INLINE int read_float(float &ret_float, common::ByteStream &in) {
        return common::SerializationUtil::read_float(ret_float, in);
    }

    FORCE_INLINE int read_double(double &ret_double, common::ByteStream &in) {
        return common::SerializationUtil::read_double(ret_double, in);
    }
};

}  // end namespace storage
#endif  // ENCODING_PLAIN_DECODER_H
