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

#ifndef ENCODING_PLAIN_ENCODER_H
#define ENCODING_PLAIN_ENCODER_H

#include "encoder.h"

namespace storage {

class PlainEncoder : public Encoder {
   public:
    PlainEncoder() {}
    ~PlainEncoder() { destroy(); }
    void destroy() { /* do nothing for PlainEncoder */
    }
    void reset() { /* do thing for PlainEncoder */
    }

    FORCE_INLINE int encode(bool value, common::ByteStream &out_stream) {
        return common::SerializationUtil::write_i8(value ? 1 : 0, out_stream);
    }

    FORCE_INLINE int encode(int32_t value, common::ByteStream &out_stream) {
        return common::SerializationUtil::write_var_int(value, out_stream);
    }

    FORCE_INLINE int encode(int64_t value, common::ByteStream &out_stream) {
        return common::SerializationUtil::write_i64(value, out_stream);
    }

    FORCE_INLINE int encode(float value, common::ByteStream &out_stream) {
        return common::SerializationUtil::write_float(value, out_stream);
    }

    FORCE_INLINE int encode(double value, common::ByteStream &out_stream) {
        return common::SerializationUtil::write_double(value, out_stream);
    }

    int flush(common::ByteStream &out_stream) {
        // do nothing for PlainEncoder
        return common::E_OK;
    }

    int get_max_byte_size() { return 0; }
};

}  // end namespace storage
#endif  // ENCODING_PLAIN_ENCODER_H
