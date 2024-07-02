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

#ifndef ENCODING_ENCODER_H
#define ENCODING_ENCODER_H

#include "common/allocator/byte_stream.h"

namespace storage {

class Encoder {
   public:
    Encoder() {}
    virtual ~Encoder() {}

    virtual void reset() = 0;
    virtual void destroy() = 0;
    // virtual int init(common::TSDataType data_type) = 0;
    virtual int encode(bool value, common::ByteStream &out_stream) = 0;
    virtual int encode(int32_t value, common::ByteStream &out_stream) = 0;
    virtual int encode(int64_t value, common::ByteStream &out_stream) = 0;
    virtual int encode(float value, common::ByteStream &out_stream) = 0;
    virtual int encode(double value, common::ByteStream &out_stream) = 0;
    virtual int flush(common::ByteStream &out_stream) = 0;

    /**
     * The maximal possible memory size occupied by current Encoder. This
     * statistic value doesn't involve OutputStream.
     *
     * @return the maximal size of possible memory occupied by current encoder
     */
    virtual int get_max_byte_size() = 0;
};

}  // end namespace storage
#endif  // ENCODING_ENCODER_H
