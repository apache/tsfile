
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
};

}  // end namespace storage
#endif  // ENCODING_PLAIN_ENCODER_H
