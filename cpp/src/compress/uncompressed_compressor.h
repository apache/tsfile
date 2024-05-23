
#ifndef COMPRESS_UNCOMPRESSED_COMPRESSOR_H
#define COMPRESS_UNCOMPRESSED_COMPRESSOR_H

#include "compressor.h"

namespace storage {

class UncompressedCompressor : public Compressor {
   public:
    UncompressedCompressor() {}
    virtual ~UncompressedCompressor() {}
    int reset(bool for_compress) {
        UNUSED(for_compress);
        return common::E_OK;
    }
    void destroy() {}
    int compress(char *uncompressed_buf, uint32_t uncompressed_buf_len,
                 char *&compressed_buf, uint32_t &compressed_buf_len) {
        compressed_buf = uncompressed_buf;
        compressed_buf_len = uncompressed_buf_len;
        return common::E_OK;
    }
    void after_compress(char *compressed_buf) { UNUSED(compressed_buf); }

    int uncompress(char *compressed_buf, uint32_t compressed_buf_len,
                   char *&uncompressed_buf, uint32_t &uncompressed_buf_len) {
        uncompressed_buf = compressed_buf;
        uncompressed_buf_len = compressed_buf_len;
        return common::E_OK;
    }
    void after_uncompress(char *uncompressed_buf) { UNUSED(uncompressed_buf); }
};

}  // end namespace storage
#endif  // COMPRESS_UNCOMPRESSED_COMPRESSOR_H
