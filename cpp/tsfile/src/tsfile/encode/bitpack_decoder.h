
#ifndef STORAGE_TSFILE_ENCODE_BITPACK_DECODER_H
#define STORAGE_TSFILE_ENCODE_BITPACK_DECODER_H

#include "encoder.h"
#include "common/allocator/alloc_base.h"
#include "storage/tsfile/encode/intpacker.h"
#include "storage/tsfile/encode/encode_utils.h"

#include <vector>

namespace timecho
{
namespace storage
{

class BitPackDecoder
{
private:
  uint32_t length_;
  uint32_t bit_width_;
  int bitpacking_num_;
  bool is_length_and_bitwidth_readed_;
  int current_count_;
  common::ByteStream byte_cache_;
  int *current_buffer_;
  IntPacker *packer_;
  uint8_t *tmp_buf;

public:
  BitPackDecoder() :byte_cache_(1024,common::MOD_DECODER_OBJ){}
  ~BitPackDecoder() { destroy(); }

  void init()
  {
    packer_ = nullptr;
    is_length_and_bitwidth_readed_ = false;
    length_ = 0;
    bit_width_ = 0;
    bitpacking_num_ = 0;
    current_count_ = 0;
  }

  bool has_next(common::ByteStream &buffer)
  {
    if (current_count_ > 0 || buffer.remaining_size()> 0 || has_next_package()) {
      return true;
    }
    return false;
  }

  bool has_next_package() 
  {
    return current_count_ > 0 || byte_cache_.remaining_size() > 0;
  }


  int read_int(common::ByteStream &buffer) 
  {
    if (!is_length_and_bitwidth_readed_) {
        // start to read a new rle+bit-packing pattern
        read_length_and_bitwidth(buffer);
    }
    if (current_count_ == 0){
      uint8_t header;
      int ret = common::E_OK;
      if (RET_FAIL(common::SerializationUtil::read_ui8(header, byte_cache_))){
        return ret;
      }
      call_read_bit_packing_buffer(header);
    }
    --current_count_;
    int result = current_buffer_[bitpacking_num_ - current_count_ - 1];
    if (!has_next_package()) {
      is_length_and_bitwidth_readed_ = false;
    }
    return result;
  }

  int call_read_bit_packing_buffer(uint8_t header)
  {
    int bit_packed_group_count = (int)(header >> 1);
    // in last bit-packing group, there may be some useless value,
    // lastBitPackedNum indicates how many values is useful
    uint8_t last_bit_packed_num;
    int ret = common::E_OK;
    if (RET_FAIL(common::SerializationUtil::read_ui8(last_bit_packed_num, byte_cache_))) { 
      return ret;
    }
    if (bit_packed_group_count > 0) {
      current_count_ = (bit_packed_group_count - 1) * 8 + last_bit_packed_num;
      bitpacking_num_ = current_count_;
    } else {
      printf("tsfile-encoding IntRleDecoder: bit_packed_group_count %d, smaller than 1",bit_packed_group_count);
    }
    read_bit_packing_buffer(bit_packed_group_count, last_bit_packed_num);
    return ret;
  }


  void read_bit_packing_buffer(int bit_packed_group_count, int last_bit_packed_num) 
  {
    current_buffer_ = new int[bit_packed_group_count * 8];
    unsigned char bytes[bit_packed_group_count * bit_width_];
    int bytes_to_read = bit_packed_group_count * bit_width_;
    if (bytes_to_read > (int)byte_cache_.remaining_size()) {
      bytes_to_read = byte_cache_.remaining_size();
    }
    for (int i = 0; i < bytes_to_read; i++) {
      common::SerializationUtil::read_ui8(bytes[i], byte_cache_);
    }
    // save all int values in currentBuffer
    packer_->unpack_all_values(bytes, bytes_to_read, current_buffer_);   //decode from bytes, save in currentBuffer
  }

  int read_length_and_bitwidth(common::ByteStream &buffer)
  {
    int ret = common::E_OK;
    if (RET_FAIL(common::SerializationUtil::read_var_uint(length_, buffer))) { 
      return common::E_PARTIAL_READ;
    } else {
      tmp_buf = (uint8_t *)common::mem_alloc(length_, common::MOD_DECODER_OBJ);
      if (tmp_buf == nullptr) {
        return common::E_OOM;
      }
      uint32_t ret_read_len = 0;
      if (RET_FAIL(buffer.read_buf((uint8_t *)tmp_buf, length_, ret_read_len))) {
        return ret;
      } else if (length_ != ret_read_len) {
        ret = common::E_PARTIAL_READ;
      } 
      byte_cache_.wrap_from((char *)tmp_buf, length_);
      is_length_and_bitwidth_readed_ = true;
      common::SerializationUtil::read_ui32(bit_width_, byte_cache_);
      init_packer();
    }
    return ret;
  }

  void init_packer()
  {
    packer_ = new IntPacker(bit_width_);
  }

  void destroy() 
  { /* do nothing for BitpackEncoder */
    delete(packer_);
    delete []current_buffer_;
    common::mem_free(tmp_buf);
  }

  void reset() 
  { 
    current_count_ = 0;
    is_length_and_bitwidth_readed_ = false;
    bitpacking_num_ = 0;
  }
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_ENCODE_BITPACK_ENCODER_H

