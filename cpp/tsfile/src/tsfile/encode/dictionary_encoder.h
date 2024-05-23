
#ifndef STORAGE_TSFILE_ENCODE_DICTIONARY_ENCODER_H
#define STORAGE_TSFILE_ENCODE_DICTIONARY_ENCODER_H

#include "encoder.h"
#include "common/allocator/byte_stream.h"
#include "storage/tsfile/encode/bitpack_encoder.h"
#include <map>
#include <vector>
#include <string>

namespace timecho
{
namespace storage
{

class DictionaryEncoder
{

private:
  std::map<std::string , int> entry_index_;
  std::vector<std::string> index_entry_;
  BitPackEncoder values_encoder_;
  int map_size_;

public:
  DictionaryEncoder() {}
  ~DictionaryEncoder() {}

  void init()
  {
    map_size_ = 0;
    values_encoder_.init();
  }

  void reset() 
  { 
    entry_index_.clear();
    index_entry_.clear();
    map_size_ = 0;
    values_encoder_.reset();
  }

  void encode(std::string value, common::ByteStream &out)
  {
    
    if(entry_index_.count(value) == 0){
      index_entry_.push_back(value);
      map_size_ = map_size_ + value.length();
      entry_index_[value] = entry_index_.size();
    }
    values_encoder_.encode(entry_index_[value], out);
  }

  int flush(common::ByteStream &out)
  {
    int ret = common::E_OK;
    ret = write_map(out);
    if (ret !=common::E_OK) {
      return ret;
    } else {
      write_encoded_data(out);
    }
    if (ret !=common::E_OK) { 
      return ret;
    }
    return common::E_OK;
  }

  int write_map(common::ByteStream &out)
  {
    int ret = common::E_OK;
    if (RET_FAIL(common::SerializationUtil::write_var_int((int)index_entry_.size(), out))) { 
      return ret;
    } else {
      for(int i = 0; i < (int)index_entry_.size(); i++) {
        if (RET_FAIL(common::SerializationUtil::write_str(index_entry_[i],out))) {
          return common::E_FILE_WRITE_ERR;
        }
      }
    }
    return common::E_OK;
  }

  void write_encoded_data(common::ByteStream &out)
  {
    values_encoder_.encode_flush(out);
  }
    
};

} // end namespace storage
} // end namespace timecho

#endif //STORAGE_TSFILE_ENCODE_DICTIONARY_ENCODER_H

