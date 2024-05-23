#include "tsblock.h"

#include <string>

namespace timecho
{
namespace common
{

int TsBlock::init()
{
  // todo: The current version select_list_ is useless, so it is not initialized
  int ret = 0;
  uint32_t row_size = tuple_desc_->get_single_row_len(&ret);
  if (ret != 0) {
    return ret;
  }

  // No maximum number of rows given, calculated with capacity_
  if (LIKELY(max_row_count_ == 0)) {
    max_row_count_ = capacity_ / row_size;
  } else {
    // max_row_count_ given, calculated with max_row_count_
    capacity_ = row_size * max_row_count_;
  }
  int colnum = tuple_desc_->get_column_count();
  for (int i = 0; i < colnum; ++i) {
    ret = build_vector(tuple_desc_->get_column_type(i), max_row_count_);
    if (ret != 0) {
      return ret;
    }
  }
  return ret;
}

int TsBlock::build_vector(common::TSDataType type, uint32_t row_count)
{
  Vector *vec;
  int ret = 0;
  if (LIKELY(type != common::TEXT)) {
    vec = new FixedLengthVector(type, row_count, get_len(type), this);
  } else if (type == common::TEXT) {
    vec = new VariableLengthVector(type, row_count, DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN, this);
  } else {
    log_error("TsBlock::BuildVector failed because of unknown type %d", static_cast<int>(type));
    ret = E_TYPE_NOT_SUPPORTED;
  }
  vectors_.push_back(vec);
  return ret;
}

void TsBlock::write_data(ByteStream* __restrict byte_stream, char* __restrict val, uint32_t len,
    bool has_null, TSDataType type)
{
  std::string strval;
  switch (type) {
    case timecho::common::INT64: {
      int64_t ival = *reinterpret_cast<int64_t*>(val);
      strval = to_string(ival);
      break;
    }
    case timecho::common::INT32: {
      int32_t ival = *reinterpret_cast<int32_t*>(val);
      strval = to_string(ival);
      break;
    }
    case timecho::common::FLOAT: {
      float ival = *reinterpret_cast<float*>(val); // cppcheck-suppress invalidPointerCast
      strval = to_string(ival);
      break;
    }
    case timecho::common::DOUBLE: {
      double ival = *reinterpret_cast<double*>(val); // cppcheck-suppress invalidPointerCast
      strval = to_string(ival);
      break;
    }
    case timecho::common::BOOLEAN: {
      bool ival = *reinterpret_cast<bool*>(val);
      if (ival) {
        strval = "true";
      } else {
        strval = "false";
      }
      break;
    }
    case timecho::common::TEXT:{
      if (LIKELY(!has_null)) {
        byte_stream->write_buf(val, len);
      } else {
        byte_stream->write_buf("null", 4);
      }
      byte_stream->write_buf(",\n", 2);
      return;
    }
    default: {
      log_error("write_data unknown type");
    }
  }
  if (LIKELY(!has_null)) {
    byte_stream->write_buf(const_cast<char*>(strval.c_str()), strval.length());
  } else {
    byte_stream->write_buf("null", 4);
  }
}

void TsBlock::tsblock_to_json(ByteStream *byte_stream)
{
  // 1. append start tag
  byte_stream->write_buf("{\n", 2);

  // 2. append output columns to bytestream
  int column_count = tuple_desc_->get_column_count();
  // 2.1 append header
  byte_stream->write_buf("  \"expressions\": [\n", 19);
  // 2.2 append column names
  for (int i = 1; i < column_count; ++i) {
    std::string name = tuple_desc_->get_column_name(i);
    byte_stream->write_buf("    ", 4);
    byte_stream->write_buf("\"", 1);
    byte_stream->write_buf(name.c_str(), name.length());
    byte_stream->write_buf("\"", 1);
    if (i == column_count - 1) {
      byte_stream->write_buf("\n", 1);
    } else {
      byte_stream->write_buf(",\n", 2);
    }
  }
  byte_stream->write_buf("  ],\n", 5);

  // 3. append column_names
  byte_stream->write_buf("  \"column_names\": null,\n", 24);

  // 4. append time value
  byte_stream->write_buf("  \"timestamps\": [\n", 18);
  ColIterator time_iter(0, this);
  bool is_first = true;
  while (!time_iter.end()) {
    uint32_t ilen;
    byte_stream->write_buf("    ", 4);
    char *val = time_iter.read(&ilen);
    if (!is_first) {
      byte_stream->write_buf(",\n", 2);
    }
    is_first = false;
    write_data(byte_stream, val, ilen, false, INT64);
    time_iter.next();
  }
  byte_stream->write_buf("  ],\n", 5);

  // 5. append user values
  byte_stream->write_buf("  \"values\": [\n", 14);
  for (int i = 1; i < column_count; ++i) {
    byte_stream->write_buf("    [\n", 6);

    ColIterator value_iter(i, this);
    bool has_null = value_iter.has_null();
    if (LIKELY(!has_null)) {
      bool is_first = true; // cppcheck-suppress shadowVariable
      while (!value_iter.end()) {
        uint32_t ilen = 0;
        byte_stream->write_buf("      ", 6);
        char *val = value_iter.read(&ilen);
        if (!is_first) {
          byte_stream->write_buf(",\n", 2);
        }
        is_first = false;
        write_data(byte_stream, val, ilen, false, tuple_desc_->get_column_type(i));
        value_iter.next();
      }
    } else {
      while (!value_iter.end()) {
        bool inull;
        uint32_t ilen = 0;
        byte_stream->write_buf("      ", 6);
        char *val = value_iter.read(&ilen, &inull);
        if (!is_first) {
          byte_stream->write_buf(",\n", 2);
        }
        is_first = false;
        write_data(byte_stream, val, ilen, inull, tuple_desc_->get_column_type(i));
        value_iter.next();
      }
    }
    if (i == column_count - 1) {
      byte_stream->write_buf("    ]\n", 6);
    } else {
      byte_stream->write_buf("    ],\n", 7);
    }
  }
  byte_stream->write_buf("  ]\n", 4);

  // 6. end
  byte_stream->write_buf("}\n", 2);
}

std::string TsBlock::debug_string()
{
  std::stringstream out;
  out << "print TsBlock: " << this << std::endl
      << "capacity_: " << capacity_ << std::endl
      << "row_count_: " << row_count_ << std::endl
      << "max_row_count_: " << max_row_count_ << std::endl;

  out << "------ tuple desc ------" << std::endl << std::endl;
  out << tuple_desc_->debug_string() << std::endl;

  out << "------ real data area ------" << std::endl;

  RowIterator iter(this);
  while (!iter.end()) {
    out << iter.debug_string() << std::endl;
    iter.next();
  }
  return out.str();
}

std::string RowIterator::debug_string()
{
  std::stringstream out;
  out.precision(20);  // Default precision is 6, set precision, otherwise double precision will be lost

  for (uint32_t i = 0; i < column_count_; ++i) {
    bool is_null = false;
    uint32_t len = 0;
    void *value = read(i, &len, &is_null);
    if (is_null) {
      out << "NULL";
    } else {
      ColumnDesc &col_desc = tsblock_->tuple_desc_->get_column_desc(i);
      switch (col_desc.type_) {
        case timecho::common::BOOLEAN: {
          out << *static_cast<bool*>(value);
          break;
        }
        case timecho::common::INT32: {
          out << *static_cast<int32_t*>(value);
          break;
        }
        case timecho::common::INT64: {
          out << *static_cast<int64_t*>(value);
          break;
        }
        case timecho::common::FLOAT: {
          out << *static_cast<float*>(value);
          break;
        }
        case timecho::common::DOUBLE: {
          out << *static_cast<double*>(value);
          break;
        }
        case timecho::common::TEXT: {
          out << std::string(static_cast<char*>(value), len);
          break;
        }
        default: {
          out << "ERR";
        }
      }
    }
    if (i + 1 < tsblock_->tuple_desc_->get_column_count()) {
      out << ", ";
    }
  }
  return out.str();
}

// TODO use memcpy in vector instead of using iter/appender
int merge_tsblock_by_row(TsBlock *sea, TsBlock *river)
{
  int ret = E_OK;
  TupleDesc *sea_tuple_desc = sea->get_tuple_desc();
  TupleDesc *river_tupe_desc = river->get_tuple_desc();
  if (!sea_tuple_desc->equal_to(*river_tupe_desc)) {
    ret = E_NOT_MATCH;
  } else {
    RowAppender sea_appender(sea);
    RowIterator river_iter(river);
    while (!river_iter.end()) {
      sea_appender.add_row();
      for (uint32_t c = 0; c < sea_tuple_desc->get_column_count(); c++) {
        uint32_t len = 0;
        bool null = false;
        char *val = river_iter.read(c, &len, &null);
        sea_appender.append(c, val, len);
      }
      river_iter.next();
    }
  }
  return ret;
}

}  // common
}  // timecho
