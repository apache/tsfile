#ifndef STORAGE_TSFILE_READ_FILTER_BASIC_OBJECT_H
#define STORAGE_TSFILE_READ_FILTER_BASIC_OBJECT_H

#include "common/db_common.h"

namespace timecho
{
namespace storage
{

class Object
{
public:
  union Value
  {
    bool    bval_;
    int32_t ival_;
    int64_t lval_;
    float   fval_;
    double  dval_;
    char    *sval_;
  } values_;

  Object() {}
  ~Object() {}

  Object(const bool &val)
  {
    type_ = common::BOOLEAN;
    values_.bval_ = val;
  }

  Object(const int32_t &val)
  {
    type_ = common::INT32;
    values_.ival_ = val;
  }

  Object(const int64_t &val)
  {
    type_ = common::INT64;
    values_.lval_ = val;
  }

  Object(const float &val)
  {
    type_ = common::FLOAT;
    values_.fval_ = val;
  }

  Object(const double &val)
  {
    type_ = common::DOUBLE;
    values_.dval_ = val;
  }

  Object(char *val)
  {
    type_ = common::TEXT;
    values_.sval_ = val;
  }

  Object(const std::string &val)
  {
    type_ = common::TEXT;
    values_.sval_ = const_cast<char*>(val.c_str());
  }

  FORCE_INLINE const common::TSDataType get_type() const { return type_; }

private:
  common::TSDataType type_;
};

}  // storage
}  // timecho

#endif  // STORAGE_TSFILE_READ_FILTER_BASIC_OBJECT_H
