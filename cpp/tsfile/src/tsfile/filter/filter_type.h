#ifndef STORAGE_TSFILE_READ_FILTER_FACTORY_FILTER_TYPE_H
#define STORAGE_TSFILE_READ_FILTER_FACTORY_FILTER_TYPE_H

#include <string>

#include "common/db_common.h"

namespace timecho
{
namespace storage
{

enum FilterType
{
  VALUE_FILTER,
  TIME_FILTER,
  GROUP_BY_FILTER
};

FORCE_INLINE std::string filter_type_to_string(FilterType type)
{
  switch (type) {
    case VALUE_FILTER: {
      return std::string("value");
    }
    case TIME_FILTER: {
      return std::string("time");
    }
    case GROUP_BY_FILTER: {
      return std::string("group by");
    }
    default: {
      std::cout << "filter_type_to_string unknown type:" << type << std::endl;
      return "";
    }
  }
}

}  // storage
}  // timecho

#endif  // STORAGE_TSFILE_READ_FILTER_FACTORY_FILTER_TYPE_H
