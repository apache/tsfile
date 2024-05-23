#include <iostream>
#include <vector>
#include <string>
#include "tsfile/path.h"
#include "tsfile/filter/filter.h"
#include "tsfile/expression.h"
#include "tsfile/tsfile_reader.h"
#include "tsfile/qds_with_timegenerator.h"
#include "tsfile/qds_without_timegenerator.h"
#include "tsfile/row_record.h"

using namespace std;

std::string field_to_string(timecho::storage::Field *value)
{
  if (value->type_ == timecho::common::TEXT) {
    return std::string(value->value_.sval_);
  } else {
    std::stringstream ss;
    switch (value->type_) {
      case timecho::common::BOOLEAN:
        ss << (value->value_.bval_ ? "true" : "false");
        break;
      case timecho::common::INT32:
        ss << value->value_.ival_;
        break;
      case timecho::common::INT64:
        ss << value->value_.lval_;
        break;
      case timecho::common::FLOAT:
        ss << value->value_.fval_;
        break;
      case timecho::common::DOUBLE:
        ss << value->value_.dval_;
        break;
      case timecho::common::NULL_TYPE:
        ss << "NULL";
        break;
      default:
        ASSERT(false);
        break;
    }
    return ss.str();
  }
}

int main (int argc, char **argv)
{
    std::cout<<"begin to read tsfile from /tmp/t1" << std::endl;
    std::string device_name = "root.db001.dev001";
    std::string measurement_name = "m001";
    timecho::storage::Path p1(device_name, measurement_name);
    std::vector<timecho::storage::Path> select_list;
    select_list.push_back(p1);
    timecho::storage::QueryExpression *query_expr =
            timecho::storage::QueryExpression::create(select_list, nullptr);

    timecho::common::init_config_value();
    timecho::storage::TsFileReader reader;
    int ret = reader.open("/tmp/t1");
    std::cout <<"open file return " << ret<< std::endl;

    std::cout << "begin to query expr" << std::endl;
    ASSERT(ret == 0);
    timecho::storage::QueryDataSet *qds = nullptr;
    ret = reader.query(query_expr, qds);


    timecho::storage::RowRecord *record;
    std::cout << "begin to dump data from tsfile ---" << std::endl;
    int row_cout = 0;
    do {
        record = qds->get_next();
        if (record) {
            std::cout<< "dump QDS :  " << record->get_timestamp() << ",";
            if (record) {
                int size = record->get_fields()->size();
                for (int i = 0; i < size; ++i) {
                    std::cout << field_to_string(record->get_field(i)) << ",";
                }
                std::cout<< std::endl;
                row_cout++;
            }
        } else {
            break;
        }
    } while (true);
    std::cout << "dump tsfile finish" << std::endl;
    std::cout << "total row count = " << row_cout<<std::endl;

    return  (0);
}
