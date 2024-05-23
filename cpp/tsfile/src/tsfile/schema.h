
#ifndef MAIN_LIBTSFILE_SCHEMA_H
#define MAIN_LIBTSFILE_SCHEMA_H

#include <string>
#include <map> // use unordered_map instead
#include "common/db_common.h"

namespace timecho
{

namespace storage
{
class ChunkWriter;
}

namespace storage
{

/* schema information for one measurement */
struct MeasurementSchema
{
  std::string measurement_name_; // for example: "s1"
  common::TSDataType data_type_;
  common::TSEncoding encoding_;
  common::CompressionType compression_type_;
  timecho::storage::ChunkWriter *chunk_writer_;

  MeasurementSchema()
    : measurement_name_(),
      data_type_(common::INVALID_DATATYPE),
      encoding_(common::INVALID_ENCODING),
      compression_type_(common::INVALID_COMPRESSION),
      chunk_writer_(NULL) {}
              
  MeasurementSchema(const std::string &measurement_name,
                    common::TSDataType data_type,
                    common::TSEncoding encoding,
                    common::CompressionType compression_type)
    : measurement_name_(measurement_name),
      data_type_(data_type),
      encoding_(encoding),
      compression_type_(compression_type),
      chunk_writer_(NULL) {}
};

typedef std::map<std::string, MeasurementSchema*> MeasurementSchemaMap;
typedef std::map<std::string, MeasurementSchema*>::iterator MeasurementSchemaMapIter;
typedef std::pair<MeasurementSchemaMapIter, bool> MeasurementSchemaMapInsertResult;

/* schema information for a device */
struct MeasurementSchemaGroup
{
  // measurement_name -> MeasurementSchema
  MeasurementSchemaMap measurement_schema_map_;
  bool is_aligned_; // currently not used.
};

} // end namespace storage
} // end namespace timecho

#endif // MAIN_LIBTSFILE_SCHEMA_H

