
#ifndef MAIN_LIBTSFILE_TABLET_H
#define MAIN_LIBTSFILE_TABLET_H

#include <vector>
#include "schema.h"
#include "common/container/bit_map.h"

namespace timecho
{
namespace storage
{

class TabletRowIterator;
class TabletColIterator;

class Tablet
{
public:
  static const int DEFAULT_MAX_ROWS = 1024;
public:
  Tablet(const std::string &device_name,
         const std::vector<MeasurementSchema> *schema_vec,
         int max_rows = DEFAULT_MAX_ROWS)
    : max_rows_(max_rows),
      device_name_(device_name),
      schema_vec_(schema_vec),
      timestamps_(NULL),
      value_matrix_(NULL),
      bitmaps_(NULL)
  {
    ASSERT(device_name.size() >= 1);
    ASSERT(schema_vec != NULL);
    ASSERT(max_rows > 0 && max_rows < (1<<30));
    if (max_rows < 0) {
      ASSERT(false);
      max_rows_ = DEFAULT_MAX_ROWS;
    }
  }
  ~Tablet() { destroy(); }

  int init();
  void destroy();
  size_t get_column_count() const { return schema_vec_->size(); }

  int set_timestamp(int row_index, int64_t timestamp);

  int set_value(int row_index, uint32_t schema_index, bool val);
  int set_value(int row_index, uint32_t schema_index, int32_t val);
  int set_value(int row_index, uint32_t schema_index, int64_t val);
  int set_value(int row_index, uint32_t schema_index, float val);
  int set_value(int row_index, uint32_t schema_index, double val);
  // int set_value(int row_index, int schema_index, double val);

  int set_value(int row_index, const std::string &measurement_name, bool val);
  int set_value(int row_index, const std::string &measurement_name, int32_t val);
  int set_value(int row_index, const std::string &measurement_name, int64_t val);
  int set_value(int row_index, const std::string &measurement_name, float val);
  int set_value(int row_index, const std::string &measurement_name, double val);
  // int set_value(int row_index, const std::string &measurement_name, double val);

  friend class TabletColIterator;
  friend class TsFileWriter;
  friend struct MeasurementNamesFromTablet;

private:
  typedef std::map<std::string, int>::iterator SchemaMapIterator;
private:
  int max_rows_;
  std::string device_name_;
  const std::vector<MeasurementSchema> *schema_vec_;
  std::map<std::string, int> schema_map_;
  int64_t *timestamps_;
  void **value_matrix_;
  timecho::common::BitMap *bitmaps_;
};

} // end namespace storage
} // end namespace timecho

#endif // MAIN_LIBTSFILE_TABLET_H

