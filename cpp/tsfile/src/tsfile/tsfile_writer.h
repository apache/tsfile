
#ifndef MAIN_LIBTSFILE_TSFILE_WRITER_H
#define MAIN_LIBTSFILE_TSFILE_WRITER_H

#include <string>
#include <vector>
#include <map>
#include <fcntl.h>
#include "record.h"
#include "tablet.h"
#include "schema.h"
#include "common/container/simple_vector.h"

namespace timecho
{

namespace storage
{
class WriteFile;
class ChunkWriter;
class TsFileIOWriter;
}

namespace storage
{

extern int libtsfile_init();
extern void libtsfile_destroy();
extern void set_page_max_point_count(uint32_t page_max_ponint_count);
extern void set_max_degree_of_index_node(uint32_t max_degree_of_index_node);

class TsFileWriter
{
public:
  TsFileWriter();
  ~TsFileWriter();
  void destroy();

  int open(const std::string &file_path, int flags, mode_t mode);
  int init(timecho::storage::WriteFile *write_file);

  int register_timeseries(const std::string &device_path,
                          const std::string &measurement_name,
                          common::TSDataType data_type,
                          common::TSEncoding encoding,
                          common::CompressionType compression_type);
  int write_record(const TsRecord &record);
  int write_tablet(const Tablet &tablet);

  /*
   * Flush buffer to disk file, but do not write file index part.
   * TsFileWriter allows user to flush many times.
   */
  int flush();

  /*
   * Flush file index part of the whole file (it may be flushed many times
   * before close, the index part should cover all data in disk file).
   */
  int close();

private:
  int write_point(timecho::storage::ChunkWriter *chunk_writer,
                  int64_t timestamp,
                  const DataPoint &point);
  int flush_chunk_group(MeasurementSchemaGroup *chunk_group);

  int write_typed_column(timecho::storage::ChunkWriter *chunk_writer,
                         int64_t *timestamps,
                         bool *col_values,
                         timecho::common::BitMap &col_bitmap,
                         int32_t row_count);
  int write_typed_column(timecho::storage::ChunkWriter *chunk_writer,
                         int64_t *timestamps,
                         int32_t *col_values,
                         timecho::common::BitMap &col_bitmap,
                         int32_t row_count);
  int write_typed_column(timecho::storage::ChunkWriter *chunk_writer,
                         int64_t *timestamps,
                         int64_t *col_values,
                         timecho::common::BitMap &col_bitmap,
                         int32_t row_count);
  int write_typed_column(timecho::storage::ChunkWriter *chunk_writer,
                         int64_t *timestamps,
                         float *col_values,
                         timecho::common::BitMap &col_bitmap,
                         int32_t row_count);
  int write_typed_column(timecho::storage::ChunkWriter *chunk_writer,
                         int64_t *timestamps,
                         double *col_values,
                         timecho::common::BitMap &col_bitmap,
                         int32_t row_count);

  template<typename MeasurementNamesGetter>
  int do_check_schema(const std::string &device_name,
                      MeasurementNamesGetter &measurement_names,
                      common::SimpleVector<timecho::storage::ChunkWriter*> &chunk_writers);
                      // std::vector<timecho::storage::ChunkWriter*> &chunk_writers);
  int write_column(timecho::storage::ChunkWriter *chunk_writer,
                   const Tablet &tablet,
                   int col_idx);
  int register_timeseries(const std::string &device_path,
                          MeasurementSchema *measurement_schema);
  int register_timeseries(const std::string &device_path,
                          const std::vector<MeasurementSchema*> &measurement_schema_vec);

private:
  timecho::storage::WriteFile *write_file_;
  timecho::storage::TsFileIOWriter *io_writer_;
  // device_name -> MeasurementSchemaGroup
  std::map<std::string, MeasurementSchemaGroup*> schemas_;
  bool start_file_done_;
  bool write_file_created_;
};

} // end namespace storage
} // end namespace timecho

#endif

