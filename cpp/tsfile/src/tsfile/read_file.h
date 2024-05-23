
#ifndef STORAGE_TSFILE_READ_FILE_H
#define STORAGE_TSFILE_READ_FILE_H

#include <string>
#include <stdint.h>
#include "common/util_define.h"
#include "common/errno_define.h"

namespace timecho
{
namespace storage
{

class ReadFile
{
public:
  ReadFile() : file_path_(), fd_(-1), file_size_(-1) {}
  ~ReadFile() { destroy(); }
  void destroy() { close(); }

  int open(const std::string &file_path);
  FORCE_INLINE bool is_opened() const { return fd_ > 0; }
  FORCE_INLINE int32_t file_size() const { return file_size_; }
  FORCE_INLINE const std::string& file_path() const { return file_path_; }

  /*
   * try to read @buf_size bytes from @offset of this file
   * into @buf. @read_len return the actual len read.
   */
  int read(int32_t offset, char *buf, int32_t buf_size, int32_t &ret_read_len);
  void close();

private:
  int get_file_size(int32_t &file_size);
  int check_file_magic();

private:
  // 2 magic strings + file_version
  static const int32_t MIN_FILE_SIZE = 13;
private:
  std::string file_path_;
  int fd_;
  int32_t file_size_;
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_READ_FILE_H

