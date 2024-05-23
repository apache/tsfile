
#ifndef FILE_READ_FILE_H
#define FILE_READ_FILE_H

#include <stdint.h>

#include <string>

#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

class ReadFile {
   public:
    ReadFile() : file_path_(), fd_(-1), file_size_(-1) {}
    ~ReadFile() { destroy(); }
    void destroy() { close(); }

    int open(const std::string &file_path);
    FORCE_INLINE bool is_opened() const { return fd_ > 0; }
    FORCE_INLINE int32_t file_size() const { return file_size_; }
    FORCE_INLINE const std::string &file_path() const { return file_path_; }

    /*
     * try to reader @buf_size bytes from @offset of this file
     * into @buf. @read_len return the actual len reader.
     */
    int read(int32_t offset, char *buf, int32_t buf_size,
             int32_t &ret_read_len);
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

}  // end namespace storage
#endif  // FILE_READ_FILE_H
