
#ifndef FILE_WRITE_FILE_H
#define FILE_WRITE_FILE_H

#include <string>

#include "utils/util_define.h"
#ifndef LIBTSFILE_SDK
#include "utils/storage_utils.h"
#endif

namespace storage {

class WriteFile {
   public:
#ifndef LIBTSFILE_SDK
    WriteFile() : path_(), file_id_(), fd_(-1) {}
    FORCE_INLINE common::FileID get_file_id() const { return file_id_; }
    int create(const common::FileID &file_id, int flags, mode_t mode);
#else
    WriteFile() : path_(), fd_(-1) {}
#endif
    int create(const std::string &file_name, int flags, mode_t mode);
    bool file_opened() const { return fd_ > 0; }
    int write(const char *buf, uint32_t len);
    // int flush() { return common::E_OK; } // TODO
    int sync();
    int close();
    FORCE_INLINE std::string get_file_path() { return path_; }

   private:
    int do_create(int flags, mode_t mode);

   private:
    std::string path_;
#ifndef LIBTSFILE_SDK
    common::FileID file_id_;
#endif
    int fd_;
};

}  // end namespace storage
#endif  // FILE_WRITE_FILE_H
