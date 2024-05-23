
#include "write_file.h"
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include "common/logger/elog.h"
#include "common/errno_define.h"
#include "common/config/config.h"

using namespace timecho::common;

namespace timecho
{
namespace storage
{

#ifndef LIBTSFILE_SDK
int WriteFile::create(const FileID &file_id, int flags, mode_t mode)
{
  if (fd_ > 0) {
    log_error("file already opened, fd=%d", fd_);
    ASSERT(false);
    return E_ALREADY_EXIST;
  }
  file_id_ = file_id;
  path_ = get_file_path_from_file_id(file_id_);
  return do_create(flags, mode);
}
#endif

int WriteFile::create(const std::string &file_path, int flags, mode_t mode)
{
  if (fd_ > 0) {
    log_error("file already opened, fd=%d", fd_);
    ASSERT(false);
    return E_ALREADY_EXIST;
  }
  path_ = file_path;
  return do_create(flags, mode);
}

int WriteFile::do_create(int flags, mode_t mode)
{
  int ret = E_OK;
  // TODO make sure no same file exists
  fd_ = ::open(path_.c_str(), flags, mode);
  if (fd_ < 0) {
    log_error("open file error, path=%s, errno=%d", path_.c_str(), errno);
    ret = E_FILE_OPEN_ERR;
  } else {
  }
  return ret;
}

int WriteFile::write(const char *buf, uint32_t len)
{
  ASSERT(fd_ > 0);

#if 0 // DEBUG_SE
    struct stat statbuf;
    if (fstat(fd_, &statbuf) < 0) {
      perror("fstat");
      ASSERT(false);
    }
    printf("write buffer(%u bytes) to file at %zu\n", len, statbuf.st_size);
    for (uint32_t i = 0; i < len;) {
      printf("0x%02x ", (uint8_t)buf[i]);
      if ((++i) % 16 == 0) {
        printf("\n");
      }
    }
    printf("\n");
#endif

  int ret = E_OK;
  uint32_t write_done = 0;
  while (write_done < len && IS_SUCC(ret)) {
    int32_t cur_write = ::write(fd_, buf + write_done, len - write_done);
    if (cur_write < 0) {
      ret = E_FILE_WRITE_ERR;
      log_error("file write error, path=%s, error=%d", path_.c_str(), errno);
    } else {
      write_done += cur_write;
    }
  }
  return ret;
}

int WriteFile::sync()
{
  ASSERT(fd_ > 0);
  if (::fsync(fd_) < 0) {
    log_error("file fsync error, path=%s, errno=%d", path_.c_str(), errno);
    return E_FILE_SYNC_ERR;
  }
  return E_OK;
}

int WriteFile::close()
{
  ASSERT(fd_ > 0);
  if (::close(fd_) < 0) {
      std::cout<<"feild to close " << path_ << " errorno " << errno<< std::endl;
      log_error("file close error, path=%s, errno=%d", path_.c_str(), errno);
    return E_FILE_CLOSE_ERR;
  }
  std::cout<<"coulse finish" << std::endl;
  return E_OK;
}

} // end namespace storage
} // end namespace timecho
