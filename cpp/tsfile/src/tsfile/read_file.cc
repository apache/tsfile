
#include "read_file.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "common/logger/elog.h"
#include "tsfile_common.h"

namespace timecho
{
using namespace common;
namespace storage
{

void ReadFile::close()
{
  if (fd_ > 0) {
    ::close(fd_);
    fd_ = -1;
  }
  file_size_ = -1;
}

int ReadFile::open(const std::string &file_path)
{
  int ret = E_OK;
  file_path_ = file_path;
  fd_ = ::open(file_path_.c_str(), O_RDONLY);
  if (fd_ < 0) {
      std::cout<<"open file "<< file_path << "  error :" << fd_ << std::endl;
      std::cout<< "open error" << errno <<"  " << strerror(errno) << std::endl;
    return E_FILE_OPEN_ERR;
  }

  if (RET_FAIL(get_file_size(file_size_))) {
  } else if (RET_FAIL(check_file_magic())) {
  }
  if (IS_FAIL(ret)) {
    ::close(fd_);
  }
  return ret;
}

int ReadFile::get_file_size(int32_t &file_size)
{
  struct stat s;
  if (fstat(fd_, &s) < 0) {
    log_error("fstat error, file_path=%s, fd=%d, errno=%d", file_path_.c_str(), fd_, errno);
    return E_FILE_STAT_ERR;
  }
  file_size = s.st_size;
  return E_OK;
}

int ReadFile::check_file_magic()
{
  int ret = E_OK;
  if (file_size_ < MIN_FILE_SIZE) {
    ret = E_TSFILE_CORRUPTED;
    log_error("tsfile %s is corrupted, file_size=%d", file_path_.c_str(), file_size_);
  } else {
    char buf[MAGIC_STRING_TSFILE_LEN];
    int32_t read_len = 0;
    // file header magic
    memset(buf, 0, MAGIC_STRING_TSFILE_LEN);
    if (RET_FAIL(read(0, buf, MAGIC_STRING_TSFILE_LEN, read_len))) {
    } else if (read_len != MAGIC_STRING_TSFILE_LEN) {
      ret = E_TSFILE_CORRUPTED;
    } else if (memcmp(buf, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) != 0) {
      ret = E_TSFILE_CORRUPTED;
    }

    // file footer magic
    memset(buf, 0, MAGIC_STRING_TSFILE_LEN);
    if (RET_FAIL(read(file_size_ - MAGIC_STRING_TSFILE_LEN, buf, MAGIC_STRING_TSFILE_LEN, read_len))) {
    } else if (read_len != MAGIC_STRING_TSFILE_LEN) {
      ret = E_TSFILE_CORRUPTED;
    } else if (memcmp(buf, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) != 0) {
      ret = E_TSFILE_CORRUPTED;
    }
  }
  return ret;
}

int ReadFile::read(int32_t offset, char *buf, int32_t buf_size, int32_t &read_len)
{
  int ret = E_OK;
  read_len = 0;
  while (read_len < buf_size) {
    ssize_t pread_size = ::pread(fd_, buf + read_len, buf_size - read_len, offset + read_len);
    if (pread_size < 0) {
      ret = E_FILE_READ_ERR;
      log_error("tsfile read error, file_path=%s, errno=%d", file_path_.c_str(), errno);
      break;
    } else if (pread_size == 0) {
      break;
    } else {
      read_len += pread_size;
    }
  }
  return ret;
}
 
} // end namespace storage
} // end namespace timecho

