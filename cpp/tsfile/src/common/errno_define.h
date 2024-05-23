
#ifndef COMMON_ERRNO_DEFINE_H
#define COMMON_ERRNO_DEFINE_H

namespace timecho
{
namespace common
{

const int E_OK = 0;
const int E_OOM = 1;
const int E_NOT_EXIST = 2;
const int E_ALREADY_EXIST = 3;
const int E_INVALID_ARG = 4;
const int E_OUT_OF_RANGE = 5;
const int E_PARTIAL_READ = 6;
const int E_NET_BIND_ERR = 7;
const int E_NET_SOCKET_ERR = 8;
const int E_NET_EPOLL_ERR = 9;
const int E_NET_EPOLL_WAIT_ERR = 10;
const int E_NET_RECV_ERR = 11;
const int E_NET_ACCEPT_ERR = 12;
const int E_NET_FCNTL_ERR = 13;
const int E_NET_LISTEN_ERR = 14;
const int E_NET_SEND_ERR = 15;
const int E_PIPE_ERR = 16;
const int E_THREAD_CREATE_ERR = 17;
const int E_MUTEX_ERR = 18;
const int E_COND_ERR = 19;
const int E_OVERFLOW = 20;
const int E_NO_MORE_DATA = 21;
const int E_OUT_OF_ORDER = 22;
const int E_TSBLOCK_TYPE_NOT_SUPPORTED = 23;
const int E_TSBLOCK_DATA_INCONSISTENCY = 24;
const int E_DDL_UNKNOWN_TYPE = 25;
const int E_TYPE_NOT_SUPPORTED = 26;
const int E_TYPE_NOT_MATCH = 27;
const int E_FILE_OPEN_ERR = 28;
const int E_FILE_CLOSE_ERR = 29;
const int E_FILE_WRITE_ERR = 30;
const int E_FILE_READ_ERR = 31;
const int E_FILE_SYNC_ERR = 32;
const int E_TSFILE_WRITER_META_ERR = 33;
const int E_FILE_STAT_ERR = 34;
const int E_TSFILE_CORRUPTED = 35;
const int E_BUF_NOT_ENOUGH = 36;
const int E_INVALID_PATH = 37;
const int E_NOT_MATCH = 38;
const int E_JSON_INVALID = 39;
const int E_NOT_SUPPORT = 40;
const int E_PARSER_ERR = 41;
const int E_ANALYZE_ERR = 42;
const int E_INVALID_DATA_POINT = 43;
const int E_DEVICE_NOT_EXIST = 44;
const int E_MEASUREMENT_NOT_EXIST = 45;
const int E_INVALID_QUERY = 46;
const int E_SDK_QUERY_OPTIMIZE_ERR = 47;
const int E_COMPRESS_ERR = 48;

} // end namespace common
} // end namespace timecho

#endif // COMMON_ERRNO_DEFINE_H

