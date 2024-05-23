#ifndef COMMON_GLOBAL_H
#define COMMON_GLOBAL_H

#include <string>
#include <vector>
#include "common/config/config.h"
#include "common/allocator/byte_stream.h"

namespace timecho
{
namespace common
{

extern ConfigValue g_config_value_;

FORCE_INLINE bool wal_cfg_enabled() { return g_config_value_.wal_flush_policy_ != WAL_DISABLED; }
FORCE_INLINE bool wal_cfg_should_wait_persisted() { return g_config_value_.wal_flush_policy_ >= WAL_FLUSH; }

extern ColumnDesc g_time_column_desc;
extern int init_common();
extern bool is_timestamp_column_name(const char *time_col_name);
extern void cols_to_json(ByteStream *byte_stream, std::vector<common::ColumnDesc> &ret_ts_list);
extern void print_backtrace();

}
}

#endif  // COMMON_GLOBAL_H
