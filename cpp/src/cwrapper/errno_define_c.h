/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef CWRAPPER_ERRNO_DEFINE_H
#define CWRAPPER_ERRNO_DEFINE_H

#define RET_OK 0
#define RET_OOM 1
#define RET_NOT_EXIST 2
#define RET_ALREADY_EXIST 3
#define RET_INVALID_ARG 4
#define RET_OUT_OF_RANGE 5
#define RET_PARTIAL_READ 6
#define RET_INVALID_SCHEMA 8
#define RET_OVERFLOW 20
#define RET_NO_MORE_DATA 21
#define RET_OUT_OF_ORDER 22
#define RET_TSBLOCK_DATA_INCONSISTENCY 24
#define RET_TYPE_NOT_SUPPORTED 26
#define RET_TYPE_NOT_MATCH 27
#define RET_FILE_OPEN_ERR 28
#define RET_FILE_CLOSE_ERR 29
#define RET_FILE_WRITE_ERR 30
#define RET_FILE_READ_ERR 31
#define RET_FILE_SYNC_ERR 32
#define RET_TSFILE_WRITER_META_ERR 33
#define RET_FILE_STAT_ERR 34
#define RET_TSFILE_CORRUPTED 35
#define RET_BUF_NOT_ENOUGH 36
#define RET_INVALID_PATH 37
#define RET_NOT_MATCH 38
#define RET_NOT_SUPPORT 40
#define RET_INVALID_DATA_POINT 43
#define RET_DEVICE_NOT_EXIST 44
#define RET_MEASUREMENT_NOT_EXIST 45
#define RET_COMPRESS_ERR 48
#define RET_TABLE_NOT_EXIST 49
#define RET_COLUMN_NOT_EXIST 50
#define RET_UNSUPPORTED_ORDER 51
#define RET_INVALID_NODE_TYPE 52
#define RET_ENCODE_ERR 53
#define RET_DECODE_ERR 54

/*
 * Deprecated legacy values retained for source compatibility. The current
 * TsFile implementation does not return these codes.
 */
#define RET_NET_EPOLL_ERR 9
#define RET_NET_EPOLL_WAIT_ERR 10
#define RET_NET_RECV_ERR 11
#define RET_NET_ACCEPT_ERR 12
#define RET_NET_FCNTL_ERR 13
#define RET_NET_LISTEN_ERR 14
#define RET_NET_SEND_ERR 15
#define RET_PIPE_ERR 16
#define RET_THREAD_CREATE_ERR 17
#define RET_MUTEX_ERR 18
#define RET_COND_ERR 19
#define RET_TSBLOCK_TYPE_NOT_SUPPORTED 23
#define RET_DDL_UNKNOWN_TYPE 25
#define RET_JSON_INVALID 39
#define RET_PARSER_ERR 41
#define RET_ANALYZE_ERR 42
#define RET_SDK_QUERY_OPTIMIZE_ERR 47

/* Backward-compatible aliases for identifiers published with misspellings. */
#define RET_PIPRET_ERR RET_PIPE_ERR
#define RET_THREAD_CREATRET_ERR RET_THREAD_CREATE_ERR
#define RET_NO_MORRET_DATA RET_NO_MORE_DATA
#define RET_TSBLOCK_TYPRET_NOT_SUPPORTED RET_TSBLOCK_TYPE_NOT_SUPPORTED
#define RET_TYPRET_NOT_SUPPORTED RET_TYPE_NOT_SUPPORTED
#define RET_TYPRET_NOT_MATCH RET_TYPE_NOT_MATCH
#define RET_FILRET_OPEN_ERR RET_FILE_OPEN_ERR
#define RET_FILRET_CLOSRET_ERR RET_FILE_CLOSE_ERR
#define RET_FILRET_WRITRET_ERR RET_FILE_WRITE_ERR
#define RET_FILRET_READ_ERR RET_FILE_READ_ERR
#define RET_FILRET_SYNC_ERR RET_FILE_SYNC_ERR
#define RET_TSFILRET_WRITER_META_ERR RET_TSFILE_WRITER_META_ERR
#define RET_FILRET_STAT_ERR RET_FILE_STAT_ERR
#define RET_TSFILRET_CORRUPTED RET_TSFILE_CORRUPTED
#define RET_ANALYZRET_ERR RET_ANALYZE_ERR
#define RET_DEVICRET_NOT_EXIST RET_DEVICE_NOT_EXIST
#define RET_SDK_QUERY_OPTIMIZRET_ERR RET_SDK_QUERY_OPTIMIZE_ERR
#define RET_TABLRET_NOT_EXIST RET_TABLE_NOT_EXIST
#define RET_INVALID_NODRET_TYPE RET_INVALID_NODE_TYPE

#endif /* CWRAPPER_ERRNO_DEFINE_H */
