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

#ifndef COMMON_ERROR_INFO_H
#define COMMON_ERROR_INFO_H

#include <string>

namespace error_info {

enum E_CODE {
#define ERRNO(name, val, desc) name = val,
#include "error_define.inc"

#undef ERRNO
};

std::string error_name(E_CODE code);
void print_err_info();
void set_err_no(int error);
void set_err_msg(const std::string& msg);
void set_err_info(int error, const std::string& msg, const std::string& file,
                  int line, const std::string& function);

#define RETURN_ERR(code, msg)                                       \
    do {                                                            \
        ::error_info::set_err_info((code), msg, __FILE__, __LINE__, \
                                   __FUNCTION__);                   \
        return (code);                                              \
    } while (0);
}  // namespace error_info

#endif