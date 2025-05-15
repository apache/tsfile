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

#include "error_info.h"

#include <iostream>
#include <unordered_map>

namespace error_info {

static std::string tsfile_err_msg;
static E_CODE tsfile_err_code;
static std::string tsfile_err_filename;
static int tsfile_err_line;
static std::string tsfile_err_function;

static const std::unordered_map<int, const char*> err_name_map = {
#define ERRNO(name, val, desc) {val, desc},
#include "error_define.inc"

#undef ERRNO
};

std::string error_name(E_CODE code) {
    auto it = err_name_map.find(code);
    return it == err_name_map.end() ? "unknown error" : it->second;
}

void set_err_no(E_CODE error) { tsfile_err_code = error; }

void set_err_msg(const std::string& msg) { tsfile_err_msg = msg; }

void set_err_info(E_CODE error, const std::string& msg, const std::string& file,
                  int line, const std::string& function) {
    tsfile_err_code = error;
    tsfile_err_msg = msg;
    tsfile_err_filename = file;
    tsfile_err_line = line;
    tsfile_err_function = function;
}

void print_err_info() {
    std::cout << " error no is " << tsfile_err_code << "("
              << error_name(tsfile_err_code) << ")";
    std::cout << " error message : " << tsfile_err_msg;
}

}  // namespace error_info
