/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TSFILE_CLI_WRITE_COLUMNS_H
#define TSFILE_CLI_WRITE_COLUMNS_H

#include <string>

namespace tsfile_cli {

// A column declaration as it appears in the write command line. Keep the
// fields structured instead of encoding them into a delimiter-based string.
struct WriteColumnSpec {
    std::string name;
    std::string type_name;
    bool is_tag = false;

    WriteColumnSpec(const std::string& name_value,
                    const std::string& type_name_value, bool tag)
        : name(name_value), type_name(type_name_value), is_tag(tag) {}
};

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_WRITE_COLUMNS_H
