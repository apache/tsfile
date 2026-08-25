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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef TSFILE_CLI_ATOMIC_OUTPUT_H
#define TSFILE_CLI_ATOMIC_OUTPUT_H

#include <ostream>
#include <string>

namespace tsfile_cli {

int prepare_atomic_output(const std::string& target_path,
                          const std::string& source_path, bool force,
                          std::string& temp_path, std::ostream& err);
int commit_atomic_output(const std::string& temp_path,
                         const std::string& target_path, bool force,
                         std::ostream& err);
bool remove_atomic_temp(const std::string& temp_path, std::ostream& err);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_ATOMIC_OUTPUT_H
