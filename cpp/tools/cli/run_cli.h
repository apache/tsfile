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

#ifndef TSFILE_CLI_RUN_CLI_H
#define TSFILE_CLI_RUN_CLI_H

#include <ostream>
#include <string>
#include <vector>

namespace tsfile_cli {

int run_cli(const std::vector<std::string>& args, std::ostream& out,
            std::ostream& err);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_RUN_CLI_H
