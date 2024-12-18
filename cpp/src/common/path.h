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
#ifndef COMMON_READ_COMMON_PATH_H
#define COMMON_READ_COMMON_PATH_H

#include "utils/errno_define.h"
#include "parser/generated/PathParser.h"
#include "parser/path_nodes_generator.h"

#include <string>

namespace storage {

struct Path {
    std::string measurement_;
    std::string device_;
    std::string full_path_;

    Path() {}

    Path(std::string &device, std::string &measurement)
        : measurement_(measurement), device_(device) {
        full_path_ = device + "." + measurement;
    }

    Path(const std::string& path_sc, bool if_split = true) {
        if (!path_sc.empty()) {
            if (!if_split) {
                full_path_ = path_sc;
                device_ = path_sc; 
            } else {
                std::vector<std::string> nodes = PathNodesGenerator::invokeParser(path_sc);
                if (nodes.size() > 0) {
                    for (uint64_t i = 0; i + 1 < nodes.size(); i++) {
                        device_ += nodes[i] + (i + 2 < nodes.size() ? "." : "");
                    }
                    measurement_ = nodes[nodes.size() - 1];
                    full_path_ = device_ + "." + measurement_;
                } else {
                    full_path_ = path_sc;
                    device_ = "";
                    measurement_ = path_sc;
                }
            }
        }
    }

    bool operator==(const Path &path) {
        if (measurement_.compare(path.measurement_) == 0 &&
            device_.compare(path.device_) == 0) {
            return true;
        } else {
            return false;
        }
    }
};

}  // namespace storage

#endif  // COMMON_READ_COMMON_PATH_H
