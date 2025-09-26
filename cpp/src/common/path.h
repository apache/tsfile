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

#include <string>

#include "common/device_id.h"
#include "parser/generated/PathParser.h"
#include "parser/path_nodes_generator.h"
#include "utils/errno_define.h"

namespace storage {

struct Path {
    std::string measurement_;
    std::shared_ptr<IDeviceID> device_id_;
    std::string full_path_;

    Path() {}

    Path(std::string &device, std::string &measurement)
        : measurement_(measurement),
          device_id_(std::make_shared<StringArrayDeviceID>(device)) {
        full_path_ = device + "." + measurement;
    }

    Path(const std::string &path_sc, bool if_split = true) {
        if (!path_sc.empty()) {
            if (!if_split) {
                full_path_ = path_sc;
                device_id_ = std::make_shared<StringArrayDeviceID>(path_sc);
            } else {
                std::vector<std::string> nodes =
                    PathNodesGenerator::invokeParser(path_sc);
                if (nodes.size() > 1) {
                    device_id_ = std::make_shared<StringArrayDeviceID>(
                        std::vector<std::string>(nodes.begin(),
                                                 nodes.end() - 1));
                    measurement_ = nodes[nodes.size() - 1];
                    full_path_ =
                        device_id_->get_device_name() + "." + measurement_;
                } else {
                    full_path_ = path_sc;
                    device_id_ = std::make_shared<StringArrayDeviceID>();
                    measurement_ = path_sc;
                }
            }
        } else {
            full_path_ = "";
            device_id_ = std::make_shared<StringArrayDeviceID>();
            measurement_ = "";
        }
    }

    bool operator==(const Path &path) {
        if (measurement_.compare(path.measurement_) == 0 &&
            device_id_->get_device_name().compare(
                path.device_id_->get_device_name()) == 0) {
            return true;
        } else {
            return false;
        }
    }
};

}  // namespace storage

#endif  // COMMON_READ_COMMON_PATH_H
