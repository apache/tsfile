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

#include "common/path.h"

#include "common/constant/tsfile_constant.h"

#ifdef ENABLE_ANTLR4
#include "parser/path_nodes_generator.h"
#endif

namespace storage {

Path::Path() = default;

Path::Path(std::string& device, std::string& measurement)
    : measurement_(measurement),
      device_id_(std::make_shared<StringArrayDeviceID>(device)) {
    full_path_ = device + "." + measurement;
}

Path::Path(const std::string& path_sc, bool if_split) {
    if (!path_sc.empty()) {
        if (!if_split) {
            full_path_ = path_sc;
            device_id_ = std::make_shared<StringArrayDeviceID>(path_sc);
        } else {
#ifdef ENABLE_ANTLR4
            std::vector<std::string> nodes =
                PathNodesGenerator::invokeParser(path_sc);
#else
            std::vector<std::string> nodes =
                IDeviceID::split_string(path_sc, '.');
#endif
            if (nodes.size() > 1) {
                // Join nodes, then parse like write path / Java Path (not
                // per-segment vector).
                std::string device_joined;
                for (size_t i = 0; i + 1 < nodes.size(); ++i) {
                    if (i > 0) {
                        device_joined += PATH_SEPARATOR_CHAR;
                    }
                    device_joined += nodes[i];
                }
                device_id_ =
                    std::make_shared<StringArrayDeviceID>(device_joined);
                measurement_ = nodes[nodes.size() - 1];
                full_path_ = device_id_->get_device_name() + "." + measurement_;
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

}  // namespace storage
