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
