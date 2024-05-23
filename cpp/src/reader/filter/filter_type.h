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
#ifndef READER_FILTER_FACTORY_FILTER_TYPE_H
#define READER_FILTER_FACTORY_FILTER_TYPE_H

#include <string>

#include "common/db_common.h"

namespace storage {

enum FilterType { VALUE_FILTER, TIME_FILTER, GROUP_BY_FILTER };

FORCE_INLINE std::string filter_type_to_string(FilterType type) {
    switch (type) {
        case VALUE_FILTER: {
            return std::string("value");
        }
        case TIME_FILTER: {
            return std::string("time");
        }
        case GROUP_BY_FILTER: {
            return std::string("group by");
        }
        default: {
            std::cout << "filter_type_to_string unknown type:" << type
                      << std::endl;
            return "";
        }
    }
}

}  // namespace storage

#endif  // READER_FILTER_FACTORY_FILTER_TYPE_H
