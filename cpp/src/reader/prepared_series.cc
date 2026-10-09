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

#include "reader/prepared_series.h"

namespace storage {

PreparedSeries::PreparedSeries(const FileGeneration& generation,
                               const PreparedLocator& locator)
    : generation_(generation), locator_(locator), arena_(), index_(nullptr) {
    arena_.init(512, common::MOD_TSFILE_READER);
}

PreparedSeries::~PreparedSeries() {
    index_ = nullptr;
    arena_.destroy();
}

}  // namespace storage
