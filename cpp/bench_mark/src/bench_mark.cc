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

#include <iostream>
#include <string>

#include "bench_mark_c_cpp.h"

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <cpp|c>" << std::endl;
        return 1;
    }

    std::string mode(argv[1]);

    if (mode == "cpp") {
        bench_mark_cpp_write();
        bench_mark_cpp_read();
    } else if (mode == "c") {
        bench_mark_c_write();
        bench_mark_c_read();
    } else {
        std::cerr << "Invalid mode. Use 'cpp' or 'c'." << std::endl;
        return 1;
    }

    return 0;
}
