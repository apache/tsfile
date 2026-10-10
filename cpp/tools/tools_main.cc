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

#include <exception>
#include <iostream>
#ifdef _WIN32
#include <fcntl.h>
#include <io.h>

#include <cstdio>
#else
#include <csignal>
#endif
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "cli/run_cli.h"

int main(int argc, char** argv) {
#ifdef _WIN32
    // Preserve CSV field bytes in pipes and redirected files. CRT text mode
    // otherwise converts CRLF on input and LF on output, including inside
    // quotes.
    const auto binary_if_redirected = [](FILE* stream) {
        const int fd = _fileno(stream);
        if (fd >= 0 && _isatty(fd) == 0) {
            _setmode(fd, _O_BINARY);
        }
    };
    binary_if_redirected(stdin);
    binary_if_redirected(stdout);
#else
    std::signal(SIGPIPE, SIG_IGN);
#endif
    std::vector<std::string> args(argv + 1, argv + argc);
    try {
        return tsfile_cli::run_cli(args, std::cout, std::cerr);
    } catch (const std::exception& e) {
        // Last-resort net (e.g. std::bad_alloc): report instead of aborting.
        std::cerr << "Error: " << e.what() << "\n";
        return tsfile_cli::kExitRuntime;
    }
}
