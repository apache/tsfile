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
#ifndef _WIN32
#include <csignal>
#endif
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "cli/run_cli.h"

int main(int argc, char** argv) {
#ifndef _WIN32
    // comment : 这里是注册了一个信号来处理什么呢？
    // Answer: 这里不是注册自定义处理函数，而是忽略 SIGPIPE。Unix 下当输出被
    // 管道连接到 `head` 等程序、下游提前关闭管道时，默认 SIGPIPE 会直接终止
    // 进程；忽略后 write/iostream 会以 EPIPE/失败状态返回，使 run_cli 能走统一的
    // 输出错误处理和退出码逻辑，而不是被信号异步杀死。Windows 没有该信号语义。
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
