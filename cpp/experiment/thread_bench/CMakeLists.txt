cmake_minimum_required(VERSION 3.10)
project(thread_bench)
message("Running in experiment/thread_bench directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

add_executable(thread_bench thread_bench.cpp)
target_link_libraries(thread_bench tsfile)

add_executable(read_thread_bench read_thread_bench.cpp)
target_link_libraries(read_thread_bench tsfile)

add_executable(granularity_bench granularity_bench.cpp)
target_link_libraries(granularity_bench tsfile)
