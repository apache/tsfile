cmake_minimum_required(VERSION 3.10)
project(write_memory)
message("Running in experiment/write_memory directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

add_executable(write_memory write_memory.cpp)
target_link_libraries(write_memory tsfile)
