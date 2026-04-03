cmake_minimum_required(VERSION 3.10)
project(read_memory)
message("Running in experiment/read_memory directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

add_executable(read_mem_model read_mem_model.cpp)
target_link_libraries(read_mem_model tsfile)
