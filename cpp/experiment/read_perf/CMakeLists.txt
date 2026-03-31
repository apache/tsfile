cmake_minimum_required(VERSION 3.10)
project(read_perf)
message("Running in experiment/read_perf directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

# Arrow + Parquet
if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH
        "/opt/homebrew/opt/apache-arrow/lib/cmake"
        "/usr/local/opt/apache-arrow/lib/cmake")
endif()
find_package(Arrow  CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

add_executable(read_benchmark read_benchmark.cpp)
target_link_libraries(read_benchmark tsfile Arrow::arrow_shared Parquet::parquet_shared)
