cmake_minimum_required(VERSION 3.10)
message("Running in experiment/chap06 directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

if (ENABLE_SIMD)
    add_definitions(-DENABLE_SIMD)
    include_directories(${CMAKE_SOURCE_DIR}/third_party/simde-0.8.4-rc3)
endif()

# Arrow + Parquet
if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH
        "/opt/homebrew/opt/apache-arrow/lib/cmake"
        "/usr/local/opt/apache-arrow/lib/cmake")
endif()
find_package(Arrow  CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

# E6-1: TsFile vs Parquet on real datasets
add_executable(dataset_bench E6_1_vs_parquet/dataset_bench.cpp)
target_link_libraries(dataset_bench tsfile Arrow::arrow_shared Parquet::parquet_shared)
