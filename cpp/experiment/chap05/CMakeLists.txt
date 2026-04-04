cmake_minimum_required(VERSION 3.10)
message("Running in experiment/chap05 directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

# Propagate SIMD flag so that headers included by experiments see ENABLE_SIMD
if (ENABLE_SIMD)
    add_definitions(-DENABLE_SIMD)
    include_directories(${CMAKE_SOURCE_DIR}/third_party/simde-0.8.4-rc3)
endif()

# E5-1: Codec throughput (encode/decode) SIMD ON/OFF
add_executable(codec_bench E5_1_codec/codec_bench.cpp)
target_link_libraries(codec_bench tsfile)

# E5-2: Time filter + late materialization
add_executable(filter_bench E5_2_filter_latmat/filter_bench.cpp)
target_link_libraries(filter_bench tsfile)

# E5-4: Block-level time filter precision (Plan A vs Plan C)
add_executable(block_filter_bench E5_4_block_filter/block_filter_bench.cpp)
target_link_libraries(block_filter_bench tsfile)
