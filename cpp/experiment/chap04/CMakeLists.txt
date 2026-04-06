cmake_minimum_required(VERSION 3.10)
message("Running in experiment/chap04 directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

if (ENABLE_SIMD)
    add_definitions(-DENABLE_SIMD)
    include_directories(${CMAKE_SOURCE_DIR}/third_party/simde-0.8.4-rc3)
endif()

if (ENABLE_THREADS)
    add_definitions(-DENABLE_THREADS)
endif()

# E4-1/E4-2: Write & Read parallel throughput benchmark
# Build with:
#   -DENABLE_THREADS=OFF  -> serial baseline (C1)
#   -DENABLE_THREADS=ON   -> column-parallel (C4)
#   -DENABLE_SIMD=ON      -> with SIMD (C3/C5)
add_executable(throughput_bench E4_12_throughput/throughput_bench.cpp)
target_link_libraries(throughput_bench tsfile)
