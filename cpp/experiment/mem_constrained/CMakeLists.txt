cmake_minimum_required(VERSION 3.10)
project(mem_constrained)
message("Running in experiment/mem_constrained directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

# Writer experiment: MemConstrainedWriter wrapping TsFileWriter.
# Does NOT require ENABLE_MEM_STAT; uses direct API measurements.
add_executable(writer_bench writer_bench.cpp)
target_link_libraries(writer_bench tsfile)

# Reader experiment: MemConstrainedReader auto-derives batch_size.
# Build with ENABLE_MEM_STAT=ON to record actual peak memory via ModStat.
add_executable(reader_bench reader_bench.cpp)
target_link_libraries(reader_bench tsfile)
