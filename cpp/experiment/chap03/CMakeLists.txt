cmake_minimum_required(VERSION 3.10)
message("Running in experiment/chap03 directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SDK_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/)
include_directories(${SDK_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)

# E3-2: Write/Read precision (PLAIN + UNCOMPRESSED)
add_executable(write_precision E3_2_write_precision/write_precision.cpp)
target_link_libraries(write_precision tsfile)

add_executable(read_precision E3_2_write_precision/read_precision.cpp)
target_link_libraries(read_precision tsfile)

add_executable(debug_estimate E3_2_write_precision/debug_estimate.cpp)
target_link_libraries(debug_estimate tsfile)

# E3-4: EOQ optimal strategy validation
add_executable(eoq_validate E3_4_write_budget/eoq_validate.cpp)
target_link_libraries(eoq_validate tsfile)

# E3-4: Write/Read with memory budget (PLAIN + UNCOMPRESSED)
add_executable(write_budget E3_4_write_budget/write_budget.cpp)
target_link_libraries(write_budget tsfile)

add_executable(read_budget E3_4_write_budget/read_budget.cpp)
target_link_libraries(read_budget tsfile)
