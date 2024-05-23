cmake_minimum_required(VERSION 3.1)
project(libtsfile_bench_mark_project)
message("Running in bench_mark/bench_mark_src directory")
if(DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
endif()

set(SDK_BENCH_MARK_DIR ${PROJECT_SOURCE_DIR}/)
message("PROJECT DIR: ${SDK_BENCH_MARK_DIR}")
set(SDK_INCLUDE_DIR_DEBUG ${SKD_BENCHH_MARK_DIR}../../build/Debug/bin/libtsfile_sdk/include)
set(SDK_INCLUDE_DIR_RELEASE ${SKD_BENCHH_MARK_DIR}../../build/Release/bin/libtsfile_sdk/include)
set(SDK_LIB_DIR_DEBUG ${SKD_BENCHH_MARK_DIR}../../build/Debug/bin/libtsfile_sdk/lib)
set(SDK_LIB_DIR_RELEASE ${SKD_BENCHH_MARK_DIR}../../build/Release/bin/libtsfile_sdk/lib)

if (USE_SDK_DEBUG) 
    SET(SKD_INCLUDE_DIR ${SDK_INCLUDE_DIR_DEBUG})
    SET(SDK_LIB_DIR ${SDK_LIB_DIR_DEBUG})
    SET(CMAKE_CXX_FLAGS "-g -O0")
else()
    SET(SKD_INCLUDE_DIR ${SDK_INCLUDE_DIR_RELEASE})
    SET(SDK_LIB_DIR ${SDK_LIB_DIR_RELEASE})
    SET(CMAKE_CXX_FLAGS "-O3")
endif()

include_directories(${SKD_INCLUDE_DIR})
set(MAKE_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/../../src)
include_directories(${MAKE_INCLUDE})
message("MAKE_INCLUDE: ${MAKE_INCLUDE}")
message("SDK_INCLUDE_DIR: ${SKD_INCLUDE_DIR}")
message("SDK_LIB_DIR: ${SDK_LIB_DIR}")
link_directories(${SDK_LIB_DIR})
find_library(my_tsfile_lib NAMES tsfile PATHS ${SDK_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
add_executable(bench_mark_src bench_mark.cc)
target_link_libraries(bench_mark_src ${my_tsfile_lib})




