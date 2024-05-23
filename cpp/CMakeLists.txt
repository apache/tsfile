cmake_minimum_required(VERSION 3.11)
project(TsFile_CPP)

set(TsFile_CPP_VERSION_MAJOR 0)
set(TsFile_CPP_VERSION_MINOR 1)

set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall -Werror")
message("cmake using: USE_CPP11=${USE_CPP11}")

if (${USE_CPP11})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++03")
endif()

if(DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
    message("cmake using: CXX=${CMAKE_CXX_COMPILER}")
endif()

if(DEFINED ENV{CC}) 
  set(CMAKE_C_COMPILER $ENV{CC})
  message("cmake using: CC=${CMAKE_C_COMPILER}")
endif()

message("cmake using: DEBUG_SE=${DEBUG_SE}")
if (${DEBUG_SE})
  add_definitions(-DDEBUG_SE=1)
  message("add_definitions -DDEBUG_SE=1")
endif()

message("build type: ${BUILD_TYPE}")
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
  set(CMAKE_CXX_FLAGS_DEBUG" ${CMAKE_CXX_FLAGS} -O0 -g")
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g") # disable -g
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS}")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS}")
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffunction-sections -fdata-sections -Os")
  set(LDFLAGS "-Wl,--gc-sections")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS}")
endif()
message("CMAKE DEBUG: CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)
set(PROJECT_INCLUDE_DIR ${PROJECT_INCLUDE_DIR} ${PROJECT_SOURCE_DIR}/src)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
include_directories(${PROJECT_INCLUDE_DIR})

add_subdirectory(src)

configure_file(
  "${PROJECT_SOURCE_DIR}/cmake/config.h.in"
  "${PROJECT_BINARY_DIR}/cmake_config.h"
)
