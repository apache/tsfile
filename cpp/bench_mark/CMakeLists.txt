message("Running in bench_mark directory")
if(DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
endif()

set(CMAKE_CXX_FLAGS "$ENV{CXX_FLAGS} -Wall -Werror")

if (${USE_CPP11})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  set(CMAKE_CXX_STANDARD 11)
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++03")
endif()

add_subdirectory(bench_mark_src)