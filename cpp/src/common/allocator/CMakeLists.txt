
message("Running in src/common/allocator directory")

message("CMAKE_CURRENT_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_allocator_SRC_LIST)
add_library(common_allocator OBJECT ${common_allocator_SRC_LIST})

