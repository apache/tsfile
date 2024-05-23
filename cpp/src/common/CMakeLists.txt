message("Running in src/common directory")

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/allocator common_allocator_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/container common_container_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/tsblock common_tsblock_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/mutex common_mutex_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/datatype common_datatype_SRC_LIST)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_library(common_obj OBJECT ${common_SRC_LIST} 
    ${common_allocator_SRC_LIST}
    ${common_container_SRC_LIST}
    ${common_tsblock_SRC_LIST} 
    ${common_mutex_SRC_LIST} 
    ${common_datatype_SRC_LIST})

