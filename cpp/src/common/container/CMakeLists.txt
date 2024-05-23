message("Running in src/common/container directory")

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_container_SRC_LIST)
add_library(common_container OBJECT ${common_container_SRC_LIST})
