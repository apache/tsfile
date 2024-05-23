message("Running in src/common/tsblock directory")

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_tsblock_SRC_LIST)
add_library(common_tsblock OBJECT ${common_tsblock_SRC_LIST})