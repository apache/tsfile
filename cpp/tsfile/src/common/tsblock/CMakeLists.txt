message("Running in src/common/tsblock directory")

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_tsblock_SRC_LIST)
set(common_tsblock_INC_LIST)
set(common_tsblock_DEFINE_OPTIONS)
set(common_tsblock_COMPILE_OPTIONS)
set(common_tsblock_LINK_OPTIONS)
add_statis_objtarget(common_tsblock common_tsblock_SRC_LIST common_tsblock_INC_LIST
                     "${common_tsblock_DEFINE_OPTIONS}"
                     "${common_tsblock_COMPILE_OPTIONS}"
                     "${common_tsblock_LINK_OPTIONS}")
