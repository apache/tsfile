message("Running in src/common directory")

add_subdirectory(allocator)
add_subdirectory(config)
add_subdirectory(container)
add_subdirectory(mutex)
add_subdirectory(tsblock)
add_subdirectory(datatype)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_SRC_LIST)
set(common_INC_LIST )
set(common_DEFINE_OPTIONS )
set(common_COMPILE_OPTIONS )
set(common_LINK_OPTIONS )

add_statis_objtarget(common common_SRC_LIST common_INC_LIST
                     "${common_DEFINE_OPTIONS}"
                     "${common_COMPILE_OPTIONS}"
                     "${common_LINK_OPTIONS}")

