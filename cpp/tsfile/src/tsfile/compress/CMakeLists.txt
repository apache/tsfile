
message("Running in src/storage/tsfile/compress directory")

aux_source_directory(. storage_tsfile_compress_SRC_LIST)
set(storage_tsfile_compress_INC_LIST )
set(storage_tsfile_compress_DEFINE_OPTIONS )
set(storage_tsfile_compress_COMPILE_OPTIONS )
set(storage_tsfile_compress_LINK_OPTIONS )

add_statis_objtarget(storage_tsfile_compress storage_tsfile_compress_SRC_LIST
                                             storage_tsfile_compress_INC_LIST
                                          "${storage_tsfile_compress_DEFINE_OPTIONS}"
                                          "${storage_tsfile_compress_COMPILE_OPTIONS}"
                                          "${storage_tsfile_compress_LINK_OPTIONS}")
