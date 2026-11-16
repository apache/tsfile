/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef PYTHON_TSFILE_PYTHON_RANDOM_ACCESS_WRITE_FILE_H
#define PYTHON_TSFILE_PYTHON_RANDOM_ACCESS_WRITE_FILE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>

void* create_tsfile_writer_from_python_file(PyObject* target,
                                            uint64_t memory_threshold,
                                            int32_t* error_code);

/**
 * Returns the text of the last sink failure (for example the OSError raised by
 * a target's flush()), or NULL when the last operation set no Python error.
 * The pointer stays valid until the next sink operation on this thread.
 */
const char* python_random_access_write_file_last_error(void);

#endif  // PYTHON_TSFILE_PYTHON_RANDOM_ACCESS_WRITE_FILE_H
