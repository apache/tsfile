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

#ifndef PYTHON_TSFILE_PYTHON_RANDOM_ACCESS_FILE_H
#define PYTHON_TSFILE_PYTHON_RANDOM_ACCESS_FILE_H

#include <Python.h>
#include <stdint.h>

void* create_tsfile_reader_from_python_file(PyObject* source,
                                            int32_t* error_code);

#endif  // PYTHON_TSFILE_PYTHON_RANDOM_ACCESS_FILE_H
