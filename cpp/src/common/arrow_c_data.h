/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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
#ifndef COMMON_ARROW_C_DATA_H
#define COMMON_ARROW_C_DATA_H

#include <stdint.h>

typedef struct arrow_schema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct arrow_schema** children;
    struct arrow_schema* dictionary;

    // Release callback
    void (*release)(struct arrow_schema*);
    // Opaque producer-specific data
    void* private_data;
} ArrowSchema;

typedef struct arrow_array {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct arrow_array** children;
    struct arrow_array* dictionary;

    // Release callback
    void (*release)(struct arrow_array*);
    // Opaque producer-specific data
    void* private_data;
} ArrowArray;

#endif
