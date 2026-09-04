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

#include "python_random_access_file.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <utility>

#include "file/random_access_file.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"

namespace {

class PythonSourceLock {
   public:
    explicit PythonSourceLock(std::mutex& mutex)
        : gil_state_(PyGILState_Ensure()), lock_(mutex, std::defer_lock) {
        PyThreadState* thread_state = PyEval_SaveThread();
        lock_.lock();
        PyEval_RestoreThread(thread_state);
    }

    ~PythonSourceLock() {
        lock_.unlock();
        PyGILState_Release(gil_state_);
    }

   private:
    PyGILState_STATE gil_state_;
    std::unique_lock<std::mutex> lock_;
};

bool has_callable_attribute(PyObject* source, const char* name) {
    PyObject* attribute = PyObject_GetAttrString(source, name);
    if (attribute == nullptr) {
        PyErr_Clear();
        return false;
    }
    const bool callable = PyCallable_Check(attribute) != 0;
    Py_DECREF(attribute);
    return callable;
}

bool seek_to(PyObject* source, int64_t offset, int whence) {
    PyObject* result = PyObject_CallMethod(
        source, "seek", "Li", static_cast<long long>(offset), whence);
    if (result == nullptr) {
        return false;
    }
    Py_DECREF(result);
    return true;
}

bool tell_position(PyObject* source, int64_t& position) {
    PyObject* result =
        PyObject_CallMethod(source, const_cast<char*>("tell"), nullptr);
    if (result == nullptr) {
        return false;
    }
    const long long value = PyLong_AsLongLong(result);
    Py_DECREF(result);
    if (value == -1 && PyErr_Occurred()) {
        return false;
    }
    position = static_cast<int64_t>(value);
    return true;
}

void restore_position_preserving_error(PyObject* source, int64_t position) {
    PyObject* error_type = nullptr;
    PyObject* error_value = nullptr;
    PyObject* traceback = nullptr;
    PyErr_Fetch(&error_type, &error_value, &traceback);
    if (!seek_to(source, position, 0)) {
        PyErr_Clear();
    }
    PyErr_Restore(error_type, error_value, traceback);
}

std::string source_name(PyObject* source) {
    std::string name("<python-file>");
    PyObject* value = PyObject_GetAttrString(source, "name");
    if (value == nullptr) {
        PyErr_Clear();
        return name;
    }
    if (PyUnicode_Check(value)) {
        const char* text = PyUnicode_AsUTF8(value);
        if (text != nullptr) {
            name.assign(text);
        } else {
            PyErr_Clear();
        }
    } else if (PyBytes_Check(value)) {
        char* text = nullptr;
        Py_ssize_t length = 0;
        if (PyBytes_AsStringAndSize(value, &text, &length) == 0) {
            name.assign(text, static_cast<size_t>(length));
        } else {
            PyErr_Clear();
        }
    }
    Py_DECREF(value);
    return name;
}

class PythonRandomAccessFile : public storage::RandomAccessFile {
   public:
    PythonRandomAccessFile(PyObject* source, int64_t size, std::string name)
        : source_(source), size_(size), name_(std::move(name)) {
        Py_INCREF(source_);
    }

    ~PythonRandomAccessFile() override { close(); }

    bool is_opened() const override {
        PythonSourceLock lock(mutex_);
        return source_ != nullptr;
    }

    int64_t file_size() const override { return size_; }

    const std::string& file_path() const override { return name_; }

    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        PythonSourceLock lock(mutex_);
        if (source_ == nullptr) {
            return common::E_FILE_READ_ERR;
        }
        size = static_cast<uint64_t>(size_);
        fingerprint = 0;
        return common::E_OK;
    }

    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override {
        read_size = 0;
        if (offset < 0 || size < 0 || (buffer == nullptr && size > 0)) {
            return common::E_INVALID_ARG;
        }

        PythonSourceLock lock(mutex_);
        if (source_ == nullptr) {
            return common::E_FILE_READ_ERR;
        }
        if (size == 0 || offset >= size_) {
            return common::E_OK;
        }

        const int64_t available = size_ - offset;
        const int32_t requested = static_cast<int32_t>(
            std::min<int64_t>(available, static_cast<int64_t>(size)));

        int ret = common::E_OK;
        int64_t saved_position = 0;
        const bool has_saved_position = tell_position(source_, saved_position);
        if (!has_saved_position || !seek_to(source_, offset, 0)) {
            PyErr_Clear();
            ret = common::E_FILE_READ_ERR;
        }

        while (ret == common::E_OK && read_size < requested) {
            const int32_t remaining = requested - read_size;
            PyObject* chunk =
                PyObject_CallMethod(source_, "read", "i", remaining);
            if (chunk == nullptr) {
                PyErr_Clear();
                ret = common::E_FILE_READ_ERR;
                break;
            }

            Py_buffer view;
            if (PyObject_GetBuffer(chunk, &view, PyBUF_CONTIG_RO) != 0) {
                PyErr_Clear();
                Py_DECREF(chunk);
                ret = common::E_FILE_READ_ERR;
                break;
            }
            if (view.len < 0 || view.len > remaining) {
                ret = common::E_FILE_READ_ERR;
            } else if (view.len == 0) {
                PyBuffer_Release(&view);
                Py_DECREF(chunk);
                break;
            } else {
                std::memcpy(buffer + read_size, view.buf,
                            static_cast<size_t>(view.len));
                read_size += static_cast<int32_t>(view.len);
            }
            PyBuffer_Release(&view);
            Py_DECREF(chunk);
        }

        if (has_saved_position && !seek_to(source_, saved_position, 0)) {
            PyErr_Clear();
            ret = common::E_FILE_READ_ERR;
        }
        return ret;
    }

    void close() override {
        PythonSourceLock lock(mutex_);
        if (source_ == nullptr) {
            return;
        }
        Py_DECREF(source_);
        source_ = nullptr;
    }

   private:
    PyObject* source_;
    int64_t size_;
    std::string name_;
    mutable std::mutex mutex_;
};

}  // namespace

void* create_tsfile_reader_from_python_file(PyObject* source,
                                            int32_t* error_code) {
    if (error_code == nullptr) {
        PyErr_SetString(PyExc_ValueError, "error_code must not be null");
        return nullptr;
    }
    *error_code = common::E_OK;
    if (source == nullptr || !has_callable_attribute(source, "seek") ||
        !has_callable_attribute(source, "tell") ||
        !has_callable_attribute(source, "read")) {
        *error_code = common::E_INVALID_ARG;
        PyErr_SetString(PyExc_TypeError,
                        "source must be a seekable binary file object");
        return nullptr;
    }

    int64_t original_position = 0;
    if (!tell_position(source, original_position)) {
        *error_code = common::E_FILE_OPEN_ERR;
        return nullptr;
    }
    if (!seek_to(source, 0, 2)) {
        *error_code = common::E_FILE_OPEN_ERR;
        restore_position_preserving_error(source, original_position);
        return nullptr;
    }

    int64_t size = 0;
    if (!tell_position(source, size)) {
        *error_code = common::E_FILE_OPEN_ERR;
        restore_position_preserving_error(source, original_position);
        return nullptr;
    }
    if (!seek_to(source, original_position, 0)) {
        *error_code = common::E_FILE_OPEN_ERR;
        return nullptr;
    }
    if (size < 0) {
        *error_code = common::E_INVALID_ARG;
        PyErr_SetString(PyExc_ValueError,
                        "source returned a negative file size");
        return nullptr;
    }

    try {
        *error_code = storage::libtsfile_init();
        if (*error_code != common::E_OK) {
            return nullptr;
        }
        std::unique_ptr<storage::RandomAccessFile> random_access_file(
            new PythonRandomAccessFile(source, size, source_name(source)));
        std::unique_ptr<storage::TsFileReader> reader(
            new storage::TsFileReader());
        *error_code = reader->open(std::move(random_access_file));
        if (*error_code != common::E_OK) {
            return nullptr;
        }
        return reader.release();
    } catch (const std::bad_alloc&) {
        *error_code = common::E_OOM;
    } catch (...) {
        *error_code = common::E_FILE_OPEN_ERR;
    }
    return nullptr;
}
