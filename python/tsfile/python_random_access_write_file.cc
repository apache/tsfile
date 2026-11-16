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

#include "python_random_access_write_file.h"

#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "common/global.h"
#include "file/random_access_write_file.h"
#include "utils/errno_define.h"
#include "writer/tsfile_writer.h"

namespace {

// Every access to the target happens with the GIL held, so the GIL itself
// serializes them; mixing it with a separate mutex (and dropping the GIL while
// waiting for that mutex) is both unnecessary and easy to get wrong.
class PythonGILGuard {
   public:
    PythonGILGuard() : state_(PyGILState_Ensure()) {}
    ~PythonGILGuard() { PyGILState_Release(state_); }

   private:
    PyGILState_STATE state_;
};

// Preserve the underlying Python error text for the caller: the C API can only
// return an errno, but dropping the exception entirely would hide e.g. the
// OSError message behind a generic FileSyncError.
thread_local std::string g_last_python_error;

std::string take_python_error_message() {
    PyObject* type = nullptr;
    PyObject* value = nullptr;
    PyObject* traceback = nullptr;
    PyErr_Fetch(&type, &value, &traceback);
    PyErr_NormalizeException(&type, &value, &traceback);
    std::string message;
    if (value != nullptr) {
        PyObject* text = PyObject_Str(value);
        if (text != nullptr) {
            const char* utf8 = PyUnicode_AsUTF8(text);
            if (utf8 != nullptr) {
                message.assign(utf8);
            }
            Py_DECREF(text);
        }
    }
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(traceback);
    return message;
}

bool has_callable_attribute(PyObject* target, const char* name) {
    PyObject* attribute = PyObject_GetAttrString(target, name);
    if (attribute == nullptr) {
        PyErr_Clear();
        return false;
    }
    const bool callable = PyCallable_Check(attribute) != 0;
    Py_DECREF(attribute);
    return callable;
}

bool seek_to(PyObject* target, int64_t offset, int whence) {
    PyObject* result = PyObject_CallMethod(
        target, "seek", "Li", static_cast<long long>(offset), whence);
    if (result == nullptr) {
        return false;
    }
    Py_DECREF(result);
    return true;
}

bool tell_position(PyObject* target, int64_t& position) {
    PyObject* result =
        PyObject_CallMethod(target, const_cast<char*>("tell"), nullptr);
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

int64_t write_bytes(PyObject* target, const char* buf, Py_ssize_t len) {
    PyObject* result =
        PyObject_CallMethod(target, const_cast<char*>("write"), "y#", buf, len);
    if (result == nullptr) {
        return -1;
    }
    const long long written = PyLong_AsLongLong(result);
    Py_DECREF(result);
    if (written < 0 && PyErr_Occurred()) {
        return -1;
    }
    return static_cast<int64_t>(written);
}

std::string target_name(PyObject* target) {
    std::string name("<python-file>");
    PyObject* value = PyObject_GetAttrString(target, "name");
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

class PythonRandomAccessWriteFile : public storage::RandomAccessWriteFile {
   public:
    PythonRandomAccessWriteFile(PyObject* target, std::string name)
        : target_(target), name_(std::move(name)), position_(0) {
        Py_INCREF(target_);
    }

    ~PythonRandomAccessWriteFile() override {
        // A writer may outlive the interpreter (for example a module-level
        // object); touching the Python API then is undefined. Leaking the
        // reference during shutdown is the only safe option.
        if (Py_IsInitialized()) {
            close();
        }
    }

    bool is_opened() const override {
        PythonGILGuard gil;
        return target_ != nullptr;
    }

    const std::string& file_path() const override { return name_; }

    int write(const char* buf, uint32_t len) override {
        if (len == 0) {
            return common::E_OK;
        }
        if (buf == nullptr) {
            return common::E_INVALID_ARG;
        }
        PythonGILGuard gil;
        g_last_python_error.clear();
        if (target_ == nullptr) {
            return common::E_FILE_WRITE_ERR;
        }
        uint32_t written = 0;
        while (written < len) {
            const int64_t cur = write_bytes(
                target_, buf + written, static_cast<Py_ssize_t>(len - written));
            if (cur <= 0) {
                g_last_python_error = take_python_error_message();
                return common::E_FILE_WRITE_ERR;
            }
            written += static_cast<uint32_t>(cur);
        }
        position_ += written;
        return common::E_OK;
    }

    int sync() override {
        PythonGILGuard gil;
        g_last_python_error.clear();
        if (target_ == nullptr) {
            return common::E_FILE_WRITE_ERR;
        }
        // flush() is optional for a non-durable sink, but if the target has
        // one, a failure means the data may not be committed.
        if (!has_callable_attribute(target_, "flush")) {
            return common::E_OK;
        }
        PyObject* result = PyObject_CallMethod(target_, "flush", nullptr);
        if (result == nullptr) {
            g_last_python_error = take_python_error_message();
            return common::E_FILE_SYNC_ERR;
        }
        Py_DECREF(result);
        return common::E_OK;
    }

    int truncate(int64_t size) override {
        if (size < 0) {
            return common::E_INVALID_ARG;
        }
        PythonGILGuard gil;
        if (target_ == nullptr) {
            return common::E_FILE_WRITE_ERR;
        }
        g_last_python_error.clear();
        PyObject* result = PyObject_CallMethod(target_, "truncate", "L",
                                               static_cast<long long>(size));
        if (result == nullptr) {
            g_last_python_error = take_python_error_message();
            return common::E_NOT_SUPPORT;
        }
        Py_DECREF(result);
        return common::E_OK;
    }

    int seek_to_end() override {
        PythonGILGuard gil;
        if (target_ == nullptr) {
            return common::E_FILE_WRITE_ERR;
        }
        g_last_python_error.clear();
        const int64_t previous_position = position_;
        if (!seek_to(target_, 0, SEEK_END)) {
            g_last_python_error = take_python_error_message();
            return common::E_NOT_SUPPORT;
        }
        int64_t end_position = 0;
        if (!tell_position(target_, end_position)) {
            g_last_python_error = take_python_error_message();
            // The cursor already moved; put it back so a caller that falls
            // back to appending does not land at the wrong offset.
            if (seek_to(target_, previous_position, SEEK_SET)) {
                PyErr_Clear();
            }
            return common::E_NOT_SUPPORT;
        }
        position_ = end_position;
        return common::E_OK;
    }

    // The writer asks for the position after every flush to record chunk
    // offsets; tell() may fail on streams that only support it while idle, so
    // track the bytes written instead of asking the target each time.
    int64_t get_position() override {
        PythonGILGuard gil;
        return position_;
    }

    int close() override {
        PythonGILGuard gil;
        if (target_ == nullptr) {
            return common::E_OK;
        }
        Py_DECREF(target_);
        target_ = nullptr;
        return common::E_OK;
    }

   private:
    PyObject* target_;
    std::string name_;
    int64_t position_;
};

}  // namespace

const char* python_random_access_write_file_last_error() {
    return g_last_python_error.empty() ? nullptr : g_last_python_error.c_str();
}

void* create_tsfile_writer_from_python_file(PyObject* target,
                                            uint64_t memory_threshold,
                                            int32_t* error_code) {
    if (error_code == nullptr) {
        PyErr_SetString(PyExc_ValueError, "error_code must not be null");
        return nullptr;
    }
    *error_code = common::E_OK;
    if (target == nullptr || !has_callable_attribute(target, "write") ||
        !has_callable_attribute(target, "tell")) {
        *error_code = common::E_INVALID_ARG;
        PyErr_SetString(PyExc_TypeError,
                        "target must be a writable binary file object");
        return nullptr;
    }

    *error_code = storage::libtsfile_init();
    if (*error_code != common::E_OK) {
        return nullptr;
    }

    // A text-mode stream would reject the first bytes with a TypeError; fail
    // early with a message that says what is actually wrong.
    PyObject* mode = PyObject_GetAttrString(target, "mode");
    if (mode == nullptr) {
        PyErr_Clear();
    } else {
        if (PyUnicode_Check(mode)) {
            const char* text = PyUnicode_AsUTF8(mode);
            if (text != nullptr && std::strchr(text, 'b') == nullptr) {
                Py_DECREF(mode);
                *error_code = common::E_INVALID_ARG;
                PyErr_SetString(PyExc_ValueError,
                                "target must be opened in binary mode");
                return nullptr;
            }
            if (text == nullptr) {
                PyErr_Clear();
            }
        }
        Py_DECREF(mode);
    }

    // The file must start empty: a header written at a nonzero cursor is not
    // a standalone TsFile, and bytes left over from a longer previous file
    // would trail the new one.
    int64_t initial_position = 0;
    if (!tell_position(target, initial_position)) {
        PyErr_Clear();
        *error_code = common::E_INVALID_ARG;
        PyErr_SetString(PyExc_TypeError, "target must support tell()");
        return nullptr;
    }
    // Rewind when the target allows it.  A forward-only stream inherits
    // callable seek()/truncate() from io.RawIOBase that raise
    // io.UnsupportedOperation, so treating a callable method as support would
    // reject freshly opened streams; only the position check matters then.
    bool rewound = false;
    if (has_callable_attribute(target, "seek")) {
        if (seek_to(target, 0, SEEK_SET)) {
            rewound = true;
        } else {
            PyErr_Clear();
        }
    }
    if (!rewound && initial_position != 0) {
        *error_code = common::E_INVALID_ARG;
        PyErr_SetString(PyExc_ValueError,
                        "target must be positioned at 0 before writing");
        return nullptr;
    }
    if (rewound && has_callable_attribute(target, "truncate")) {
        PyObject* truncated = PyObject_CallMethod(target, "truncate", "i", 0);
        if (truncated == nullptr) {
            PyErr_Clear();
            // Truncation is unsupported: tolerate it only if the target is
            // provably empty, otherwise a shorter write would leave stale
            // trailing bytes behind a valid-looking file.
            int64_t end_position = 0;
            const bool sized = seek_to(target, 0, SEEK_END) &&
                               tell_position(target, end_position);
            if (!sized) {
                PyErr_Clear();
            }
            if (!sized || end_position != 0 || !seek_to(target, 0, SEEK_SET)) {
                PyErr_Clear();
                *error_code = common::E_INVALID_ARG;
                PyErr_SetString(PyExc_ValueError,
                                "target has existing content and cannot be "
                                "truncated");
                return nullptr;
            }
        } else {
            Py_DECREF(truncated);
        }
    }

    try {
        common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
        std::unique_ptr<storage::RandomAccessWriteFile> write_file(
            new PythonRandomAccessWriteFile(target, target_name(target)));
        std::unique_ptr<storage::TsFileWriter> writer(
            new storage::TsFileWriter());
        *error_code = writer->init(std::move(write_file));
        if (*error_code != common::E_OK) {
            return nullptr;
        }
        return writer.release();
    } catch (const std::bad_alloc&) {
        *error_code = common::E_OOM;
    } catch (...) {
        *error_code = common::E_FILE_OPEN_ERR;
    }
    return nullptr;
}
