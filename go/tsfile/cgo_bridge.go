// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tsfile

/*
#cgo CFLAGS: -I${SRCDIR}/../../cpp/target/build/include
#cgo !windows LDFLAGS: -L${SRCDIR}/../../cpp/target/build/lib -ltsfile -Wl,-rpath,${SRCDIR}/../../cpp/target/build/lib
#cgo windows LDFLAGS: -L${SRCDIR}/../../cpp/target/build/lib -ltsfile
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
*/
import "C"

// This file is the only place in the package that imports "C". All cgo
// traffic goes through the unexported helpers and handle types defined here
// so that C allocation and release sites stay easy to audit. Go pointers are
// never retained by C: values crossing the boundary are copied into
// temporary C allocations (C.CString / C.malloc) that are freed before the
// helper returns, and C-allocated results are copied back into Go memory
// before the matching free call.
//
// Handle constructors return plain Go values (unsafe.Pointer, int32) rather
// than _Ctype aliases so that the narrow API surface of the bridge is usable
// from the package's other files and _test files, which live in separate
// cgo translation units and cannot reference C types directly.

import (
	"fmt"
	"math"
	"strings"
	"unsafe"
)

// These errno values are plain-Go mirrors of the C RET_* constants. Keeping
// the C names in this bridge lets the rest of the package and its tests use
// meaningful Go names without importing "C" or duplicating ABI numbers.
const (
	errnoOK         = C.RET_OK
	errnoNoMoreData = C.RET_NO_MORE_DATA
	errnoFileClose  = C.RET_FILE_CLOSE_ERR
)

var errNoMoreData = &Error{Code: errnoNoMoreData}

// cerrno is the Go-side mirror of the C ERRNO type (int32_t). The bridge
// converts between it and C.ERRNO at the boundary.
type cerrno = int32

// nativeHandle owns exactly one opaque C handle. release is the single
// release site for that C memory: it frees the handle and returns a C ERRNO
// for mapping through newError.
type nativeHandle struct {
	ptr     unsafe.Pointer
	release func(unsafe.Pointer) cerrno
}

// close releases the C handle through release. It clears the stored pointer
// only after release reports success (RET_OK): the C ABI does not delete the
// writer on a failed close (the generic writer returns early without delete
// when flush or close fails), so on a release error the wrapper keeps
// ownership and a later retry can release it. On success the handle is
// irrevocably gone, making a second close an idempotent no-op.
func (h *nativeHandle) close() error {
	if h.ptr == nil {
		return nil
	}
	code := h.release(h.ptr)
	if code == C.RET_OK {
		h.ptr = nil
		return nil
	}
	return newError("close", code)
}

// ---------------------------------------------------------------------------
// String and byte marshaling (Go -> C copies; Go pointers never retained)
// ---------------------------------------------------------------------------

// cStringPtr returns a freshly allocated NUL-terminated C copy of value. The
// caller owns the allocation and must release it with freeCString after use.
// Every value maps to a non-NULL allocation; callers must not pass empty or
// NUL-containing values without running validateCString first, because the
// native constructors do not accept NULL and C.CString would truncate at an
// embedded NUL.
func cStringPtr(value string) *C.char {
	return C.CString(value)
}

// validateCString rejects values that cannot cross the C string boundary
// safely: an empty value maps to NULL in the C ABI (the native constructors
// dereference the argument and would crash on NULL), and an embedded NUL
// would be silently truncated by C.CString, handing C a different name than
// Go asked for. It returns ErrInvalidArgument without touching C memory.
func validateCString(op, field, value string) error {
	if value == "" || strings.IndexByte(value, 0) >= 0 {
		err := newError(op, C.RET_INVALID_ARG)
		return fmt.Errorf("%w: %s %q must be a non-empty string without embedded NUL", err, field, value)
	}
	return nil
}

// validateCStrings applies validateCString to every element of values,
// returning the index of the first invalid element so the caller can name it.
func validateCStrings(op, field string, values []string) (int, error) {
	for i, v := range values {
		if v == "" || strings.IndexByte(v, 0) >= 0 {
			err := newError(op, C.RET_INVALID_ARG)
			return i, fmt.Errorf("%w: %s[%d] %q must be a non-empty string without embedded NUL", err, field, i, v)
		}
	}
	return 0, nil
}

// freeCString releases one C.CString allocation; nil is a no-op.
func freeCString(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}

// cgoString copies the NUL-terminated C string at p into Go memory, or ""
// for a nil pointer. It is the read-back side of cStringPtr.
func cgoString(p unsafe.Pointer) string {
	if p == nil {
		return ""
	}
	return C.GoString((*C.char)(p))
}

// marshalCStrings copies values into a C-allocated array of C strings. The
// returned Go slice is only a view over that C allocation. Keeping both the
// pointer array and its elements in C memory is required by the cgo pointer
// rules when the array is passed to a char** parameter.
func marshalCStrings(values []string) ([]*C.char, error) {
	if len(values) == 0 {
		return nil, nil
	}
	mem := C.malloc(C.size_t(len(values)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	if mem == nil {
		return nil, newError("marshal strings", C.RET_OOM)
	}
	out := unsafe.Slice((**C.char)(mem), len(values))
	for i, v := range values {
		cs := C.CString(v)
		if cs == nil {
			for _, p := range out[:i] {
				C.free(unsafe.Pointer(p))
			}
			C.free(mem)
			return nil, newError("marshal strings", C.RET_OOM)
		}
		out[i] = cs
	}
	return out, nil
}

// freeCStrings releases every element of a slice produced by
// marshalCStrings.
func freeCStrings(ptrs []*C.char) {
	for _, p := range ptrs {
		if p != nil {
			C.free(unsafe.Pointer(p))
		}
	}
	if len(ptrs) > 0 {
		C.free(unsafe.Pointer(&ptrs[0]))
	}
}

// cBytesLen is the Go-side mirror of the byte lengths the C ABI takes
// (uint32_t length parameters). Values crossing the boundary are capped at
// cBytesLenMax by validateCBytesLen, so a cBytesLen produced by the bridge
// always fits the C ABI's effective length domain.
type cBytesLen uint64

// cBytesLenMax is the largest byte length the bridge accepts for a single C
// byte buffer. The bound is INT32_MAX, not MaxUint32, because both ends of
// the byte boundary agree on it: the native property API
// (tsfile_generic_writer_add_tsfile_property) takes uint32_t lengths but
// rejects any length above INT32_MAX with E_OUT_OF_RANGE, and the 32-bit
// signed C.int used by C.GoBytes cannot express a larger count. Admitting
// MaxUint32 would let values through that C.int(n) in cgoBytes silently
// truncates (MaxUint32 wraps to -1; MaxInt32+5 wraps to 5), handing C or
// Go a different length than requested.
const cBytesLenMax = math.MaxInt32

// validateCBytesLen is a pure integer-length validator for the C ABI's byte
// length parameters: it reports whether a length of n bytes fits the
// ABI's effective domain [0, cBytesLenMax]. It performs no allocation and
// needs no backing buffer, so the boundary can be tested with bare integers
// like cBytesLenMax+1.
func validateCBytesLen(op string, n int) error {
	if n < 0 || n > cBytesLenMax {
		err := newError(op, C.RET_OVERFLOW)
		return fmt.Errorf("%w: byte length %d exceeds max C length %d", err, n, int64(cBytesLenMax))
	}
	return nil
}

// copyCBytes copies b into a temporary C allocation (C.malloc + memcpy),
// returning the C pointer and the length in bytes (an unsigned cBytesLen).
// Go slice memory is never exposed to C and no Go pointer ever crosses the
// boundary. Empty input returns (nil, 0) with no allocation, which the C ABI
// accepts as an empty buffer. The returned pointer is valid only until the
// caller passes it to freeCBytes.
func copyCBytes(op string, b []byte) (unsafe.Pointer, cBytesLen, error) {
	if len(b) == 0 {
		return nil, 0, nil
	}
	if err := validateCBytesLen(op, len(b)); err != nil {
		return nil, 0, err
	}
	p := C.malloc(C.size_t(len(b)))
	if p == nil {
		err := newError(op, C.RET_OOM)
		return nil, 0, fmt.Errorf("%w: allocating %d bytes", err, len(b))
	}
	C.memcpy(p, unsafe.Pointer(&b[0]), C.size_t(len(b)))
	return p, cBytesLen(len(b)), nil
}

// freeCBytes releases one copyCBytes allocation; nil is a no-op. It must be
// called exactly once per non-nil pointer returned by copyCBytes so the C
// allocation is freed immediately after the crossing C call returns.
func freeCBytes(p unsafe.Pointer) {
	if p != nil {
		C.free(p)
	}
}

// cgoBytes copies the n bytes at the C buffer p into a fresh Go slice,
// returning nil for a nil pointer. It is the read-back side of copyCBytes
// and must be called before freeCBytes. n must be non-negative (a negative
// length is a caller bug and panics). n may be any value up to
// math.MaxUint32: lengths above cBytesLenMax cannot be expressed in the
// 32-bit signed C.int argument of C.GoBytes, so they are copied in
// cBytesLenMax-sized chunks with 64-bit C.size_t memcpy counts.
func cgoBytes(p unsafe.Pointer, n int) []byte {
	if p == nil || n == 0 {
		return nil
	}
	if n < 0 {
		panic("tsfile: cgoBytes with negative length")
	}
	if n <= cBytesLenMax {
		return C.GoBytes(p, C.int(n))
	}
	out := make([]byte, n)
	for off := 0; off < n; {
		m := nextCgoBytesChunk(off, n)
		C.memcpy(unsafe.Pointer(&out[off]), unsafe.Add(p, uintptr(off)), C.size_t(m))
		off += m
	}
	return out
}

// nextCgoBytesChunk is the pure allocation-free step function of the
// cgoBytes chunked copy: it returns how many bytes to copy at offset off
// when copying n bytes total, capped at cBytesLenMax per chunk so each count
// fits the 32-bit signed C.int domain.
func nextCgoBytesChunk(off, n int) int {
	rest := n - off
	if rest > cBytesLenMax {
		return cBytesLenMax
	}
	return rest
}

// ---------------------------------------------------------------------------
// Reader handle
// ---------------------------------------------------------------------------

// readerHandle is the native half of the public Reader type.
type readerHandle struct{ nativeHandle }

// resultSetHandle is the native half of ResultSet. It is valid only while
// its owning reader remains open.
type resultSetHandle struct {
	nativeHandle
	tagFilter unsafe.Pointer
}

func (h *resultSetHandle) close() error {
	if err := h.nativeHandle.close(); err != nil {
		return err
	}
	if h.tagFilter != nil {
		C.tsfile_tag_filter_free(C.TagFilterHandle(h.tagFilter))
		h.tagFilter = nil
	}
	return nil
}

// releaseReader calls tsfile_reader_close, which deletes the native reader.
// A nil pointer is reported as RET_OK, matching close-on-nil semantics.
func releaseReader(ptr unsafe.Pointer) cerrno {
	if ptr == nil {
		return C.RET_OK
	}
	return cerrno(C.tsfile_reader_close(C.TsFileReader(ptr)))
}

// newReaderHandle opens the TsFile at path. The C ABI returns a NULL
// handle with the error code (RET_FILE_OPEN_ERR for a missing path) on
// failure.
func newReaderHandle(path string) (*readerHandle, error) {
	if err := validateCString("open reader", "path", path); err != nil {
		return nil, err
	}
	cs := cStringPtr(path)
	defer freeCString(cs)
	var code C.ERRNO
	h := C.tsfile_reader_new(cs, &code)
	if h == nil {
		return nil, newError("open reader", cerrno(code))
	}
	return &readerHandle{nativeHandle{ptr: unsafe.Pointer(h), release: releaseReader}}, nil
}

func releaseResultSet(ptr unsafe.Pointer) cerrno {
	if ptr == nil {
		return C.RET_OK
	}
	p := C.ResultSet(ptr)
	C.free_tsfile_result_set(&p)
	return C.RET_OK
}

// ---------------------------------------------------------------------------
// Tablet handle
// ---------------------------------------------------------------------------

// tabletHandle is the native half of the public Tablet type.
type tabletHandle struct{ nativeHandle }

// releaseTablet calls free_tablet, which deletes the native tablet and nulls
// the in-place C pointer.
func releaseTablet(ptr unsafe.Pointer) cerrno {
	if ptr == nil {
		return C.RET_OK
	}
	var p C.Tablet = C.Tablet(ptr)
	C.free_tablet(&p)
	return C.RET_OK
}

// cTabletMaxRowsMax is the native row-capacity limit: storage::Tablet
// asserts max_rows > 0 && max_rows < (1 << 30), so the bridge only admits
// 1 <= maxRows < cTabletMaxRowsMax. Beyond the native assertion, larger
// capacities would make the native row-buffer index math (int32 offsets of
// length max_rows + 1) unsafe.
const cTabletMaxRowsMax = 1 << 30

// validateTabletMaxRows is a pure integer validator for the tablet row
// capacity: it reports whether maxRows fits the native domain
// [1, cTabletMaxRowsMax). It performs no allocation, so the boundary can be
// tested with bare integers like cTabletMaxRowsMax.
func validateTabletMaxRows(op string, maxRows int) error {
	if maxRows < 1 {
		return newError(op, C.RET_INVALID_ARG)
	}
	if maxRows >= cTabletMaxRowsMax {
		err := newError(op, C.RET_OVERFLOW)
		return fmt.Errorf("%w: maxRows %d exceeds native tablet limit %d", err, maxRows, cTabletMaxRowsMax)
	}
	return nil
}

// validateCInt32 is a pure integer validator for the C ABI's signed int
// parameters (C int is 32-bit on every host this bridge targets): it reports
// whether v survives the Go int -> C int conversion without truncation. It
// performs no allocation, so the boundary can be tested with bare integers
// like math.MaxInt32+1.
func validateCInt32(op, field string, v int) error {
	if v < math.MinInt32 || v > math.MaxInt32 {
		err := newError(op, C.RET_OVERFLOW)
		return fmt.Errorf("%w: %s %d exceeds C int range", err, field, int64(v))
	}
	return nil
}

// newTabletHandle creates a targetless table batch. The bound Writer supplies
// the table name when the batch is written.
func newTabletHandle(columnNames []string, dataTypes []DataType, maxRows int) (*tabletHandle, error) {
	if len(columnNames) == 0 || len(dataTypes) != len(columnNames) || maxRows < 1 {
		return nil, newError("new tablet", C.RET_INVALID_ARG)
	}
	if err := validateTabletMaxRows("new tablet", maxRows); err != nil {
		return nil, err
	}
	if err := validateCInt32("new tablet", "column count", len(columnNames)); err != nil {
		return nil, err
	}
	if idx, err := validateCStrings("new tablet", "column", columnNames); err != nil {
		return nil, fmt.Errorf("%w: column name %d is invalid", err, idx)
	}
	names, err := marshalCStrings(columnNames)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(names)

	types := make([]C.TSDataType, len(dataTypes))
	for i, t := range dataTypes {
		types[i] = C.TSDataType(t)
	}
	h := C.tablet_new(&names[0], &types[0], C.uint32_t(len(dataTypes)), C.uint32_t(maxRows))
	if h == nil {
		return nil, newError("new tablet", C.RET_OOM)
	}
	return &tabletHandle{nativeHandle{ptr: unsafe.Pointer(h), release: releaseTablet}}, nil
}

// rowCount returns the number of rows currently added to the tablet.
func (h *tabletHandle) rowCount() int {
	if h.ptr == nil {
		return 0
	}
	return int(C.tablet_get_cur_row_size(C.Tablet(h.ptr)))
}

func (h *tabletHandle) addTimestamp(row int, timestamp int64) error {
	return newError("add tablet timestamp", cerrno(C.tablet_add_timestamp(
		C.Tablet(h.ptr), C.uint32_t(row), C.Timestamp(timestamp))))
}

func (h *tabletHandle) addBool(row, column int, value bool) error {
	return newError("set tablet bool", cerrno(C.tablet_add_value_by_index_bool(
		C.Tablet(h.ptr), C.uint32_t(row), C.uint32_t(column), C.bool(value))))
}

func (h *tabletHandle) addInt32(row, column int, value int32) error {
	return newError("set tablet int32", cerrno(C.tablet_add_value_by_index_int32_t(
		C.Tablet(h.ptr), C.uint32_t(row), C.uint32_t(column), C.int32_t(value))))
}

func (h *tabletHandle) addInt64(row, column int, value int64) error {
	return newError("set tablet int64", cerrno(C.tablet_add_value_by_index_int64_t(
		C.Tablet(h.ptr), C.uint32_t(row), C.uint32_t(column), C.int64_t(value))))
}

func (h *tabletHandle) addFloat32(row, column int, value float32) error {
	return newError("set tablet float32", cerrno(C.tablet_add_value_by_index_float(
		C.Tablet(h.ptr), C.uint32_t(row), C.uint32_t(column), C.float(value))))
}

func (h *tabletHandle) addFloat64(row, column int, value float64) error {
	return newError("set tablet float64", cerrno(C.tablet_add_value_by_index_double(
		C.Tablet(h.ptr), C.uint32_t(row), C.uint32_t(column), C.double(value))))
}

func (h *tabletHandle) addString(row, column int, value string) error {
	if err := validateCBytesLen("set tablet string", len(value)); err != nil {
		return err
	}
	cs := C.CString(value)
	if cs == nil {
		return newError("set tablet string", C.RET_OOM)
	}
	defer freeCString(cs)
	return newError("set tablet string", cerrno(C.tablet_add_value_by_index_string_with_len(
		C.Tablet(h.ptr), C.uint32_t(row), C.uint32_t(column), cs, C.int(len(value)))))
}

func (h *tabletHandle) addBytes(row, column int, value []byte) error {
	p, n, err := copyCBytes("set tablet bytes", value)
	if err != nil {
		return err
	}
	defer freeCBytes(p)
	return newError("set tablet bytes", cerrno(C.tablet_add_value_by_index_string_with_len(
		C.Tablet(h.ptr), C.uint32_t(row), C.uint32_t(column), (*C.char)(p), C.int(n))))
}

// ---------------------------------------------------------------------------
// Writer handle
// ---------------------------------------------------------------------------

// writerHandle owns a table writer and the WriteFile passed to it.
type writerHandle struct {
	nativeHandle
	file unsafe.Pointer
}

// releaseWriter closes and deletes the native writer; nil is RET_OK.
func releaseWriter(ptr unsafe.Pointer) cerrno {
	if ptr == nil {
		return C.RET_OK
	}
	return cerrno(C.tsfile_writer_close(C.TsFileWriter(ptr)))
}

func (h *writerHandle) close() error {
	if err := h.nativeHandle.close(); err != nil {
		return err
	}
	if h.file != nil {
		file := C.WriteFile(h.file)
		C.free_write_file(&file)
		h.file = nil
	}
	return nil
}

// newWriterHandle opens (or creates) the TsFile at path for writing.
// memoryThresholdBytes bounds the in-memory write buffer.
func newWriterHandle(path string, schema TableSchema, memoryThresholdBytes uint64) (*writerHandle, error) {
	if err := validateCString("open writer", "path", path); err != nil {
		return nil, err
	}
	cs := cStringPtr(path)
	defer freeCString(cs)
	var code C.ERRNO
	file := C.write_file_new(cs, &code)
	if file == nil {
		return nil, newError("open writer", cerrno(code))
	}
	nativeSchema, freeSchema, err := allocTableSchema(schema)
	if err != nil {
		C.free_write_file(&file)
		return nil, err
	}
	defer freeSchema()
	h := C.tsfile_writer_new_with_memory_threshold(file, nativeSchema, C.uint64_t(memoryThresholdBytes), &code)
	if h == nil {
		C.free_write_file(&file)
		return nil, newError("open writer", cerrno(code))
	}
	return &writerHandle{
		nativeHandle: nativeHandle{ptr: unsafe.Pointer(h), release: releaseWriter},
		file:         unsafe.Pointer(file),
	}, nil
}

func allocTableSchema(schema TableSchema) (*C.TableSchema, func(), error) {
	tableName := cStringPtr(schema.Table)
	names, err := marshalCStrings(columnNames(schema.Columns))
	if err != nil {
		freeCString(tableName)
		return nil, func() {}, err
	}
	mem := C.malloc(C.size_t(len(schema.Columns)) * C.size_t(C.sizeof_ColumnSchema))
	if mem == nil {
		freeCString(tableName)
		freeCStrings(names)
		return nil, func() {}, newError("marshal table schema", C.RET_OOM)
	}
	columns := unsafe.Slice((*C.ColumnSchema)(mem), len(schema.Columns))
	for i, item := range schema.Columns {
		columns[i].column_name = names[i]
		columns[i].data_type = C.TSDataType(item.DataType)
		columns[i].column_category = C.ColumnCategory(item.Category)
	}
	native := (*C.TableSchema)(C.malloc(C.size_t(C.sizeof_TableSchema)))
	if native == nil {
		freeCString(tableName)
		freeCStrings(names)
		C.free(mem)
		return nil, func() {}, newError("marshal table schema", C.RET_OOM)
	}
	native.table_name = tableName
	native.column_schemas = (*C.ColumnSchema)(mem)
	native.column_num = C.int(len(schema.Columns))
	return native, func() {
		freeCString(tableName)
		freeCStrings(names)
		C.free(mem)
		C.free(unsafe.Pointer(native))
	}, nil
}

func (h *writerHandle) writeTableTablet(tablet *tabletHandle) error {
	return newError("write table tablet", cerrno(C.tsfile_writer_write(
		C.TsFileWriter(h.ptr), C.Tablet(tablet.ptr))))
}

func (h *writerHandle) writeArrow(array, schema unsafe.Pointer, timeColumn int) error {
	return newError("write Arrow batch", cerrno(C.tsfile_writer_write_arrow(
		C.TsFileWriter(h.ptr), (*C.ArrowArray)(array), (*C.ArrowSchema)(schema), C.int(timeColumn))))
}

func (h *writerHandle) flush() error {
	return newError("flush writer", cerrno(C.tsfile_writer_flush(C.TsFileWriter(h.ptr))))
}

func (h *writerHandle) addProperty(key string, value []byte) error {
	keyPtr, keyLen, err := copyCBytes("add property", []byte(key))
	if err != nil {
		return err
	}
	defer freeCBytes(keyPtr)
	valuePtr, valueLen, err := copyCBytes("add property", value)
	if err != nil {
		return err
	}
	defer freeCBytes(valuePtr)
	return newError("add property", cerrno(C.tsfile_writer_add_tsfile_property(
		C.TsFileWriter(h.ptr), (*C.char)(keyPtr), C.uint32_t(keyLen),
		(*C.uint8_t)(valuePtr), C.uint32_t(valueLen))))
}

func newResultSetHandle(ptr C.ResultSet, code C.ERRNO, op string) (*resultSetHandle, error) {
	if code != C.RET_OK {
		if ptr != nil {
			p := ptr
			C.free_tsfile_result_set(&p)
		}
		return nil, newError(op, cerrno(code))
	}
	if ptr == nil {
		return nil, fmt.Errorf("%s: native query returned a nil result", op)
	}
	return &resultSetHandle{nativeHandle: nativeHandle{ptr: unsafe.Pointer(ptr), release: releaseResultSet}}, nil
}

func (h *readerHandle) queryTableOptions(table string, columns []string, options queryOptions) (*resultSetHandle, error) {
	tableName := cStringPtr(table)
	defer freeCString(tableName)
	columnNames, err := marshalCStrings(columns)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(columnNames)
	var nativeFilter C.TagFilterHandle
	if options.tagFilter != nil {
		nativeFilter, err = h.compileTagFilter(table, options.tagFilter)
		if err != nil {
			return nil, err
		}
	}
	var code C.ERRNO
	result := C.tsfile_reader_query_table(
		C.TsFileReader(h.ptr), tableName, &columnNames[0], C.uint32_t(len(columnNames)),
		C.Timestamp(options.start), C.Timestamp(options.end), C.int(options.offset), C.int(options.limit),
		nativeFilter, C.int(options.batchSize), &code)
	handle, err := newResultSetHandle(result, code, "query table")
	if err != nil {
		if nativeFilter != nil {
			C.tsfile_tag_filter_free(nativeFilter)
		}
		return nil, err
	}
	handle.tagFilter = unsafe.Pointer(nativeFilter)
	return handle, nil
}

func (h *readerHandle) compileTagFilter(table string, filter TagFilter) (C.TagFilterHandle, error) {
	value := filter.(*tagFilter)
	switch value.op {
	case tagEqual, tagNotEqual, tagLess, tagLessEqual, tagGreater, tagGreaterEqual:
		tableName, column, operand := cStringPtr(table), cStringPtr(value.column), cStringPtr(value.value)
		defer freeCString(tableName)
		defer freeCString(column)
		defer freeCString(operand)
		var code C.ERRNO
		handle := C.tsfile_tag_filter_create(C.TsFileReader(h.ptr), tableName, column, operand,
			C.TagFilterOp(value.op), &code)
		if code != C.RET_OK || handle == nil {
			if code == C.RET_OK {
				code = C.RET_COLUMN_NOT_EXIST
			}
			return nil, newError("create tag filter", cerrno(code))
		}
		return handle, nil
	case tagBetween:
		tableName, column := cStringPtr(table), cStringPtr(value.column)
		lower, upper := cStringPtr(value.value), cStringPtr(value.secondValue)
		defer freeCString(tableName)
		defer freeCString(column)
		defer freeCString(lower)
		defer freeCString(upper)
		var code C.ERRNO
		handle := C.tsfile_tag_filter_between(C.TsFileReader(h.ptr), tableName, column, lower, upper, false, &code)
		if code != C.RET_OK || handle == nil {
			if code == C.RET_OK {
				code = C.RET_COLUMN_NOT_EXIST
			}
			return nil, newError("create tag filter", cerrno(code))
		}
		return handle, nil
	case tagAnd, tagOr:
		left, err := h.compileTagFilter(table, value.left)
		if err != nil {
			return nil, err
		}
		right, err := h.compileTagFilter(table, value.right)
		if err != nil {
			C.tsfile_tag_filter_free(left)
			return nil, err
		}
		var combined C.TagFilterHandle
		if value.op == tagAnd {
			combined = C.tsfile_tag_filter_and(left, right)
		} else {
			combined = C.tsfile_tag_filter_or(left, right)
		}
		if combined == nil {
			C.tsfile_tag_filter_free(left)
			C.tsfile_tag_filter_free(right)
			return nil, newError("create tag filter", C.RET_INVALID_ARG)
		}
		return combined, nil
	case tagNot:
		child, err := h.compileTagFilter(table, value.left)
		if err != nil {
			return nil, err
		}
		combined := C.tsfile_tag_filter_not(child)
		if combined == nil {
			C.tsfile_tag_filter_free(child)
			return nil, newError("create tag filter", C.RET_INVALID_ARG)
		}
		return combined, nil
	default:
		return nil, newError("create tag filter", C.RET_INVALID_ARG)
	}
}

func copyTableSchemaFromC(schema *C.TableSchema) TableSchema {
	result := TableSchema{Table: C.GoString(schema.table_name)}
	if schema.column_num <= 0 || schema.column_schemas == nil {
		return result
	}
	columns := unsafe.Slice(schema.column_schemas, int(schema.column_num))
	result.Columns = make([]ColumnSchema, len(columns))
	for i, column := range columns {
		result.Columns[i] = ColumnSchema{
			Name: C.GoString(column.column_name), DataType: DataType(column.data_type), Category: ColumnCategory(column.column_category),
		}
	}
	return result
}

func (h *readerHandle) tableSchema(table string) (TableSchema, error) {
	tableName := cStringPtr(table)
	defer freeCString(tableName)
	var native C.TableSchema
	code := C.tsfile_reader_get_table_schema_checked(C.TsFileReader(h.ptr), tableName, &native)
	if code != C.RET_OK {
		return TableSchema{}, newError("get table schema", cerrno(code))
	}
	defer C.free_table_schema(native)
	return copyTableSchemaFromC(&native), nil
}

func (h *readerHandle) allTableSchemas() ([]TableSchema, error) {
	var native *C.TableSchema
	var count C.uint32_t
	code := C.tsfile_reader_get_all_table_schemas_checked(C.TsFileReader(h.ptr), &native, &count)
	if code != C.RET_OK {
		return nil, newError("get all table schemas", cerrno(code))
	}
	if native == nil || count == 0 {
		return []TableSchema{}, nil
	}
	items := unsafe.Slice(native, int(count))
	result := make([]TableSchema, len(items))
	for i := range items {
		result[i] = copyTableSchemaFromC(&items[i])
		C.free_table_schema(items[i])
	}
	C.free(unsafe.Pointer(native))
	return result, nil
}

func (h *resultSetHandle) next() (bool, error) {
	var code C.ERRNO
	ok := bool(C.tsfile_result_set_next(C.ResultSet(h.ptr), &code))
	return ok, newError("advance result set", cerrno(code))
}

func (h *resultSetHandle) nextArrow(array, schema unsafe.Pointer) error {
	return newError("read Arrow batch", cerrno(C.tsfile_result_set_get_next_tsblock_as_arrow(
		C.ResultSet(h.ptr), (*C.ArrowArray)(array), (*C.ArrowSchema)(schema))))
}

func (h *resultSetHandle) metadata() []ColumnMetadata {
	meta := C.tsfile_result_set_get_metadata(C.ResultSet(h.ptr))
	defer C.free_result_set_meta_data(meta)
	count := int(C.tsfile_result_set_metadata_get_column_num(meta))
	columns := make([]ColumnMetadata, count)
	for i := range columns {
		// The C metadata ABI and the public Go API are both 1-based.
		index := C.uint32_t(i + 1)
		columns[i] = ColumnMetadata{
			Name:     C.GoString(C.tsfile_result_set_metadata_get_column_name(meta, index)),
			DataType: DataType(C.tsfile_result_set_metadata_get_data_type(meta, index)),
		}
	}
	return columns
}

func (h *resultSetHandle) isNull(column int) bool {
	return bool(C.tsfile_result_set_is_null_by_index(C.ResultSet(h.ptr), C.uint32_t(column)))
}

func (h *resultSetHandle) bool(column int) bool {
	return bool(C.tsfile_result_set_get_value_by_index_bool(C.ResultSet(h.ptr), C.uint32_t(column)))
}

func (h *resultSetHandle) int32(column int) int32 {
	return int32(C.tsfile_result_set_get_value_by_index_int32_t(C.ResultSet(h.ptr), C.uint32_t(column)))
}

func (h *resultSetHandle) int64(column int) int64 {
	return int64(C.tsfile_result_set_get_value_by_index_int64_t(C.ResultSet(h.ptr), C.uint32_t(column)))
}

func (h *resultSetHandle) float32(column int) float32 {
	return float32(C.tsfile_result_set_get_value_by_index_float(C.ResultSet(h.ptr), C.uint32_t(column)))
}

func (h *resultSetHandle) float64(column int) float64 {
	return float64(C.tsfile_result_set_get_value_by_index_double(C.ResultSet(h.ptr), C.uint32_t(column)))
}

func (h *resultSetHandle) string(column int) string {
	p := C.tsfile_result_set_get_value_by_index_string(C.ResultSet(h.ptr), C.uint32_t(column))
	if p == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(p))
	return C.GoString(p)
}

func (h *resultSetHandle) bytes(column int) ([]byte, error) {
	var value *C.uint8_t
	var length C.uint32_t
	code := C.tsfile_result_set_get_value_by_index_binary(
		C.ResultSet(h.ptr), C.uint32_t(column), &value, &length)
	if code != C.RET_OK {
		return nil, newError("get result bytes", cerrno(code))
	}
	if value == nil {
		return []byte{}, nil
	}
	defer C.free(unsafe.Pointer(value))
	return cgoBytes(unsafe.Pointer(value), int(length)), nil
}

// unsafe_NewPointer returns a small, non-nil Go-managed pointer suitable for
// tests that need to verify nativeHandle bookkeeping (close keeps the stored
// pointer on failure and clears it on success) without creating a real C
// handle. The returned pointer is never passed to any release function;
// callers must only observe and compare it.
func unsafe_NewPointer() unsafe.Pointer {
	return unsafe.Pointer(&cgoTestPointer)
}

// cgoTestPointer is a package-level byte so unsafe_NewPointer has stable
// non-nil backing memory. It is never freed and never handed to C.
var cgoTestPointer byte
