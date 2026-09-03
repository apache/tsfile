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

import (
	"fmt"
	"math"
	"strings"
	"unsafe"
)

// errnoOK and errnoFileClose are plain-Go mirrors of the C RET_* constants
// used by the test stubs: _test files live in a separate cgo translation
// unit and cannot reference C types or C.RET_* constants directly, so the
// bridge exposes just the numeric values they need.
const (
	errnoOK        = C.RET_OK
	errnoFileClose = C.RET_FILE_CLOSE_ERR
)

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
type resultSetHandle struct{ nativeHandle }

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

// newTabletHandle creates a tablet whose rows target target (a device path or
// a table name) with the given column schema and row capacity. maxRows must
// be in [1, cTabletMaxRowsMax) per the native Tablet contract.
func newTabletHandle(target string, columnNames []string, dataTypes []DataType, maxRows int) (*tabletHandle, error) {
	if len(columnNames) == 0 || len(dataTypes) != len(columnNames) || maxRows < 1 {
		return nil, newError("new tablet", C.RET_INVALID_ARG)
	}
	if err := validateTabletMaxRows("new tablet", maxRows); err != nil {
		return nil, err
	}
	if err := validateCInt32("new tablet", "column count", len(columnNames)); err != nil {
		return nil, err
	}
	if err := validateCString("new tablet", "target", target); err != nil {
		return nil, err
	}
	if idx, err := validateCStrings("new tablet", "column", columnNames); err != nil {
		return nil, fmt.Errorf("%w: column name %d is invalid", err, idx)
	}
	targetCs := cStringPtr(target)
	defer freeCString(targetCs)
	names, err := marshalCStrings(columnNames)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(names)

	types := make([]C.TSDataType, len(dataTypes))
	for i, t := range dataTypes {
		types[i] = C.TSDataType(t)
	}
	h := C.tablet_new_with_target_name(
		targetCs,
		&names[0],
		&types[0],
		C.int(len(dataTypes)),
		C.int(maxRows),
	)
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

// ---------------------------------------------------------------------------
// Writer handle
// ---------------------------------------------------------------------------

// writerHandle is the native half of the public Writer type. It uses the
// public generic writer API; tsfile_generic_writer_close flushes, closes, and
// deletes the native writer.
type writerHandle struct{ nativeHandle }

// releaseWriter closes and deletes the native writer; nil is RET_OK.
func releaseWriter(ptr unsafe.Pointer) cerrno {
	if ptr == nil {
		return C.RET_OK
	}
	return cerrno(C.tsfile_generic_writer_close(C.TsFileGenericWriter(ptr)))
}

// newWriterHandle opens (or creates) the TsFile at path for writing.
// memoryThresholdBytes bounds the in-memory write buffer.
func newWriterHandle(path string, memoryThresholdBytes uint64) (*writerHandle, error) {
	if err := validateCString("open writer", "path", path); err != nil {
		return nil, err
	}
	cs := cStringPtr(path)
	defer freeCString(cs)
	var code C.ERRNO
	h := C.tsfile_generic_writer_new(cs, C.uint64_t(memoryThresholdBytes), &code)
	if h == nil {
		return nil, newError("open writer", cerrno(code))
	}
	return &writerHandle{nativeHandle{ptr: unsafe.Pointer(h), release: releaseWriter}}, nil
}

func allocTimeseriesSchema(schema TimeseriesSchema) (*C.TimeseriesSchema, func(), error) {
	name := cStringPtr(schema.Name)
	if name == nil {
		return nil, func() {}, newError("marshal timeseries schema", C.RET_OOM)
	}
	p := (*C.TimeseriesSchema)(C.malloc(C.size_t(C.sizeof_TimeseriesSchema)))
	if p == nil {
		freeCString(name)
		return nil, func() {}, newError("marshal timeseries schema", C.RET_OOM)
	}
	p.timeseries_name = name
	p.data_type = C.TSDataType(schema.DataType)
	p.encoding = C.TSEncoding(schema.Encoding)
	p.compression = C.CompressionType(schema.Compression)
	return p, func() {
		freeCString(name)
		C.free(unsafe.Pointer(p))
	}, nil
}

func (h *writerHandle) registerTimeseries(device string, schema TimeseriesSchema) error {
	deviceName := cStringPtr(device)
	defer freeCString(deviceName)
	native, freeSchema, err := allocTimeseriesSchema(schema)
	if err != nil {
		return err
	}
	defer freeSchema()
	return newError("register timeseries", cerrno(C.tsfile_generic_writer_register_timeseries(
		C.TsFileGenericWriter(h.ptr), deviceName, native)))
}

func (h *writerHandle) registerDevice(schema DeviceSchema) error {
	deviceName := cStringPtr(schema.Device)
	defer freeCString(deviceName)
	names, err := marshalCStrings(timeseriesNames(schema.TimeSeries))
	if err != nil {
		return err
	}
	defer freeCStrings(names)

	var series *C.TimeseriesSchema
	if len(schema.TimeSeries) > 0 {
		mem := C.malloc(C.size_t(len(schema.TimeSeries)) * C.size_t(C.sizeof_TimeseriesSchema))
		if mem == nil {
			return newError("marshal device schema", C.RET_OOM)
		}
		defer C.free(mem)
		items := unsafe.Slice((*C.TimeseriesSchema)(mem), len(schema.TimeSeries))
		for i, item := range schema.TimeSeries {
			items[i].timeseries_name = names[i]
			items[i].data_type = C.TSDataType(item.DataType)
			items[i].encoding = C.TSEncoding(item.Encoding)
			items[i].compression = C.CompressionType(item.Compression)
		}
		series = (*C.TimeseriesSchema)(mem)
	}
	native := (*C.DeviceSchema)(C.malloc(C.size_t(C.sizeof_DeviceSchema)))
	if native == nil {
		return newError("marshal device schema", C.RET_OOM)
	}
	defer C.free(unsafe.Pointer(native))
	native.device_name = deviceName
	native.timeseries_schema = series
	native.timeseries_num = C.int(len(schema.TimeSeries))
	return newError("register device", cerrno(C.tsfile_generic_writer_register_device(
		C.TsFileGenericWriter(h.ptr), native)))
}

func (h *writerHandle) registerTable(schema TableSchema) error {
	tableName := cStringPtr(schema.Table)
	defer freeCString(tableName)
	names, err := marshalCStrings(columnNames(schema.Columns))
	if err != nil {
		return err
	}
	defer freeCStrings(names)

	mem := C.malloc(C.size_t(len(schema.Columns)) * C.size_t(C.sizeof_ColumnSchema))
	if mem == nil {
		return newError("marshal table schema", C.RET_OOM)
	}
	defer C.free(mem)
	columns := unsafe.Slice((*C.ColumnSchema)(mem), len(schema.Columns))
	for i, item := range schema.Columns {
		columns[i].column_name = names[i]
		columns[i].data_type = C.TSDataType(item.DataType)
		columns[i].column_category = C.ColumnCategory(item.Category)
	}
	native := (*C.TableSchema)(C.malloc(C.size_t(C.sizeof_TableSchema)))
	if native == nil {
		return newError("marshal table schema", C.RET_OOM)
	}
	defer C.free(unsafe.Pointer(native))
	native.table_name = tableName
	native.column_schemas = (*C.ColumnSchema)(mem)
	native.column_num = C.int(len(schema.Columns))
	return newError("register table", cerrno(C.tsfile_generic_writer_register_table(
		C.TsFileGenericWriter(h.ptr), native)))
}

func (h *writerHandle) writeTreeTablet(tablet *tabletHandle) error {
	return newError("write tree tablet", cerrno(C.tsfile_generic_writer_write_tree_tablet(
		C.TsFileGenericWriter(h.ptr), C.Tablet(tablet.ptr))))
}

func (h *writerHandle) writeTableTablet(tablet *tabletHandle) error {
	return newError("write table tablet", cerrno(C.tsfile_generic_writer_write_table_tablet(
		C.TsFileGenericWriter(h.ptr), C.Tablet(tablet.ptr))))
}

func (h *writerHandle) flush() error {
	return newError("flush writer", cerrno(C.tsfile_generic_writer_flush(C.TsFileGenericWriter(h.ptr))))
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
	return newError("add property", cerrno(C.tsfile_generic_writer_add_tsfile_property(
		C.TsFileGenericWriter(h.ptr), (*C.char)(keyPtr), C.uint32_t(keyLen),
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
	return &resultSetHandle{nativeHandle{ptr: unsafe.Pointer(ptr), release: releaseResultSet}}, nil
}

func (h *readerHandle) queryTable(query TableQuery) (*resultSetHandle, error) {
	table := cStringPtr(query.Table)
	defer freeCString(table)
	columns, err := marshalCStrings(query.Columns)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(columns)
	var code C.ERRNO
	result := C.tsfile_query_table(C.TsFileReader(h.ptr), table, &columns[0],
		C.uint32_t(len(columns)), C.Timestamp(query.Start), C.Timestamp(query.End), &code)
	return newResultSetHandle(result, code, "query table")
}

func (h *readerHandle) queryTree(query TreeQuery) (*resultSetHandle, error) {
	paths, err := marshalCStrings(query.Paths)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(paths)
	var code C.ERRNO
	result := C.tsfile_reader_query_tree(C.TsFileReader(h.ptr), &paths[0],
		C.uint32_t(len(paths)), C.Timestamp(query.Start), C.Timestamp(query.End), &code)
	return newResultSetHandle(result, code, "query tree")
}

func (h *readerHandle) queryTreeRows(query TreeRowsQuery) (*resultSetHandle, error) {
	devices, err := marshalCStrings(query.Devices)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(devices)
	measurements, err := marshalCStrings(query.Measurements)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(measurements)
	var code C.ERRNO
	result := C.tsfile_reader_query_tree_by_row(C.TsFileReader(h.ptr), &devices[0], C.int(len(devices)),
		&measurements[0], C.int(len(measurements)), C.int(query.Offset), C.int(query.Limit), &code)
	return newResultSetHandle(result, code, "query tree rows")
}

func (h *readerHandle) queryTableRows(query TableRowsQuery) (*resultSetHandle, error) {
	table := cStringPtr(query.Table)
	defer freeCString(table)
	columns, err := marshalCStrings(query.Columns)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(columns)
	var code C.ERRNO
	result := C.tsfile_reader_query_table_by_row(C.TsFileReader(h.ptr), table, &columns[0],
		C.int(len(columns)), C.int(query.Offset), C.int(query.Limit), nil, C.int(query.BatchSize), &code)
	return newResultSetHandle(result, code, "query table rows")
}

func (h *resultSetHandle) next() (bool, error) {
	var code C.ERRNO
	ok := bool(C.tsfile_result_set_next(C.ResultSet(h.ptr), &code))
	return ok, newError("advance result set", cerrno(code))
}

func (h *resultSetHandle) metadata() []ColumnMetadata {
	meta := C.tsfile_result_set_get_metadata(C.ResultSet(h.ptr))
	defer C.free_result_set_meta_data(meta)
	count := int(C.tsfile_result_set_metadata_get_column_num(meta))
	columns := make([]ColumnMetadata, count)
	for i := range columns {
		// The C metadata ABI is 1-based; the public Go API is 0-based.
		index := C.uint32_t(i + 1)
		columns[i] = ColumnMetadata{
			Name:     C.GoString(C.tsfile_result_set_metadata_get_column_name(meta, index)),
			DataType: DataType(C.tsfile_result_set_metadata_get_data_type(meta, index)),
		}
	}
	return columns
}

func (h *resultSetHandle) isNull(column int) bool {
	return bool(C.tsfile_result_set_is_null_by_index(C.ResultSet(h.ptr), C.uint32_t(column+1)))
}

func (h *resultSetHandle) bool(column int) bool {
	return bool(C.tsfile_result_set_get_value_by_index_bool(C.ResultSet(h.ptr), C.uint32_t(column+1)))
}

func (h *resultSetHandle) int32(column int) int32 {
	return int32(C.tsfile_result_set_get_value_by_index_int32_t(C.ResultSet(h.ptr), C.uint32_t(column+1)))
}

func (h *resultSetHandle) int64(column int) int64 {
	return int64(C.tsfile_result_set_get_value_by_index_int64_t(C.ResultSet(h.ptr), C.uint32_t(column+1)))
}

func (h *resultSetHandle) float32(column int) float32 {
	return float32(C.tsfile_result_set_get_value_by_index_float(C.ResultSet(h.ptr), C.uint32_t(column+1)))
}

func (h *resultSetHandle) float64(column int) float64 {
	return float64(C.tsfile_result_set_get_value_by_index_double(C.ResultSet(h.ptr), C.uint32_t(column+1)))
}

func (h *resultSetHandle) string(column int) string {
	p := C.tsfile_result_set_get_value_by_index_string(C.ResultSet(h.ptr), C.uint32_t(column+1))
	if p == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(p))
	return C.GoString(p)
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
