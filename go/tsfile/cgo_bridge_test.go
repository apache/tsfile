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

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

// TestBridgeMapsMissingFileError exercises one real C call through the cgo
// boundary. tsfile_reader_new on a path that cannot possibly exist must
// return a NULL handle and RET_FILE_OPEN_ERR; the bridge must map that into a
// *Error carrying the same numeric code, with no retained native handle.
func TestBridgeMapsMissingFileError(t *testing.T) {
	missing := filepath.Join(os.TempDir(), "tsfile-go-bridge", "definitely-missing.tsfile")
	if _, err := os.Stat(missing); err == nil {
		t.Fatalf("test path unexpectedly exists: %s", missing)
	}

	h, err := newReaderHandle(missing)
	if h != nil {
		// Never expected, but release any created handle to keep C
		// allocations auditable.
		_ = h.close()
		t.Fatalf("expected NULL reader handle, got %v", h.ptr)
	}
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, ErrFileOpen) {
		t.Fatalf("expected error to match ErrFileOpen, got %v", err)
	}
}

// TestBridgeTabletHandleReleases proves a created C handle is released:
// close must be idempotent (the second close is a no-op, not a
// use-after-free) and must clear the stored pointer.
func TestBridgeTabletHandleReleases(t *testing.T) {
	h, err := newTabletHandle("bridge-dev", []string{"s1"}, []DataType{DataTypeInt64}, 8)
	if err != nil {
		t.Fatalf("newTabletHandle: %v", err)
	}
	if h.ptr == nil {
		t.Fatal("expected non-nil native pointer")
	}
	if err := h.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if h.ptr != nil {
		t.Fatal("handle pointer not cleared after close")
	}
	if err := h.close(); err != nil {
		t.Fatalf("second close must be a no-op, got %v", err)
	}
}

// TestBridgeWriterHandleLifecycle creates a real writer handle through the
// public generic writer ABI, verifies the non-NULL pointer, and releases it;
// the C ABI deletes the writer inside tsfile_generic_writer_close.
func TestBridgeWriterHandleLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bridge.tsfile")
	h, err := newWriterHandle(path, 1<<20)
	if err != nil {
		t.Fatalf("newWriterHandle: %v", err)
	}
	if h.ptr == nil {
		t.Fatal("expected non-nil native pointer")
	}
	if err := h.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if h.ptr != nil {
		t.Fatal("handle pointer not cleared after close")
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat written file: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("expected a non-empty TsFile after writer close")
	}
}

// TestBridgeTabletRowCount checks the tablet handle exposes its row count
// through the C API before release.
func TestBridgeTabletRowCount(t *testing.T) {
	h, err := newTabletHandle("bridge-dev", []string{"s1"}, []DataType{DataTypeInt64}, 4)
	if err != nil {
		t.Fatalf("newTabletHandle: %v", err)
	}
	defer func() {
		if err := h.close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()
	if got := h.rowCount(); got != 0 {
		t.Fatalf("expected 0 rows, got %d", got)
	}
}

// TestBridgeMarshalStrings verifies the C string array helper round-trips
// values and releases every allocation.
func TestBridgeMarshalStrings(t *testing.T) {
	values := []string{"a", "b c", "d"}
	names, err := marshalCStrings(values)
	if err != nil {
		t.Fatalf("marshalCStrings: %v", err)
	}
	defer freeCStrings(names)
	for i, want := range values {
		got := cgoString(unsafe.Pointer(names[i]))
		if got != want {
			t.Fatalf("round-trip[%d] = %q, want %q", i, got, want)
		}
	}

	// Empty input must allocate nothing.
	names, err = marshalCStrings(nil)
	if err != nil || len(names) != 0 {
		t.Fatalf("empty marshal = (%v, %v), want (empty, nil)", len(names), err)
	}
}

// TestBridgeCStringCopy pins that every non-empty value maps to a non-NULL C
// copy that round-trips, and that freeCString releases the allocation.
func TestBridgeCStringCopy(t *testing.T) {
	for _, want := range []string{"x", "device.1", "a b c"} {
		p := cStringPtr(want)
		if p == nil {
			t.Fatalf("cStringPtr(%q) returned nil", want)
		}
		if got := cgoString(unsafe.Pointer(p)); got != want {
			t.Fatalf("round-trip = %q, want %q", got, want)
		}
		freeCString(p)
	}
}

// TestBridgeValidateCString pins the pre-C validation: empty and
// NUL-embedded strings must be rejected with ErrInvalidArgument without any
// C allocation or call.
func TestBridgeValidateCString(t *testing.T) {
	for _, value := range []string{"", "dev\x00ice", "\x00", "a\x00b\x00"} {
		err := validateCString("op", "field", value)
		if err == nil {
			t.Fatalf("validateCString(%q) = nil, want error", value)
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("validateCString(%q) = %v, want ErrInvalidArgument", value, err)
		}
	}
	for _, value := range []string{"dev", "device.1", "s-1"} {
		if err := validateCString("op", "field", value); err != nil {
			t.Fatalf("validateCString(%q) = %v, want nil", value, err)
		}
	}
}

// TestBridgeReaderRejectsInvalidPath proves the reader constructor validates
// the path before crossing C: both empty and embedded-NUL paths must fail
// with ErrInvalidArgument (a NULL path would crash the native reader).
func TestBridgeReaderRejectsInvalidPath(t *testing.T) {
	for _, path := range []string{"", "dev\x00.tsfile"} {
		if _, err := newReaderHandle(path); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("newReaderHandle(%q) = %v, want ErrInvalidArgument", path, err)
		}
	}
}

// TestBridgeWriterRejectsInvalidPath pins the same validation for the writer
// constructor.
func TestBridgeWriterRejectsInvalidPath(t *testing.T) {
	for _, path := range []string{"", "out\x00.tsfile"} {
		if _, err := newWriterHandle(path, 1<<20); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("newWriterHandle(%q) = %v, want ErrInvalidArgument", path, err)
		}
	}
}

// TestBridgeTabletRejectsInvalidNames pins empty/NUL validation for the
// tablet target and column names: every case must fail with
// ErrInvalidArgument before any C call, so no native tablet is created.
func TestBridgeTabletRejectsInvalidNames(t *testing.T) {
	cases := []struct {
		name    string
		target  string
		columns []string
	}{
		{"embedded NUL target", "dev\x00", []string{"s1"}},
		{"embedded NUL column", "dev", []string{"s1", "s\x002"}},
		{"empty target", "", []string{"s1"}},
		{"empty column", "dev", []string{"s1", ""}},
	}
	for _, tc := range cases {
		types := make([]DataType, len(tc.columns))
		for i := range types {
			types[i] = DataTypeInt64
		}
		h, err := newTabletHandle(tc.target, tc.columns, types, 4)
		if h != nil {
			t.Fatalf("%s: expected no handle", tc.name)
		}
		if err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s: got %v, want ErrInvalidArgument", tc.name, err)
		}
	}
}

// TestBridgeCopyCBytes pins the C-copy semantics: the returned pointer is a
// C allocation (distinct from the Go slice, so cgocheck2 cannot observe a Go
// pointer), the contents round-trip through C including embedded NUL bytes,
// the length is the full byte length, and freeCBytes releases it. Empty and
// nil inputs map to (nil, 0) with no allocation.
func TestBridgeCopyCBytes(t *testing.T) {
	if p, n, err := copyCBytes("copy", nil); p != nil || n != 0 || err != nil {
		t.Fatalf("copyCBytes(nil) = (%v, %d, %v), want (nil, 0, nil)", p, n, err)
	}
	if p, n, err := copyCBytes("copy", []byte{}); p != nil || n != 0 || err != nil {
		t.Fatalf("copyCBytes(empty) = (%v, %d, %v), want (nil, 0, nil)", p, n, err)
	}
	b := []byte{0xde, 0xad, 0x00, 0xbe, 0xef, 0x00, 0x42}
	p, n, err := copyCBytes("copy", b)
	if err != nil {
		t.Fatalf("copyCBytes: %v", err)
	}
	defer freeCBytes(p)
	if n != 7 {
		t.Fatalf("length = %d, want 7", n)
	}
	// Read the C buffer back into Go and compare; this proves the embedded
	// zero bytes survived the C round trip and that the copy is a true copy
	// (mutating the Go slice after the copy must not change the C buffer).
	got := cgoBytes(p, int(n))
	if string(got) != string(b) {
		t.Fatalf("round-trip = % x, want % x", got, b)
	}
	b[0] = 0xff
	got2 := cgoBytes(p, int(n))
	if got2[0] != 0xde {
		t.Fatalf("C buffer changed after Go slice mutation: % x", got2)
	}
}

// TestBridgeCloseIsSafeOnNil ensures closing a zero-valued handle is a no-op,
// mirroring the idempotent Close contract of the public API.
func TestBridgeCloseIsSafeOnNil(t *testing.T) {
	var h readerHandle
	if err := h.close(); err != nil {
		t.Fatalf("close on nil handle: %v", err)
	}
}

// TestBridgeCloseKeepsOwnershipOnFailure proves the ownership contract of
// nativeHandle.close: when release fails (a non-RET_OK code) the stored
// pointer must be preserved so a later retry can release the C handle, and a
// retry that succeeds must clear it and be idempotent afterwards.
//
// The failing release is a pure Go stub (no C memory is involved), so the
// test cannot leak or double-free a native handle.
func TestBridgeCloseKeepsOwnershipOnFailure(t *testing.T) {
	dummy := unsafe_NewPointer()

	var calls int
	var h nativeHandle
	h.ptr = dummy
	h.release = func(ptr unsafe.Pointer) cerrno {
		calls++
		if calls == 1 {
			return errnoFileClose // pinned to RET_FILE_CLOSE_ERR in errno_define_c.h
		}
		return errnoOK
	}

	if err := h.close(); err == nil {
		t.Fatal("first close: expected error from failing release")
	} else if !errors.Is(err, ErrFileClose) {
		t.Fatalf("first close: got %v, want ErrFileClose", err)
	}
	if h.ptr == nil {
		t.Fatal("handle pointer cleared after failed close; ownership lost")
	}
	if h.ptr != dummy {
		t.Fatal("handle pointer changed after failed close")
	}

	if err := h.close(); err != nil {
		t.Fatalf("retry close after failure: %v", err)
	}
	if h.ptr != nil {
		t.Fatal("handle pointer not cleared after successful retry")
	}
	if err := h.close(); err != nil {
		t.Fatalf("close after successful retry must be a no-op, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("release called %d times, want 2", calls)
	}
}

// TestBridgeValidateCBytesLen pins the pure integer-length validator that
// protects every copyCBytes call. The effective C ABI length domain is
// [0, MaxInt32]: the native property API rejects uint32 lengths above
// INT32_MAX, and C.GoBytes takes a 32-bit signed count. All boundary cases
// use bare integers with no backing slice, so the boundaries are tested
// without allocating large memory.
func TestBridgeValidateCBytesLen(t *testing.T) {
	for _, n := range []int{0, 1, 16, 1 << 20, math.MaxInt32} {
		if err := validateCBytesLen("op", n); err != nil {
			t.Fatalf("validateCBytesLen(%d) = %v, want nil", n, err)
		}
	}
	for _, tc := range []struct {
		name string
		n    int
	}{
		{"maxInt32+1", math.MaxInt32 + 1},
		{"maxUint32", int(math.MaxUint32)},
		{"maxUint32+1", int(math.MaxUint32) + 1},
		{"negative", -1},
	} {
		if err := validateCBytesLen("op", tc.n); !errors.Is(err, ErrOverflow) {
			t.Fatalf("validateCBytesLen(%s) = %v, want ErrOverflow", tc.name, err)
		}
	}
}

// TestBridgeValidateTabletMaxRows pins the pure tablet row-capacity
// validator against the native domain [1, 1<<30). Every case is a bare
// integer: no tablet and no C allocation is involved, so the boundary at
// 1<<30 is tested without reserving row buffers.
func TestBridgeValidateTabletMaxRows(t *testing.T) {
	for _, n := range []int{1, 8, 1024, cTabletMaxRowsMax - 1} {
		if err := validateTabletMaxRows("op", n); err != nil {
			t.Fatalf("validateTabletMaxRows(%d) = %v, want nil", n, err)
		}
	}
	for _, n := range []int{0, -1, math.MinInt32, math.MinInt64} {
		if err := validateTabletMaxRows("op", n); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("validateTabletMaxRows(%d) = %v, want ErrInvalidArgument", n, err)
		}
	}
	for _, n := range []int{cTabletMaxRowsMax, cTabletMaxRowsMax + 1, math.MaxInt32, math.MaxInt64} {
		if err := validateTabletMaxRows("op", n); !errors.Is(err, ErrOverflow) {
			t.Fatalf("validateTabletMaxRows(%d) = %v, want ErrOverflow", n, err)
		}
	}
}

// TestBridgeValidateCInt32 pins the pure signed C int validator: values that
// would truncate in the Go int -> C int conversion must be rejected before
// the boundary. All cases are bare integers, no allocation.
func TestBridgeValidateCInt32(t *testing.T) {
	for _, n := range []int{0, 1, 4096, math.MaxInt32, math.MinInt32} {
		if err := validateCInt32("op", "field", n); err != nil {
			t.Fatalf("validateCInt32(%d) = %v, want nil", n, err)
		}
	}
	for _, n := range []int{math.MaxInt32 + 1, math.MaxInt64, math.MinInt32 - 1, math.MinInt64} {
		if err := validateCInt32("op", "field", n); !errors.Is(err, ErrOverflow) {
			t.Fatalf("validateCInt32(%d) = %v, want ErrOverflow", n, err)
		}
	}
}

// TestBridgeTabletRejectsOversizedMaxRows proves newTabletHandle rejects
// row capacities outside the native domain before any C call: the truncated
// C.int path (values >= 1<<32 wrap to small or negative ints on a 64-bit
// host) must be unreachable. No handle is returned and no native tablet is
// created, so no row buffers are allocated.
func TestBridgeTabletRejectsOversizedMaxRows(t *testing.T) {
	for _, maxRows := range []int{cTabletMaxRowsMax, 1 << 31, 1 << 32, math.MaxInt32, math.MaxInt64} {
		h, err := newTabletHandle("dev", []string{"s1"}, []DataType{DataTypeInt64}, maxRows)
		if h != nil {
			t.Fatalf("maxRows %d: expected no handle", maxRows)
		}
		if !errors.Is(err, ErrOverflow) {
			t.Fatalf("maxRows %d: got %v, want ErrOverflow", maxRows, err)
		}
	}
}

// TestBridgeNextCgoBytesChunk pins the pure step function of the cgoBytes
// chunked copy: the last chunk is capped at cBytesLenMax so every per-chunk
// count fits the 32-bit signed C.int domain, and the walk covers exactly n
// bytes. Pure integers only, no buffer is allocated.
func TestBridgeNextCgoBytesChunk(t *testing.T) {
	if got := nextCgoBytesChunk(0, 7); got != 7 {
		t.Fatalf("nextCgoBytesChunk(0, 7) = %d, want 7", got)
	}
	if got := nextCgoBytesChunk(0, math.MaxInt32); got != math.MaxInt32 {
		t.Fatalf("nextCgoBytesChunk(0, MaxInt32) = %d, want MaxInt32", got)
	}
	if got := nextCgoBytesChunk(0, cBytesLenMax+1); got != cBytesLenMax {
		t.Fatalf("nextCgoBytesChunk(0, MaxInt32+1) = %d, want MaxInt32", got)
	}
	// Walk representative multi-chunk ranges and check exact coverage.
	for _, n := range []int{math.MaxInt32 + 1, math.MaxInt32 + 2, int(math.MaxUint32) - 1, int(math.MaxUint32)} {
		off := 0
		covered := 0
		for off < n {
			m := nextCgoBytesChunk(off, n)
			if m <= 0 || m > cBytesLenMax {
				t.Fatalf("nextCgoBytesChunk(%d, %d) = %d, want (0, MaxInt32]", off, n, m)
			}
			off += m
			covered++
			if covered > 3 {
				t.Fatalf("walk of n=%d did not finish in 3 chunks", n)
			}
		}
	}
}

// TestBridgeCgoBytesNilAndZero pins the allocation-free edges of cgoBytes:
// a nil pointer or zero length returns nil without touching C memory.
func TestBridgeCgoBytesNilAndZero(t *testing.T) {
	if got := cgoBytes(nil, 0); got != nil {
		t.Fatalf("cgoBytes(nil, 0) = %v, want nil", got)
	}
	if got := cgoBytes(nil, 8); got != nil {
		t.Fatalf("cgoBytes(nil, 8) = %v, want nil", got)
	}
	p := cStringPtr("x")
	defer freeCString(p)
	if got := cgoBytes(unsafe.Pointer(p), 0); got != nil {
		t.Fatalf("cgoBytes(p, 0) = %v, want nil", got)
	}
}
