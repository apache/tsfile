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
	"fmt"
	"sync"
)

const defaultMemoryThreshold = 128 * 1024 * 1024

// nativeWriterConfigMu serializes construction because the current native
// writer ABI applies the memory threshold to process-global configuration.
var nativeWriterConfigMu sync.Mutex

type writerOptions struct{ memoryThreshold uint64 }

// WriterOption customizes a writer constructor.
type WriterOption func(*writerOptions) error

// WithMemoryThreshold sets the native writer's in-memory threshold in bytes.
func WithMemoryThreshold(bytes uint64) WriterOption {
	return func(options *writerOptions) error {
		if bytes == 0 {
			return fmt.Errorf("%w: memory threshold must be positive", ErrInvalidArgument)
		}
		options.memoryThreshold = bytes
		return nil
	}
}

// Writer writes tree-model and table-model tablets to one TsFile.
type Writer struct {
	mu     sync.Mutex
	handle *writerHandle
}

// NewWriter creates or truncates a TsFile at path.
func NewWriter(path string, options ...WriterOption) (*Writer, error) {
	settings := writerOptions{memoryThreshold: defaultMemoryThreshold}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil writer option", ErrInvalidArgument)
		}
		if err := option(&settings); err != nil {
			return nil, err
		}
	}
	nativeWriterConfigMu.Lock()
	defer nativeWriterConfigMu.Unlock()
	handle, err := newWriterHandle(path, settings.memoryThreshold)
	if err != nil {
		return nil, err
	}
	return &Writer{handle: handle}, nil
}

func (w *Writer) withHandle(fn func(*writerHandle) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil || w.handle.ptr == nil {
		return ErrClosed
	}
	return fn(w.handle)
}

// RegisterTable registers a table-model schema.
func (w *Writer) RegisterTable(schema TableSchema) error {
	if err := validateCString("register table", "table name", schema.Table); err != nil {
		return err
	}
	if len(schema.Columns) == 0 {
		return fmt.Errorf("%w: table requires at least one column", ErrInvalidSchema)
	}
	for i, column := range schema.Columns {
		if err := validateCString("register table", "column name", column.Name); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
		if !validDataType(column.DataType) || !validColumnCategory(column.Category) {
			return fmt.Errorf("%w: invalid column %q", ErrInvalidSchema, column.Name)
		}
	}
	return w.withHandle(func(h *writerHandle) error { return h.registerTable(schema) })
}

// RegisterTimeseries registers one tree-model measurement for a device.
func (w *Writer) RegisterTimeseries(device string, schema TimeseriesSchema) error {
	if err := validateCString("register timeseries", "device", device); err != nil {
		return err
	}
	if err := validateTimeseriesSchema("register timeseries", schema); err != nil {
		return err
	}
	return w.withHandle(func(h *writerHandle) error { return h.registerTimeseries(device, schema) })
}

// RegisterDevice registers all tree-model measurements for a device.
func (w *Writer) RegisterDevice(schema DeviceSchema) error {
	if err := validateCString("register device", "device", schema.Device); err != nil {
		return err
	}
	if len(schema.TimeSeries) == 0 {
		return fmt.Errorf("%w: device requires at least one timeseries", ErrInvalidSchema)
	}
	for i, series := range schema.TimeSeries {
		if err := validateTimeseriesSchema("register device", series); err != nil {
			return fmt.Errorf("timeseries %d: %w", i, err)
		}
	}
	return w.withHandle(func(h *writerHandle) error { return h.registerDevice(schema) })
}

func (w *Writer) writeTablet(tablet *Tablet, tableModel bool) error {
	if tablet == nil {
		return fmt.Errorf("%w: nil tablet", ErrInvalidArgument)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil || w.handle.ptr == nil {
		return ErrClosed
	}
	tablet.mu.Lock()
	defer tablet.mu.Unlock()
	if tablet.handle == nil || tablet.handle.ptr == nil {
		return ErrClosed
	}
	if tableModel {
		return w.handle.writeTableTablet(tablet.handle)
	}
	return w.handle.writeTreeTablet(tablet.handle)
}

// WriteTreeTablet writes a tree-model tablet without consuming it.
func (w *Writer) WriteTreeTablet(tablet *Tablet) error { return w.writeTablet(tablet, false) }

// WriteTableTablet writes a table-model tablet without consuming it.
func (w *Writer) WriteTableTablet(tablet *Tablet) error { return w.writeTablet(tablet, true) }

// AddProperty adds a binary TsFile property. Empty values are rejected because
// the native ABI represents a nil pointer as a NULL property, not an empty
// byte string.
func (w *Writer) AddProperty(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("%w: property key must not be empty", ErrInvalidArgument)
	}
	if len(value) == 0 {
		return fmt.Errorf("%w: property value must not be empty", ErrInvalidArgument)
	}
	return w.withHandle(func(h *writerHandle) error { return h.addProperty(key, value) })
}

// Flush writes buffered data without closing the writer.
func (w *Writer) Flush() error {
	return w.withHandle(func(h *writerHandle) error { return h.flush() })
}

// Close flushes and releases the writer. It is idempotent.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil {
		return nil
	}
	return w.handle.close()
}
