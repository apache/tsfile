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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

package tsfile

import (
	"errors"
	"fmt"
)

// Error represents a TsFile error code paired with an optional operation.
type Error struct {
	Code int32
	Op   string
}

func (e *Error) Error() string {
	message := errorCodeMessage(e.Code)
	if e.Op != "" {
		return e.Op + ": " + message
	}
	return message
}

// Is compares TsFile errors by numeric code.
func (e *Error) Is(target error) bool {
	targetError, ok := target.(*Error)
	return ok && e.Code == targetError.Code
}

func newError(op string, code int32) error {
	if code == 0 {
		return nil
	}
	return &Error{Code: code, Op: op}
}

var (
	ErrInvalidArgument     = &Error{Code: 4}
	ErrOutOfRange          = &Error{Code: 5}
	ErrOverflow            = &Error{Code: 20}
	ErrInvalidSchema       = &Error{Code: 8}
	ErrTypeNotSupported    = &Error{Code: 26}
	ErrTypeMismatch        = &Error{Code: 27}
	ErrFileOpen            = &Error{Code: 28}
	ErrFileClose           = &Error{Code: 29}
	ErrFileWrite           = &Error{Code: 30}
	ErrFileRead            = &Error{Code: 31}
	ErrInvalidPath         = &Error{Code: 37}
	ErrDeviceNotExist      = &Error{Code: 44}
	ErrMeasurementNotExist = &Error{Code: 45}
	ErrTableNotExist       = &Error{Code: 49}
	ErrColumnNotExist      = &Error{Code: 50}

	ErrClosed    = errors.New("tsfile: closed")
	ErrNullValue = errors.New("tsfile: null value")
)

func errorCodeMessage(code int32) string {
	switch code {
	case 0:
		return "ok"
	case 1:
		return "out of memory"
	case 2:
		return "does not exist"
	case 3:
		return "already exists"
	case 4:
		return "invalid argument"
	case 5:
		return "out of range"
	case 6:
		return "partial read"
	case 8:
		return "invalid schema"
	case 20:
		return "overflow"
	case 21:
		return "no more data"
	case 22:
		return "out of order"
	case 24:
		return "TsBlock data inconsistency"
	case 26:
		return "type not supported"
	case 27:
		return "type mismatch"
	case 28:
		return "file open failed"
	case 29:
		return "file close failed"
	case 30:
		return "file write failed"
	case 31:
		return "file read failed"
	case 32:
		return "file sync failed"
	case 33:
		return "TsFile writer metadata error"
	case 34:
		return "file stat failed"
	case 35:
		return "TsFile corrupted"
	case 36:
		return "buffer not large enough"
	case 37:
		return "invalid path"
	case 38:
		return "does not match"
	case 40:
		return "operation not supported"
	case 43:
		return "invalid data point"
	case 44:
		return "device does not exist"
	case 45:
		return "measurement does not exist"
	case 48:
		return "compression failed"
	case 49:
		return "table does not exist"
	case 50:
		return "column does not exist"
	case 51:
		return "unsupported order"
	case 52:
		return "invalid node type"
	case 53:
		return "encoding failed"
	case 54:
		return "decoding failed"
	default:
		return fmt.Sprintf("unknown error code %d", code)
	}
}
