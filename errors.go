// Copyright 2020 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package zetta

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sdk error wrapper
type Error struct {
	// gRPC status error code
	Code codes.Code
	// more detail info
	Desc string
	// trailers are the trailers returned in the response, if any.
	trailers metadata.MD
}

func (e *Error) Error() string {
	if e != nil {
		return fmt.Sprintf("[code]: %q, [desc]: %q", e.Code, e.Desc)
	}
	return "ok"
}

// wrap a zetta error
func wrapError(ec codes.Code, format string, args ...interface{}) error {
	return &Error{
		Code: ec,
		Desc: fmt.Sprintf(format, args...),
	}
}

// extract error code
func ErrCode(err error) codes.Code {
	se, ok := err.(*Error)
	if !ok {
		return codes.Unknown
	}
	return se.Code
}

// extract error description
func ErrDesc(err error) string {
	se, ok := err.(*Error)
	if !ok {
		return err.Error()
	}
	return se.Desc
}

// decorate decorates an existing spanner.Error with more information.
func (e *Error) decorate(info string) {
	e.Desc = fmt.Sprintf("%v, %v", info, e.Desc)
}

var (
	// mutations
	ERR_MUTATION_EMPTY      = errors.New("empty mutations")
	ERR_MUTATION_DIFF_TABLE = errors.New("mutations not in same table")

	// sessions
	ERR_SESSION_POOL_INVALID = errors.New("current session pool is invalid")
	ERR_SESSION_POOL_TIMEOUT = errors.New("get next session timeout")

	ErrInvalidSessionPool = zettaErrorf(codes.InvalidArgument, "invalid session pool")
	ErrGetSessionTimeout  = zettaErrorf(codes.Canceled, "timeout / context canceled during getting session")
)

// errTrailers extracts the grpc trailers if present from a Go error.
func errTrailers(err error) metadata.MD {
	se, ok := err.(*Error)
	if !ok {
		return nil
	}
	return se.trailers
}

// zettaErrorf generates a *zetta.Error with the given error code and
// description.
func zettaErrorf(ec codes.Code, format string, args ...interface{}) error {
	return &Error{
		Code: ec,
		Desc: fmt.Sprintf(format, args...),
	}
}

// toSpannerError converts general Go error to *spanner.Error.
func toZettaError(err error) error {
	return toZettaErrorWithMetadata(err, nil)
}

// toSpannerErrorWithMetadata converts general Go error and grpc trailers to
// *spanner.Error.
//
// Note: modifies original error if trailers aren't nil.
func toZettaErrorWithMetadata(err error, trailers metadata.MD) error {
	if err == nil {
		return nil
	}
	if se, ok := err.(*Error); ok {
		if trailers != nil {
			se.trailers = metadata.Join(se.trailers, trailers)
		}
		return se
	}
	switch {
	case err == context.DeadlineExceeded:
		return &Error{codes.DeadlineExceeded, err.Error(), trailers}
	case err == context.Canceled:
		return &Error{codes.Canceled, err.Error(), trailers}
	case status.Code(err) == codes.Unknown:
		return &Error{codes.Unknown, err.Error(), trailers}
	default:
		return &Error{status.Code(err), grpc.ErrorDesc(err), trailers}
	}
}
