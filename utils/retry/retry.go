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

package retry

import (
	"time"

	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
)

type CallOption interface {
	Resolve(cs *CallSettings)
}

type Retryer interface {
	Retry(err error) (pause time.Duration, shouldRetry bool)
}

type CallSettings struct {
	Retry func() Retryer
	GRPC  []grpc.CallOption
}

type retryerOption func() Retryer

func WithRetry(f func() Retryer) CallOption {
	return retryerOption(f)
}

func (ro retryerOption) Resolve(cs *CallSettings) {
	cs.Retry = ro
}

type grpcOptions []grpc.CallOption

func (o grpcOptions) Resolve(cs *CallSettings) {
	cs.GRPC = o
}

func WithGRPCOptions(options ...grpc.CallOption) CallOption {
	return grpcOptions(append([]grpc.CallOption(nil), options...))
}

func GetBackoffRetryer(codes []codes.Code, bo Backoff) Retryer {
	return &BackoffRetryer{
		backoff:    bo,
		validCodes: codes,
	}
}
