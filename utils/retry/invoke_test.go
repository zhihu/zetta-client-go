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
	"context"
	"errors"
	"testing"
	"time"
)

type sleepCounter int

func (s *sleepCounter) Sleep(ctx context.Context, d time.Duration) error {
	*s++
	return ctx.Err()
}

func TestInvokeSuccess(t *testing.T) {
	apiFn := func(ctx context.Context, settings CallSettings) error { return nil }
	var settings CallSettings
	var sc sleepCounter
	err := invoke(context.Background(), apiFn, settings, sc.Sleep)
	if err != nil || sc != 0 {
		t.Errorf("err: %v, sc: %d", err, sc)
	}
}

func TestInvokeNilRetry(t *testing.T) {
	apiErr := errors.New("fatal error")
	apiFn := func(ctx context.Context, settings CallSettings) error { return apiErr }
	var settings CallSettings
	var sc sleepCounter
	// WithRetry(func() Retryer { return nil }).Resolve(&settings) // retryer 不可用
	err := invoke(context.Background(), apiFn, settings, sc.Sleep)
	if err != apiErr || sc != 0 {
		t.Errorf("err: %v, sc: %d", err, sc)
	}
}

type boolRetryer bool

func (b boolRetryer) Retry(err error) (time.Duration, bool) {
	return 0, bool(b)
}

func TestInvokeNormalRetry(t *testing.T) {
	tempErr := errors.New("temp error")
	const target = 3
	var retried = 0
	apiFn := func(ctx context.Context, settings CallSettings) error {
		retried++
		if retried < target {
			return tempErr
		}
		return nil
	}
	var settings CallSettings
	var sc sleepCounter
	WithRetry(func() Retryer { return boolRetryer(true) }).Resolve(&settings)

	err := invoke(context.Background(), apiFn, settings, sc.Sleep)
	if err != nil || sc != 2 {
		t.Errorf("err: %v, sc: %d", err, sc)
	}
}

func TestInvokeRetryTimeout(t *testing.T) {
	apiErr := errors.New("fatal error")
	apiFn := func(ctx context.Context, settings CallSettings) error { return apiErr }
	var settings CallSettings
	var sc sleepCounter
	WithRetry(func() Retryer { return boolRetryer(true) }).Resolve(&settings)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := invoke(ctx, apiFn, settings, sc.Sleep)
	if err != context.Canceled || sc != 1 {
		t.Errorf("err: %v, sc: %d", err, sc)
	}
}
