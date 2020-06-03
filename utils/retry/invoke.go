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
	"strings"
	"time"
)

type APIFunc func(context.Context, CallSettings) error

func Invoke(ctx context.Context, fn APIFunc, options ...CallOption) error {
	var settings CallSettings
	for _, opt := range options {
		opt.Resolve(&settings) // modify settings' filed
	}
	return invoke(ctx, fn, settings, Sleep)
}

type sleeper func(ctx context.Context, d time.Duration) error

func invoke(ctx context.Context, apiFn APIFunc, settings CallSettings, sleepFn sleeper) error {
	var retryer Retryer
	for {
		err := apiFn(ctx, settings)
		if err == nil {
			return nil // call successfully
		}
		if settings.Retry == nil { // no retry action
			return err
		}
		if strings.Contains(err.Error(), "Error while dialing dial tcp") {
			return err
		}
		if retryer == nil {
			if r := settings.Retry(); r == nil {
				return err
			} else {
				retryer = r
			}
		}
		if pause, ok := retryer.Retry(err); !ok {
			return err
		} else if err = sleepFn(ctx, pause); err != nil {
			return err
		}
	}
}

func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
