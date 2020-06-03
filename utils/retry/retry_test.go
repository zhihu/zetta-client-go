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
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackoff(t *testing.T) {
	f := func(ds []time.Duration) []time.Duration {
		for i, d := range ds {
			ds[i] = d * time.Second
		}
		return ds
	}

	// normal case
	b1 := Backoff{}
	maxs1 := f([]time.Duration{1, 2, 4, 8, 16, 30, 30, 30})
	for i, max := range maxs1 {
		d := b1.Pause()
		if d > max {
			t.Errorf("backoff too long, most %s, but got %s", max, d)
		}
		if i < len(maxs1)-1 && b1.cur != maxs1[i+1] {
			t.Errorf("current pause, want %s, but got %s", maxs1[i+1], b1.cur)
		}
	}

	// defined case
	b2 := Backoff{
		Init:       1 * time.Second,
		Max:        20 * time.Second,
		Multiplier: 2,
	}
	maxs2 := f([]time.Duration{1, 2, 4, 8, 16, 20, 20, 20})
	for _, max := range maxs2 {
		if d := b2.Pause(); d > max {
			t.Errorf("backoff too long, most %s, but got %s", max, d)
		}
	}
}

func TestOnCodes(t *testing.T) {
	respErr := status.Errorf(codes.Unavailable, "")
	cases := []struct {
		codes   []codes.Code
		isRetry bool
	}{
		{nil, false},
		{[]codes.Code{codes.DeadlineExceeded}, false},                   // timeout
		{[]codes.Code{codes.DeadlineExceeded, codes.Unavailable}, true}, // Unavailable is able for retry
		{[]codes.Code{codes.Unavailable}, true},
	}

	for _, c := range cases {
		retryer := GetBackoffRetryer(c.codes, Backoff{})
		if _, ok := retryer.Retry(respErr); ok != c.isRetry {
			t.Errorf("retrying codes: %v, error: %s, retried: %t, want: %t", c.codes, respErr, ok, c.isRetry)
		}
	}
}
