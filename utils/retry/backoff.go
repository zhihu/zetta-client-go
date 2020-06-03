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
	"math/rand"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// backoff retry
type Backoff struct {
	Init       time.Duration // waiting time for initial retry, default 1s
	Max        time.Duration // max retry wait time, default 30s
	Multiplier float64       // multiply increment factor
	cur        time.Duration // current wait time
}

func (b *Backoff) Pause() time.Duration {
	if b.Init == 0 {
		b.Init = time.Second
	}
	if b.cur == 0 {
		b.cur = b.Init
	}
	if b.Max == 0 {
		b.Max = 30 * time.Second
	}
	if b.Multiplier < 1 {
		b.Multiplier = 2 // default increment by quadratic
	}

	d := time.Duration(1 + rand.Int63n(int64(b.cur))) // suspended randomly
	b.cur = time.Duration(float64(b.cur) * b.Multiplier)
	if b.cur > b.Max {
		b.cur = b.Max // no more increment
	}
	return d
}

type BackoffRetryer struct {
	backoff    Backoff
	validCodes []codes.Code
}

func (br *BackoffRetryer) Retry(err error) (time.Duration, bool) {
	s, ok := status.FromError(err)
	if !ok {
		return 0, false
	}
	code := s.Code()
	for _, validCode := range br.validCodes { // if resp code is able to retry, pause and try again
		if code == validCode {
			return br.backoff.Pause(), true
		}
	}
	return 0, false
}
