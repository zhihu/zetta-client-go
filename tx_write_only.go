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
	"time"

	"github.com/zhihu/zetta-client-go/utils/retry"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// writeOnlyTransaction provides the most efficient way of doing write-only transactions. It essentially does blind writes to Cloud Spanner.
type writeOnlyTransaction struct {
	// sp is the session pool which writeOnlyTransaction uses to get Cloud Spanner sessions for blind writes.
	sp *sessionPool
}

// applyAtLeastOnce commits a list of mutations to Cloud Spanner for at least once, unless one of the following happends:
//     1) Context is timeout.
//     2) An unretryable error(e.g. database not found) occurs.
//     3) There is a malformed Mutation object.
func (t *writeOnlyTransaction) applyAtLeastOnce(ctx context.Context, ms ...*Mutation) (time.Time, error) {
	var (
		ts time.Time
		sh *sessionHandle
	)

	table := ms[0].table
	mPb, err := mutationsProto(ms)
	if err != nil {
		// Malformed mutation found, just return the error.
		return ts, err
	}
	err = retry.Invoke(ctx, func(ct context.Context, settings retry.CallSettings) error {
		var e error
		var trailers metadata.MD

		if sh == nil || sh.getID() == "" || sh.getClient() == nil {
			// No usable session for doing the commit, take one from pool.
			sh, e = t.sp.take(ctx)
			if e != nil {
				// sessionPool.Take already retries for session creations/retrivals.
				return e
			}
		}

		res, e := sh.getClient().Commit(ctx, &tspb.CommitRequest{
			Session: sh.getID(),
			Table:   table,
			Transaction: &tspb.CommitRequest_SingleUseTransaction{
				SingleUseTransaction: &tspb.TransactionOptions{
					Mode: &tspb.TransactionOptions_ReadWrite_{
						ReadWrite: &tspb.TransactionOptions_ReadWrite{},
					},
				},
			},
			Mutations: mPb,
		}, grpc.Trailer(&trailers))
		if e != nil {
			if isAbortErr(e) {
				// Mask ABORT error as retryable, because aborted transactions are allowed to be retried.
				return e
			}
			if shouldDropSession(e) {
				// Discard the bad session.
				sh.destroy()
			}
			return e
		}
		if tstamp := res.GetCommitTimestamp(); tstamp != nil {
			ts = time.Unix(tstamp.Seconds, int64(tstamp.Nanos))
		}
		return nil
	})
	if sh != nil {
		sh.recycle()
	}
	return ts, err
}

// isAbortedErr returns true if the error indicates that an gRPC call is aborted on the server side.
func isAbortErr(err error) bool {
	if err == nil {
		return false
	}
	if ErrCode(err) == codes.Aborted {
		return true
	}
	return false
}
