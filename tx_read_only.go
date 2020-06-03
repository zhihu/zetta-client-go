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

/*
Copyright 2017 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zetta

import (
	"sync"
	"time"

	"github.com/zhihu/zetta-client-go/utils/retry"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

// ReadOnlyTransaction provides a snapshot transaction with guaranteed
// consistency across reads, but does not allow writes.  Read-only
// transactions can be configured to read at timestamps in the past.
//
// Read-only transactions do not take locks. Instead, they work by choosing a
// Cloud Spanner timestamp, then executing all reads at that timestamp. Since they do
// not acquire locks, they do not block concurrent read-write transactions.
//
// Unlike locking read-write transactions, read-only transactions never
// abort. They can fail if the chosen read timestamp is garbage collected;
// however, the default garbage collection policy is generous enough that most
// applications do not need to worry about this in practice. See the
// documentation of TimestampBound for more details.
//
// A ReadOnlyTransaction consumes resources on the server until Close() is
// called.
type ReadOnlyTransaction struct {
	// txReadOnly contains methods for performing transactional reads.
	txReadOnly

	// singleUse indicates that the transaction can be used for only one read.
	singleUse bool

	// sp is the session pool for allocating a session to execute the read-only transaction. It is set only once during initialization of the ReadOnlyTransaction.
	sp *sessionPool
	// mu protects concurrent access to the internal states of ReadOnlyTransaction.
	mu sync.Mutex
	// tx is the transaction ID in Cloud Spanner that uniquely identifies the ReadOnlyTransaction.
	tx transactionID
	// txReadyOrClosed is for broadcasting that transaction ID has been returned by Cloud Spanner or that transaction is closed.
	txReadyOrClosed chan struct{}
	// state is the current transaction status of the ReadOnly transaction.
	state txState
	// sh is the sessionHandle allocated from sp.
	sh *sessionHandle
	// rts is the read timestamp returned by transactional reads.
	rts time.Time
	// tb is the read staleness bound specification for transactional reads.
	tb TimestampBound
}

// errTxInitTimeout returns error for timeout in waiting for initialization of the transaction.
func errTxInitTimeout() error {
	return wrapError(codes.Canceled, "timeout/context canceled in waiting for transaction's initialization")
}

// getTimestampBound returns the read staleness bound specified for the ReadOnlyTransaction.
func (t *ReadOnlyTransaction) getTimestampBound() TimestampBound {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tb
}

// begin starts a snapshot read-only Transaction on Cloud Spanner.
func (t *ReadOnlyTransaction) begin(ctx context.Context) error {
	var (
		locked bool
		tx     transactionID
		rts    time.Time
		sh     *sessionHandle
		err    error
	)
	defer func() {
		if !locked {
			t.mu.Lock()
			// Not necessary, just to make it clear that t.mu is being held when locked == true.
			locked = true
		}
		if t.state != txClosed {
			// Signal other initialization routines.
			close(t.txReadyOrClosed)
			t.txReadyOrClosed = make(chan struct{})
		}
		t.mu.Unlock()
		if err != nil && sh != nil {
			// Got a valid session handle, but failed to initalize transaction on Cloud Spanner.
			if shouldDropSession(err) {
				sh.destroy()
			}
			// If sh.destroy was already executed, this becomes a noop.
			sh.recycle()
		}
	}()
	sh, err = t.sp.take(ctx)
	if err != nil {
		return err
	}
	err = retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		res, e := sh.getClient().BeginTransaction(ctx, &tspb.BeginTransactionRequest{
			Session: sh.getID(),
			Options: &tspb.TransactionOptions{
				Mode: &tspb.TransactionOptions_ReadOnly_{
					ReadOnly: buildTransactionOptionsReadOnly(t.getTimestampBound(), true),
				},
			},
		})
		if e != nil {
			return e
		}
		tx = res.Id
		if res.ReadTimestamp != nil {
			rts = time.Unix(res.ReadTimestamp.Seconds, int64(res.ReadTimestamp.Nanos))
		}
		return nil
	})
	t.mu.Lock()
	locked = true            // defer function will be executed with t.mu being held.
	if t.state == txClosed { // During the execution of t.begin(), t.Close() was invoked.
		return errSessionClosed(sh)
	}
	// If begin() fails, this allows other queries to take over the initialization.
	t.tx = nil
	if err == nil {
		t.tx = tx
		t.rts = rts
		t.sh = sh
		// State transite to txActive.
		t.state = txActive
	}
	return err
}

// acquire implements txReadEnv.acquire.
func (t *ReadOnlyTransaction) acquire(ctx context.Context) (*sessionHandle, *tspb.TransactionSelector, error) {
	if t.singleUse {
		return t.acquireSingleUse(ctx)
	}
	return t.acquireMultiUse(ctx)
}

func (t *ReadOnlyTransaction) acquireSingleUse(ctx context.Context) (*sessionHandle, *tspb.TransactionSelector, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.state {
	case txClosed:
		// A closed single-use transaction can never be reused.
		return nil, nil, errTxClosed()
	case txNew:
		t.state = txClosed
		ts := &tspb.TransactionSelector{
			Selector: &tspb.TransactionSelector_SingleUse{
				SingleUse: &tspb.TransactionOptions{
					Mode: &tspb.TransactionOptions_ReadOnly_{
						ReadOnly: buildTransactionOptionsReadOnly(t.tb, true),
					},
				},
			},
		}
		sh, err := t.sp.take(ctx)
		if err != nil {
			return nil, nil, err
		}
		// Install session handle into t, which can be used for readonly operations later.
		t.sh = sh
		return sh, ts, nil
	}
	us := t.state
	// SingleUse transaction should only be in either txNew state or txClosed state.
	return nil, nil, errUnexpectedTxState(us)
}

func (t *ReadOnlyTransaction) acquireMultiUse(ctx context.Context) (*sessionHandle, *tspb.TransactionSelector, error) {
	for {
		t.mu.Lock()
		switch t.state {
		case txClosed:
			t.mu.Unlock()
			return nil, nil, errTxClosed()
		case txNew:
			// State transit to txInit so that no further TimestampBound change is accepted.
			t.state = txInit
			t.mu.Unlock()
			continue
		case txInit:
			if t.tx != nil {
				// Wait for a transaction ID to become ready.
				txReadyOrClosed := t.txReadyOrClosed
				t.mu.Unlock()
				select {
				case <-txReadyOrClosed:
					// Need to check transaction state again.
					continue
				case <-ctx.Done():
					// The waiting for initialization is timeout, return error directly.
					return nil, nil, errTxInitTimeout()
				}
			}
			// Take the ownership of initializing the transaction.
			t.tx = transactionID{}
			t.mu.Unlock()
			// Begin a read-only transaction.
			// TODO: consider adding a transaction option which allow queries to initiate transactions by themselves. Note that this option might not be
			// always good because the ID of the new transaction won't be ready till the query returns some data or completes.
			if err := t.begin(ctx); err != nil {
				return nil, nil, err
			}
			// If t.begin() succeeded, t.state should have been changed to txActive, so we can just continue here.
			continue
		case txActive:
			sh := t.sh
			ts := &tspb.TransactionSelector{
				Selector: &tspb.TransactionSelector_Id{
					Id: t.tx,
				},
			}
			t.mu.Unlock()
			return sh, ts, nil
		}
		state := t.state
		t.mu.Unlock()
		return nil, nil, errUnexpectedTxState(state)
	}
}

// release implements txReadEnv.release.
func (t *ReadOnlyTransaction) release(rts time.Time, err error) {
	t.mu.Lock()
	if t.singleUse && !rts.IsZero() {
		t.rts = rts
	}
	sh := t.sh
	t.mu.Unlock()
	if sh != nil { // sh could be nil if t.acquire() fails.
		if shouldDropSession(err) {
			sh.destroy()
		}
		if t.singleUse {
			// If session handle is already destroyed, this becomes a noop.
			sh.recycle()
		}
	}
}

// Close closes a ReadOnlyTransaction, the transaction cannot perform any reads after being closed.
func (t *ReadOnlyTransaction) Close() {
	if t.singleUse {
		return
	}
	t.mu.Lock()
	if t.state != txClosed {
		t.state = txClosed
		close(t.txReadyOrClosed)
	}
	sh := t.sh
	t.mu.Unlock()
	// If session handle is already destroyed, this becomes a noop.
	// If there are still active queries and if the recycled session is reused before they complete, Cloud Spanner will cancel them
	// on behalf of the new transaction on the session.
	sh.recycle()
}

// Timestamp returns the timestamp chosen to perform reads and
// queries in this transaction. The value can only be read after some
// read or query has either returned some data or completed without
// returning any data.
func (t *ReadOnlyTransaction) Timestamp() (time.Time, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rts.IsZero() {
		return t.rts, errRtsUnavailable()
	}
	return t.rts, nil
}

// WithTimestampBound specifies the TimestampBound to use for read or query.
// This can only be used before the first read or query is invoked. Note:
// bounded staleness is not available with general ReadOnlyTransactions; use a
// single-use ReadOnlyTransaction instead.
//
// The returned value is the ReadOnlyTransaction so calls can be chained.
func (t *ReadOnlyTransaction) WithTimestampBound(tb TimestampBound) *ReadOnlyTransaction {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == txNew {
		// Only allow to set TimestampBound before the first query.
		t.tb = tb
	}
	return t
}
