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
	"sync"
	"time"

	"github.com/zhihu/zetta-client-go/utils/retry"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ReadWriteTransaction provides a locking read-write transaction.
//
// This type of transaction is the only way to write data into Cloud Spanner;
// (*Client).Apply and (*Client).ApplyAtLeastOnce use transactions
// internally. These transactions rely on pessimistic locking and, if
// necessary, two-phase commit. Locking read-write transactions may abort,
// requiring the application to retry. However, the interface exposed by
// (*Client).ReadWriteTransaction eliminates the need for applications to write
// retry loops explicitly.
//
// Locking transactions may be used to atomically read-modify-write data
// anywhere in a database. This type of transaction is externally consistent.
//
// Clients should attempt to minimize the amount of time a transaction is
// active. Faster transactions commit with higher probability and cause less
// contention. Cloud Spanner attempts to keep read locks active as long as the
// transaction continues to do reads.  Long periods of inactivity at the client
// may cause Cloud Spanner to release a transaction's locks and abort it.
//
// Reads performed within a transaction acquire locks on the data being
// read. Writes can only be done at commit time, after all reads have been
// completed. Conceptually, a read-write transaction consists of zero or more
// reads or SQL queries followed by a commit.
//
// See (*Client).ReadWriteTransaction for an example.
//
// Semantics
//
// Cloud Spanner can commit the transaction if all read locks it acquired are still
// valid at commit time, and it is able to acquire write locks for all
// writes. Cloud Spanner can abort the transaction for any reason. If a commit
// attempt returns ABORTED, Cloud Spanner guarantees that the transaction has not
// modified any user data in Cloud Spanner.
//
// Unless the transaction commits, Cloud Spanner makes no guarantees about how long
// the transaction's locks were held for. It is an error to use Cloud Spanner locks
// for any sort of mutual exclusion other than between Cloud Spanner transactions
// themselves.
//
// Aborted transactions
//
// Application code does not need to retry explicitly; RunInTransaction will
// automatically retry a transaction if an attempt results in an abort. The
// lock priority of a transaction increases after each prior aborted
// transaction, meaning that the next attempt has a slightly better chance of
// success than before.
//
// Under some circumstances (e.g., many transactions attempting to modify the
// same row(s)), a transaction can abort many times in a short period before
// successfully committing. Thus, it is not a good idea to cap the number of
// retries a transaction can attempt; instead, it is better to limit the total
// amount of wall time spent retrying.
//
// Idle transactions
//
// A transaction is considered idle if it has no outstanding reads or SQL
// queries and has not started a read or SQL query within the last 10
// seconds. Idle transactions can be aborted by Cloud Spanner so that they don't hold
// on to locks indefinitely. In that case, the commit will fail with error
// ABORTED.
//
// If this behavior is undesirable, periodically executing a simple SQL query
// in the transaction (e.g., SELECT 1) prevents the transaction from becoming
// idle.
type ReadWriteTransaction struct {
	// txReadOnly contains methods for performing transactional reads.
	txReadOnly
	// sh is the sessionHandle allocated from sp. It is set only once during the initialization of ReadWriteTransaction.
	sh *sessionHandle
	// tx is the transaction ID in Cloud Spanner that uniquely identifies the ReadWriteTransaction.
	// It is set only once in ReadWriteTransaction.begin() during the initialization of ReadWriteTransaction.
	tx transactionID
	// mu protects concurrent access to the internal states of ReadWriteTransaction.
	mu sync.Mutex
	// state is the current transaction status of the read-write transaction.
	state txState
	// wb is the set of buffered mutations waiting to be commited.
	wb []*Mutation
}

// BufferWrite adds a list of mutations to the set of updates that will be
// applied when the transaction is committed. It does not actually apply the
// write until the transaction is committed, so the operation does not
// block. The effects of the write won't be visible to any reads (including
// reads done in the same transaction) until the transaction commits.
//
// See the example for Client.ReadWriteTransaction.
func (t *ReadWriteTransaction) BufferWrite(ms []*Mutation) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == txClosed {
		return errTxClosed()
	}
	if t.state != txActive {
		return errUnexpectedTxState(t.state)
	}
	t.wb = append(t.wb, ms...)
	return nil
}

// acquire implements txReadEnv.acquire.
func (t *ReadWriteTransaction) acquire(ctx context.Context) (*sessionHandle, *tspb.TransactionSelector, error) {
	ts := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_Id{
			Id: t.tx,
		},
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.state {
	case txClosed:
		return nil, nil, errTxClosed()
	case txActive:
		return t.sh, ts, nil
	}
	return nil, nil, errUnexpectedTxState(t.state)
}

// release implements txReadEnv.release.
func (t *ReadWriteTransaction) release(_ time.Time, err error) {
	t.mu.Lock()
	sh := t.sh
	t.mu.Unlock()
	if sh != nil && shouldDropSession(err) {
		sh.destroy()
	}
}

func beginTransaction(ctx context.Context, sid string, client tspb.TablestoreClient) (transactionID, error) {
	var tx transactionID
	err := retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		res, e := client.BeginTransaction(ctx, &tspb.BeginTransactionRequest{
			Session: sid,
			Options: &tspb.TransactionOptions{
				Mode: &tspb.TransactionOptions_ReadWrite_{
					ReadWrite: &tspb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if e != nil {
			return e
		}
		tx = res.Id
		return nil
	})

	if err != nil {
		return nil, err
	}
	return tx, nil
}

// begin starts a read-write transacton on Cloud Spanner, it is always called before any of the public APIs.
func (t *ReadWriteTransaction) begin(ctx context.Context) error {
	if t.tx != nil {
		t.state = txActive
		return nil
	}
	tx, err := beginTransaction(ctx, t.sh.getID(), t.sh.getClient())
	if err == nil {
		t.tx = tx
		t.state = txActive
		return nil
	}
	if shouldDropSession(err) {
		t.sh.destroy()
	}
	return err
}

// commit tries to commit a readwrite transaction to Cloud Spanner. It also returns the commit timestamp for the transactions.
func (t *ReadWriteTransaction) commit(ctx context.Context) (time.Time, error) {
	var ts time.Time
	t.mu.Lock()
	t.state = txClosed // No futher operations after commit.
	mPb, err := mutationsProto(t.wb)
	t.mu.Unlock()
	if err != nil {
		return ts, err
	}
	// In case that sessionHandle was destroyed but transaction body fails to report it.
	sid, client := t.sh.getID(), t.sh.getClient()
	if sid == "" || client == nil {
		return ts, errSessionClosed(t.sh)
	}
	err = retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		var trailer metadata.MD
		res, e := client.Commit(ctx, &tspb.CommitRequest{
			Session: sid,
			Transaction: &tspb.CommitRequest_TransactionId{
				TransactionId: t.tx,
			},
			Mutations: mPb,
		}, grpc.Trailer(&trailer))
		if e != nil {
			return e
		}
		if tstamp := res.GetCommitTimestamp(); tstamp != nil {
			ts = time.Unix(tstamp.Seconds, int64(tstamp.Nanos))
		}
		return nil
	})
	if shouldDropSession(err) {
		t.sh.destroy()
	}
	return ts, err
}

// rollback is called when a commit is aborted or the transaction body runs into error.
func (t *ReadWriteTransaction) rollback(ctx context.Context) {
	t.mu.Lock()
	// Forbid further operations on rollbacked transaction.
	t.state = txClosed
	t.mu.Unlock()
	// In case that sessionHandle was destroyed but transaction body fails to report it.
	sid, client := t.sh.getID(), t.sh.getClient()
	if sid == "" || client == nil {
		return
	}
	err := retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, e := client.Rollback(ctx, &tspb.RollbackRequest{
			Session:       sid,
			TransactionId: t.tx,
		})
		return e
	})
	if shouldDropSession(err) {
		t.sh.destroy()
	}
	return
}

// runInTransaction executes f under a read-write transaction context.
func (t *ReadWriteTransaction) runInTransaction(ctx context.Context, f func(t *ReadWriteTransaction) error) (time.Time, error) {
	var (
		ts  time.Time
		err error
	)
	if err = f(t); err == nil {
		// Try to commit if transaction body returns no error.
		ts, err = t.commit(ctx)
	}
	if err != nil {
		if isAbortErr(err) {
			// Retry the transaction using the same session on ABORT error.
			// Cloud Spanner will create the new transaction with the previous one's wound-wait priority.
			return ts, err
		}
		// Not going to commit, according to API spec, should rollback the transaction.
		t.rollback(ctx)
		return ts, err
	}
	// err == nil, return commit timestamp.
	return ts, err
}
