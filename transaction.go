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

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// transactionID stores a transaction ID which uniquely identifies a transaction in Cloud Spanner.
type transactionID []byte

// txReadEnv manages a read-transaction environment consisting of a session handle and a transaction selector.
type txReadEnv interface {
	// acquire returns a read-transaction environment that can be used to perform a transactional read.
	acquire(ctx context.Context) (*sessionHandle, *tspb.TransactionSelector, error)
	// release should be called at the end of every transactional read to deal with session recycling and read timestamp recording.
	release(time.Time, error)
}

// txReadOnly contains methods for doing transactional reads.
type txReadOnly struct {
	// read-transaction environment for performing transactional read operations.
	txReadEnv
}

// errSessionClosed returns error for using a recycled/destroyed session
func errSessionClosed(sh *sessionHandle) error {
	return wrapError(codes.FailedPrecondition,
		"session is already recycled / destroyed: session_id = %q, rpc_client = %v", sh.getID(), sh.getClient())
}

// Read reads multiple rows from the database.
//
// The provided function is called once in serial for each row read.  If the
// function returns a non-nil error, Read immediately returns that value.
//
// If no rows are read, Read will return nil without calling the provided
// function.
func (t *txReadOnly) Read(ctx context.Context, table string, keys KeySet, columns []string) *RowIterator {
	// ReadUsingIndex will use primary index if an empty index name is provided.
	return t.ReadUsingIndex(ctx, table, "", keys, columns)
}

// ReadUsingIndex reads multiple rows from the database using an index.
//
// Currently, this function can only read columns that are part of the index
// key, part of the primary key, or stored in the index due to a STORING clause
// in the index definition.
//
// The provided function is called once in serial for each row read. If the
// function returns a non-nil error, ReadUsingIndex immediately returns that
// value.
//
// If no rows are read, ReadUsingIndex will return nil without calling the
// provided function.
func (t *txReadOnly) ReadUsingIndex(ctx context.Context, table, index string, keys KeySet, columns []string) *RowIterator {
	var (
		sh  *sessionHandle
		ts  *tspb.TransactionSelector
		err error
	)
	kset, err := keys.keySetProto()
	if err != nil {
		return &RowIterator{err: err}
	}
	if sh, ts, err = t.acquire(ctx); err != nil {
		return &RowIterator{err: err}
	}
	// Cloud Spanner will return "Session not found" on bad sessions.
	sid, client := sh.getID(), sh.getClient()
	if sid == "" || client == nil {
		// Might happen if transaction is closed in the middle of a API call.
		return &RowIterator{err: errSessionClosed(sh)}
	}
	return stream(
		ctx,
		func(ctx context.Context, resumeToken []byte) (streamingReceiver, error) {
			return client.StreamingRead(ctx,
				&tspb.ReadRequest{
					Session:     sid,
					Transaction: ts,
					Table:       table,
					Index:       index,
					Columns:     columns,
					KeySet:      kset,
					ResumeToken: resumeToken,
				})
		},
		t.release,
	)
}

func (t *txReadOnly) SparseStreamRead(ctx context.Context, table, family string, rows []*SparseRow, limit int64) *RowIterator {
	var (
		sh  *sessionHandle
		ts  *tspb.TransactionSelector
		err error
	)

	if err != nil {
		return &RowIterator{err: err}
	}
	if sh, ts, err = t.acquire(ctx); err != nil {
		return &RowIterator{err: err}
	}
	// Cloud Spanner will return "Session not found" on bad sessions.
	sid, client := sh.getID(), sh.getClient()
	if sid == "" || client == nil {
		// Might happen if transaction is closed in the middle of a API call.
		return &RowIterator{err: errSessionClosed(sh)}
	}

	srows := []*tspb.Row{}
	for _, r := range rows {
		row, err := r.proto()
		if err != nil {
			return &RowIterator{err: err}
		}
		srows = append(srows, row)
	}
	return stream(
		ctx,
		func(ctx context.Context, resumeToken []byte) (streamingReceiver, error) {
			return client.StreamingSparseRead(ctx,
				&tspb.SparseReadRequest{
					Session:     sid,
					Transaction: ts,
					Table:       table,
					Family:      family,
					Rows:        srows,
					Limit:       limit,
				})
		},
		t.release,
	)
}

func (t *txReadOnly) SparseStreamScan(ctx context.Context, table, family string, keySet KeySet, limit int64) *RowIterator {
	var (
		sh  *sessionHandle
		ts  *tspb.TransactionSelector
		err error
	)

	if err != nil {
		return &RowIterator{err: err}
	}
	if sh, ts, err = t.acquire(ctx); err != nil {
		return &RowIterator{err: err}
	}
	// Cloud Spanner will return "Session not found" on bad sessions.
	sid, client := sh.getID(), sh.getClient()
	if sid == "" || client == nil {
		// Might happen if transaction is closed in the middle of a API call.
		return &RowIterator{err: errSessionClosed(sh)}
	}

	keysProto, err := keySet.keySetProto()
	if err != nil {
		return &RowIterator{err: err}
	}

	return stream(
		ctx,
		func(ctx context.Context, resumeToken []byte) (streamingReceiver, error) {
			return client.StreamingSparseScan(ctx,
				&tspb.SparseScanRequest{
					Session:     sid,
					Transaction: ts,
					Table:       table,
					Family:      family,
					KeySet:      keysProto,
					Limit:       limit,
				})
		},
		t.release,
	)
}

// errRowNotFound returns error for not being able to read the row identified by key.
func errRowNotFound(table string, key KeySet) error {
	return wrapError(codes.NotFound, "row not found(Table: %v, PrimaryKey: %v)", table, key)
}

// ReadRow reads a single row from the database.
//
// If no row is present with the given key, then ReadRow returns an error where
// IsRowNotFound(err) is true.
func (t *txReadOnly) ReadRow(ctx context.Context, table string, key KeySet, columns []string) (*Row, error) {
	iter := t.Read(ctx, table, key, columns)
	defer iter.Stop()
	row, err := iter.Next()
	switch err {
	case iterator.Done:
		return nil, errRowNotFound(table, key)
	case nil:
		return row, nil
	default:
		return nil, err
	}
}

// Query executes a query against the database.
//
// The provided function is called once in serial for each row read.  If the
// function returns a non-nil error, Query immediately returns that value.
//
// If no rows are read, Query will return nil without calling the provided
// function.
func (t *txReadOnly) Query(ctx context.Context, statement Statement) *RowIterator {
	var (
		sh  *sessionHandle
		ts  *tspb.TransactionSelector
		err error
	)
	if sh, ts, err = t.acquire(ctx); err != nil {
		return &RowIterator{err: err}
	}
	// Cloud Spanner will return "Session not found" on bad sessions.
	sid, client := sh.getID(), sh.getClient()
	if sid == "" || client == nil {
		// Might happen if transaction is closed in the middle of a API call.
		return &RowIterator{err: errSessionClosed(sh)}
	}
	// req := &tspb.ExecuteSqlRequest{
	// 	Session:     sid,
	// 	Transaction: ts,
	// 	Sql:         statement.SQL,
	// }
	// if err := statement.bindParams(req); err != nil {
	// 	return &RowIterator{err: err}
	// }
	// return stream(
	// 	contextWithMetadata(ctx, sh.getMetadata()),
	// 	func(ctx context.Context, resumeToken []byte) (streamingReceiver, error) {
	// 		req.ResumeToken = resumeToken
	// 		return client.ExecuteStreamingSql(ctx, req)
	// 	},
	// 	t.release)
	_ = ts
	return nil
}

// txState is the status of a transaction.
type txState int

const (
	// transaction is new, waiting to be initialized.
	txNew txState = iota
	// transaction is being initialized.
	txInit
	// transaction is active and can perform read/write.
	txActive
	// transaction is closed, cannot be used anymore.
	txClosed
)

// errRtsUnavailable returns error for read transaction's read timestamp being unavailable.
func errRtsUnavailable() error {
	return wrapError(codes.Internal, "read timestamp is unavailable")
}

// errTxNotInitialized returns error for using an uninitialized transaction.
func errTxNotInitialized() error {
	return wrapError(codes.InvalidArgument, "cannot use a uninitialized transaction")
}

// errTxClosed returns error for using a closed transaction.
func errTxClosed() error {
	return wrapError(codes.InvalidArgument, "cannot use a closed transaction")
}

// errUnexpectedTxState returns error for transaction enters an unexpected state.
func errUnexpectedTxState(ts txState) error {
	return wrapError(codes.FailedPrecondition, "unexpected transaction state: %v", ts)
}
