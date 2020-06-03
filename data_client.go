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
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/dns"

	"github.com/zhihu/zetta-client-go/utils/retry"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

const (
	SPARSE_READ = "sparseread"

	// numChannels is the default value for NumChannels of client
	numChannels = 4
)

func init() {
	resolver.Register(dns.NewBuilder())
}

type DataClient struct {
	database string

	// multi clients and sessions
	sp      *sessionPool
	counter uint32
	conns   []*grpc.ClientConn
	clients []tspb.TablestoreClient
}

type DataClientConfig struct {
	NumChannels int // 并发度，对应 session pool
	SessionPoolConfig
}

func NewDataClient(ctx context.Context, serverAddr, dbName string, conf DataClientConfig) (*DataClient, error) {
	// batch init clients
	if conf.NumChannels == 0 {
		conf.NumChannels = numChannels
	}
	// Default configs for session pool.
	if conf.MaxOpened == 0 {
		conf.MaxOpened = uint64(conf.NumChannels * 100)
	}
	if conf.MaxBurst == 0 {
		conf.MaxBurst = DefaultSessionPoolConfig.MaxBurst
	}

	dc := &DataClient{database: dbName}

	dialOpts := []grpc.DialOption{grpc.WithInsecure()}

	rsTarget := ParseTarget(serverAddr)
	if strings.ToLower(rsTarget.Scheme) == "dns" {
		dialOpts = append(dialOpts, grpc.WithBalancerName("round_robin"))
	}
	for i := 0; i < conf.NumChannels; i++ {

		conn, err := grpc.Dial(serverAddr, dialOpts...)
		if err != nil {
			return nil, err
		}
		dc.conns = append(dc.conns, conn)
		dc.clients = append(dc.clients, tspb.NewTablestoreClient(conn))
	}

	// fetch next valid client
	conf.SessionPoolConfig.getNextDataRPCClient = func() (client tspb.TablestoreClient, err error) {
		i := atomic.AddUint32(&dc.counter, 1) % uint32(len(dc.clients))
		return dc.clients[i], nil
	}

	sp, err := newSessionPool(dbName, conf.SessionPoolConfig)
	if err != nil {
		return nil, err
	}
	dc.sp = sp
	return dc, nil
}

// Close dataclient close
func (dc *DataClient) Close() {
	if dc.sp != nil {
		dc.sp.close()
	}
	for _, c := range dc.conns {
		c.Close()
	}
}

// Single provides a read-only snapshot transaction optimized for the case
// where only a single read or query is needed.  This is more efficient than
// using ReadOnlyTransaction() for a single read or query.
//
// Single will use a strong TimestampBound by default. Use
// ReadOnlyTransaction.WithTimestampBound to specify a different
// TimestampBound. A non-strong bound can be used to reduce latency, or
// "time-travel" to prior versions of the database, see the documentation of
// TimestampBound for details.
func (dc *DataClient) Single() *ReadOnlyTransaction {
	t := &ReadOnlyTransaction{singleUse: true, sp: dc.sp}
	t.txReadOnly.txReadEnv = t
	return t
}

// ReadOnlyTransaction returns a ReadOnlyTransaction that can be used for
// multiple reads from the database.  You must call Close() when the
// ReadOnlyTransaction is no longer needed to release resources on the server.
//
// ReadOnlyTransaction will use a strong TimestampBound by default.  Use
// ReadOnlyTransaction.WithTimestampBound to specify a different
// TimestampBound.  A non-strong bound can be used to reduce latency, or
// "time-travel" to prior versions of the database, see the documentation of
// TimestampBound for details.
func (dc *DataClient) ReadOnlyTransaction() *ReadOnlyTransaction {
	t := &ReadOnlyTransaction{
		singleUse:       false,
		sp:              dc.sp,
		txReadyOrClosed: make(chan struct{}),
	}
	t.txReadOnly.txReadEnv = t
	return t
}

// ReadWriteTransaction executes a read-write transaction, with retries as
// necessary.
//
// The function f will be called one or more times. It must not maintain
// any state between calls.
//
// If the transaction cannot be committed or if f returns an IsAborted error,
// ReadWriteTransaction will call f again. It will continue to call f until the
// transaction can be committed or the Context times out or is cancelled.  If f
// returns an error other than IsAborted, ReadWriteTransaction will abort the
// transaction and return the error.
//
// To limit the number of retries, set a deadline on the Context rather than
// using a fixed limit on the number of attempts. ReadWriteTransaction will
// retry as needed until that deadline is met.
func (dc *DataClient) ReadWriteTransaction(ctx context.Context, f func(t *ReadWriteTransaction) error) (time.Time, error) {
	var (
		ts time.Time
		sh *sessionHandle
	)
	err := retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		var (
			err error
			t   *ReadWriteTransaction
		)
		if sh == nil || sh.getID() == "" || sh.getClient() == nil {
			// Session handle hasn't been allocated or has been destroyed.
			sh, err = dc.sp.takeWriteSession(ctx)
			if err != nil {
				// If session retrieval fails, just fail the transaction.
				return err
			}
			t = &ReadWriteTransaction{
				sh: sh,
				tx: sh.getTransactionID(),
			}
		} else {
			t = &ReadWriteTransaction{
				sh: sh,
			}
		}
		t.txReadOnly.txReadEnv = t
		if err = t.begin(ctx); err != nil {
			// Mask error from begin operation as retryable error.
			return err
		}
		ts, err = t.runInTransaction(ctx, f)
		if err != nil {
			return err
		}
		return nil
	})
	if sh != nil {
		sh.recycle()
	}
	return ts, err
}

// applyOption controls the behavior of Client.Apply.
type applyOption struct {
	// If atLeastOnce == true, Client.Apply will execute the mutations on Cloud Spanner at least once.
	atLeastOnce bool
}

// An ApplyOption is an optional argument to Apply.
type ApplyOption func(*applyOption)

// ApplyAtLeastOnce returns an ApplyOption that removes replay protection.
//
// With this option, Apply may attempt to apply mutations more than once; if
// the mutations are not idempotent, this may lead to a failure being reported
// when the mutation was applied more than once. For example, an insert may
// fail with ALREADY_EXISTS even though the row did not exist before Apply was
// called. For this reason, most users of the library will prefer not to use
// this option.  However, ApplyAtLeastOnce requires only a single RPC, whereas
// Apply's default replay protection may require an additional RPC.  So this
// option may be appropriate for latency sensitive and/or high throughput blind
// writing.
func ApplyAtLeastOnce() ApplyOption {
	return func(ao *applyOption) {
		ao.atLeastOnce = true
	}
}

// Apply applies a list of mutations atomically to the database.
func (dc *DataClient) Apply(ctx context.Context, ms []*Mutation, opts ...ApplyOption) (time.Time, error) {
	ao := &applyOption{}
	for _, opt := range opts {
		opt(ao)
	}
	if !ao.atLeastOnce {
		return dc.ReadWriteTransaction(ctx, func(t *ReadWriteTransaction) error {
			return t.BufferWrite(ms)
		})
	}
	t := &writeOnlyTransaction{dc.sp}
	return t.applyAtLeastOnce(ctx, ms...)
}

func (dc *DataClient) Read(ctx context.Context, table string, keys KeySet, index string, columns []string, limit int64) (*tspb.ResultSet, error) {
	sh, err := dc.sp.take(ctx)
	if err != nil {
		return nil, err
	}
	keysProto, err := keys.proto()
	if err != nil {
		return nil, err
	}
	session := sh.getID()

	req := &tspb.ReadRequest{
		Session: session,
		Transaction: &tspb.TransactionSelector{
			Selector: &tspb.TransactionSelector_SingleUse{
				SingleUse: &tspb.TransactionOptions{
					Mode: &tspb.TransactionOptions_ReadOnly_{
						ReadOnly: &tspb.TransactionOptions_ReadOnly{
							TimestampBound: &tspb.TransactionOptions_ReadOnly_Strong{Strong: true},
						},
					},
				},
			},
		},
		Table:   table,
		Index:   index,
		KeySet:  keysProto,
		Columns: columns,
		Limit:   limit,
	}
	defer func() {
		if sh != nil {
			if shouldDropSession(err) {
				sh.destroy()
			}
			sh.recycle()
		}
	}()
	res, err := sh.getClient().Read(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (dc *DataClient) SparseRead(ctx context.Context, table, family string, rows []*SparseRow, limit int64) (*SparseResultSet, error) {
	metricCount(SPARSE_READ)
	t := metricStartTiming()
	sh, err := dc.sp.take(ctx)
	if err != nil {
		return nil, err
	}

	session := sh.getID()

	srows := []*tspb.Row{}
	for _, r := range rows {
		row, err := r.proto()
		if err != nil {
			return nil, err
		}
		srows = append(srows, row)
	}

	req := &tspb.SparseReadRequest{
		Session: session,
		Transaction: &tspb.TransactionSelector{
			Selector: &tspb.TransactionSelector_SingleUse{
				SingleUse: &tspb.TransactionOptions{
					Mode: &tspb.TransactionOptions_ReadOnly_{
						ReadOnly: &tspb.TransactionOptions_ReadOnly{
							TimestampBound: &tspb.TransactionOptions_ReadOnly_Strong{Strong: true},
						},
					},
				},
			},
		},
		Table:  table,
		Family: family,
		Rows:   srows,
		Limit:  limit,
	}
	res, err := sh.getClient().SparseRead(ctx, req)
	if err != nil {
		metricCountError(SPARSE_READ)
		return nil, err
	}
	defer func() {
		metricRecordTiming(t, SPARSE_READ)
		if sh != nil {
			if shouldDropSession(err) {
				sh.destroy()
			}
			sh.recycle()
		}
	}()

	return BuildSparseResultSet(res), nil
}

func (dc *DataClient) readRaw(ctx context.Context, in *tspb.ReadRequest) (*tspb.ResultSet, error) {
	sh, err := dc.sp.take(ctx)
	if err != nil {
		return nil, err
	}

	in.Session = sh.getID()
	res, err := sh.getClient().Read(ctx, in)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// raw mutate once time only in one table
func (dc *DataClient) Mutate(ctx context.Context, rawMS ...*Mutation) error {
	if len(rawMS) <= 0 {
		return ERR_MUTATION_EMPTY
	}
	sameTable := rawMS[0].table
	for _, m := range rawMS[1:] {
		if m.table != sameTable {
			return ERR_MUTATION_DIFF_TABLE
		}
	}
	protoMS, err := mutationsProto(rawMS)
	if err != nil {
		return err
	}
	sh, err := dc.sp.take(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if sh != nil {
			if shouldDropSession(err) {
				sh.destroy()
			}
			sh.recycle()
		}
	}()

	in := &tspb.MutationRequest{
		Session: sh.getID(),
		Table:   sameTable,
		Transaction: &tspb.TransactionSelector{
			Selector: &tspb.TransactionSelector_SingleUse{
				SingleUse: &tspb.TransactionOptions{
					Mode: &tspb.TransactionOptions_ReadWrite_{
						ReadWrite: &tspb.TransactionOptions_ReadWrite{},
					},
				},
			},
		},
		Mutations: protoMS,
	}
	resp, err := sh.getClient().Mutate(ctx, in)
	if err != nil {
		return err
	}
	_ = resp

	return nil
}
