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
	"bytes"
	"io"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/googleapis/gax-go/v2"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// streamingReceiver is the interface for receiving data from a client side stream.
// streamingReceiver 是从客户端流中的读取数据的接口
type streamingReceiver interface {
	Recv() (*tspb.PartialResultSet, error)
}

// errEarlyReadEnd returns error for read finishes when gRPC stream is still active.
func errEarlyReadEnd() error {
	return wrapError(codes.FailedPrecondition, "read completed with active stream")
}

// stream is the internal fault tolerant method for streaming data from
// Cloud Spanner.
func stream(ctx context.Context, rpc func(ct context.Context, resumeToken []byte) (streamingReceiver, error), release func(time.Time, error)) *RowIterator {
	ctx, cancel := context.WithCancel(ctx)
	return &RowIterator{
		streamd: newResumableStreamDecoder(ctx, rpc),
		rowd:    &partialResultSetDecoder{},
		release: release,
		cancel:  cancel,
	}
}

/* 行迭代器 */
//
// RowIterator is an iterator over Rows.
//
type RowIterator struct {
	streamd      *resumableStreamDecoder  // 可恢复流 decoder
	rowd         *partialResultSetDecoder // prs decoder
	setTimestamp func(time.Time)
	release      func(time.Time, error)
	cancel       func()
	err          error
	rows         []*Row
}

/*
 * Next 返回下一行和是否迭代结束
 * 若迭代结束后续调用都会返回 Done
 */
// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns Done, all subsequent calls
// will return Done.
func (r *RowIterator) Next() (*Row, error) {
	if r.err != nil {
		return nil, r.err
	}
	for len(r.rows) == 0 && r.streamd.next() { /* 批量取 */
		prs := r.streamd.get()
		r.rows, r.err = r.rowd.add(prs)
		if r.err != nil {
			return nil, r.err
		}
		if !r.rowd.ts.IsZero() && r.setTimestamp != nil {
			r.setTimestamp(r.rowd.ts)
			r.setTimestamp = nil
		}

	}
	if len(r.rows) > 0 {
		row := r.rows[0]
		r.rows = r.rows[1:] /* 返回第一个 */
		return row, nil
	}
	if err := r.streamd.lastErr(); err != nil {
		r.err = toZettaError(err)
	} else if !r.rowd.done() {
		r.err = errEarlyReadEnd()
	} else {
		r.err = iterator.Done
	}
	return nil, r.err
}

/*
 * 对迭代到的每一行都调用 f, r.Stop() 始终都会被调用
 */
// Do calls the provided function once in sequence for each row in the iteration.  If the
// function returns a non-nil error, Do immediately returns that value.
//
// If there are no rows in the iterator, Do will return nil without calling the
// provided function.
//
// Do always calls Stop on the iterator.
func (r *RowIterator) Do(f func(r *Row) error) error {
	defer r.Stop()
	for {
		row, err := r.Next()
		switch err {
		case iterator.Done:
			return nil
		case nil:
			if err = f(row); err != nil {
				return err
			}
		default:
			return err
		}
	}
}

/* 主动结束 iterator，取消 context */
// Stop terminates the iteration. It should be called after every iteration.
func (r *RowIterator) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	if r.release != nil {
		r.release(r.rowd.ts, r.err)
		if r.err == nil {
			r.err = wrapError(codes.FailedPrecondition, "Next called after Stop")
		}
		r.release = nil
	}
}

/*
 * 模拟存储 PartialResultSet 的队列，队列满时 2 倍扩容
 */
// partialResultQueue implements a simple FIFO queue. The zero value is a valid queue.
type partialResultQueue struct {
	q     []*tspb.PartialResultSet
	first int
	last  int
	n     int
}

func (q *partialResultQueue) empty() bool {
	return q.n == 0
}

func errEmptyQueue() error {
	return wrapError(codes.OutOfRange, "empty partialResultQueue")
}

func (q *partialResultQueue) peekLast() (*tspb.PartialResultSet, error) {
	if q.empty() {
		return nil, errEmptyQueue()
	}
	return q.q[(q.last+cap(q.q)-1)%cap(q.q)], nil
}

func (q *partialResultQueue) push(r *tspb.PartialResultSet) {
	if q.q == nil {
		q.q = make([]*tspb.PartialResultSet, 8 /* arbitrary */)
	}
	if q.n == cap(q.q) {
		buf := make([]*tspb.PartialResultSet, cap(q.q)*2)
		for i := 0; i < q.n; i++ {
			buf[i] = q.q[(q.first+i)%cap(q.q)]
		}
		q.q = buf
		q.first = 0
		q.last = q.n
	}
	q.q[q.last] = r
	q.last = (q.last + 1) % cap(q.q)
	q.n++
}

func (q *partialResultQueue) pop() *tspb.PartialResultSet {
	if q.n == 0 {
		return nil
	}
	r := q.q[q.first]
	q.q[q.first] = nil
	q.first = (q.first + 1) % cap(q.q)
	q.n--
	return r
}

func (q *partialResultQueue) clear() {
	*q = partialResultQueue{}
}

func (q *partialResultQueue) dump() []*tspb.PartialResultSet {
	var dq []*tspb.PartialResultSet
	for i := q.first; len(dq) < q.n; i = (i + 1) % cap(q.q) {
		dq = append(dq, q.q[i])
	}
	return dq
}

// resumableStreamDecoderState encodes resumableStreamDecoder's status.
// See also the comments for resumableStreamDecoder.Next.
type resumableStreamDecoderState int

const (
	unConnected         resumableStreamDecoderState = iota // 0
	queueingRetryable                                      // 1
	queueingUnretryable                                    // 2
	aborted                                                // 3
	finished                                               // 4
)

//
//
// resumableStreamDecoder提供了一个可恢复接口，用于从由 resumableStreamDecoder.rpc() 包装的给定查询中接收tspb.PartialResultSet。
// resumableStreamDecoder provides a resumable interface for receiving
// tspb.PartialResultSet(s) from a given query wrapped by resumableStreamDecoder.rpc().
//
//
type resumableStreamDecoder struct {
	// state is the current status of resumableStreamDecoder, see also the comments for resumableStreamDecoder.Next.
	/* 当前 decoder 状态 */
	state resumableStreamDecoderState
	// stateWitness when non-nil is called to observe state change, used for testing.
	/* 测试用状态变更 */
	stateWitness func(resumableStreamDecoderState)

	// ctx is the caller's context, used for cancel/timeout Next().
	/* 调用方上下文 */
	ctx context.Context

	// rpc is a factory of streamingReceiver, which might resume a pervious stream from the point encoded in restartToken.
	// rpc is always a wrapper of a Cloud Spanner query which is resumable.
	/* rpc() 是 receiver 的工厂函数，能从 restartToken 标志的上一个流中恢复 */
	rpc func(ctx context.Context, restartToken []byte) (streamingReceiver, error)

	// stream is the current RPC streaming receiver.
	/* stream 是 decoder 当前的 receiver 接口的实现 */
	stream streamingReceiver

	// q buffers received yet undecoded partial results.
	q partialResultQueue

	// bytesBetweenResumeTokens is the proxy of the byte size of PartialResultSets being queued between two resume tokens.
	// Once bytesBetweenResumeTokens is greater than maxBytesBetweenResumeTokens, resumableStreamDecoder goes into queueingUnretryable state.
	/* 在 2 个 resumeToken 之间缓存与队列的 PRS 字节数 */
	bytesBetweenResumeTokens int32
	// maxBytesBetweenResumeTokens is the max number of bytes that can be buffered between two resume tokens
	// it is always copied from the global maxBytesBetweenResumeTokens atomically.
	maxBytesBetweenResumeTokens int32

	// np is the next tspb.PartialResultSet ready to be returned to caller of resumableStreamDecoder.Get().
	/* np 是下一个被 .Get() 方法返回的 PartialResultSet */
	np *tspb.PartialResultSet

	// resumeToken stores the resume token that resumableStreamDecoder has last revealed to caller.
	resumeToken []byte
	// retryCount is the number of retries that have been carried out so far
	retryCount int
	// err is the last error resumableStreamDecoder has encountered so far.
	err error
	// backoff is used for the retry settings
	backoff gax.Backoff
}

// newResumableStreamDecoder creates a new resumeableStreamDecoder instance.
// Parameter rpc should be a function that creates a new stream
// beginning at the restartToken if non-nil.
func newResumableStreamDecoder(ctx context.Context, rpc func(ct context.Context, restartToken []byte) (streamingReceiver, error)) *resumableStreamDecoder {
	return &resumableStreamDecoder{
		ctx:                         ctx,
		rpc:                         rpc,
		maxBytesBetweenResumeTokens: atomic.LoadInt32(&maxBytesBetweenResumeTokens),
		backoff:                     DefaultRetryBackoff,
	}
}

// changeState fulfills state transition for resumableStateDecoder.
func (d *resumableStreamDecoder) changeState(target resumableStreamDecoderState) {
	if d.state == queueingRetryable && d.state != target {
		// Reset bytesBetweenResumeTokens because it is only meaningful/changed under
		// queueingRetryable state.
		d.bytesBetweenResumeTokens = 0
	}
	d.state = target
	if d.stateWitness != nil {
		d.stateWitness(target)
	}
}

// 本次 rt 和上次 token 不一致则认为是新 token
// isNewResumeToken returns if the observed resume token is different from
// the one returned from server last time.
func (d *resumableStreamDecoder) isNewResumeToken(rt []byte) bool {
	if rt == nil {
		return false
	}
	if bytes.Compare(rt, d.resumeToken) == 0 {
		return false
	}
	return true
}

// Next advances to the next available partial result set.  If error or no
// more, returns false, call Err to determine if an error was encountered.
// The following diagram illustrates the state machine of resumableStreamDecoder
// that Next() implements. Note that state transition can be only triggered by
// RPC activities.
/*
        rpc() fails retryable
      +---------+
      |         |    rpc() fails unretryable/ctx timeouts or cancelled
      |         |   +------------------------------------------------+
      |         |   |                                                |
      |         v   |                                                v
      |     +---+---+---+                       +--------+    +------+--+
      +-----+unConnected|                       |finished|    | aborted |<----+
            |           |                       ++-----+-+    +------+--+     |
            +---+----+--+                        ^     ^             ^        |
                |    ^                           |     |             |        |
                |    |                           |     |     recv() fails     |
                |    |                           |     |             |        |
                |    |recv() fails retryable     |     |             |        |
                |    |with valid ctx             |     |             |        |
                |    |                           |     |             |        |
      rpc() succeeds |   +-----------------------+     |             |        |
                |    |   |         recv EOF         recv EOF         |        |
                |    |   |                             |             |        |
                v    |   |     Queue size exceeds      |             |        |
            +---+----+---+----+threshold       +-------+-----------+ |        |
+---------->+                 +--------------->+                   +-+        |
|           |queueingRetryable|                |queueingUnretryable|          |
|           |                 +<---------------+                   |          |
|           +---+----------+--+ pop() returns  +--+----+-----------+          |
|               |          |    resume token      |    ^                      |
|               |          |                      |    |                      |
|               |          |                      |    |                      |
+---------------+          |                      |    |                      |
   recv() succeeds         |                      +----+                      |
                           |                      recv() succeeds             |
                           |                                                  |
                           |                                                  |
                           |                                                  |
                           |                                                  |
                           |                                                  |
                           +--------------------------------------------------+
                                               recv() fails unretryable

*/
var (
	// maxBytesBetweenResumeTokens is the maximum amount of bytes that resumableStreamDecoder
	// in queueingRetryable state can use to queue PartialResultSets before getting
	// into queueingUnretryable state.
	/* decoder 从 queueingRetryable 到 queueingUnretryable 之间最多能缓存 128 MB 数据 */
	maxBytesBetweenResumeTokens = int32(128 * 1024 * 1024)
)

func errContextCanceled(lastErr error) error {
	return wrapError(codes.Canceled, "context is canceled, lastErr is <%v>", lastErr)
}

// TODO
func isRetryable(err error) bool {

	return true
}

func (d *resumableStreamDecoder) next() bool {
	retryer := gax.OnCodes([]codes.Code{codes.Unavailable, codes.Internal}, d.backoff)
	for {
		// fmt.Printf("hererer state: %v\n", d.state)
		switch d.state {
		case unConnected:
			// If no gRPC stream is available, try to initiate one.
			/* 尝试新建 stream */
			d.stream, d.err = d.rpc(d.ctx, d.resumeToken)
			if d.err == nil {
				d.changeState(queueingRetryable)
				continue
			}
			// fmt.Printf("rpc err %v\n", d.err)
			if strings.Contains(d.err.Error(), "Error while dialing dial tcp") {

				d.changeState(aborted)
				continue
			}
			delay, shouldRetry := retryer.Retry(d.err)
			if !shouldRetry {
				d.changeState(aborted)
				continue
			}
			if err := gax.Sleep(d.ctx, delay); err == nil {
				// Be explicit about state transition, although the
				// state doesn't actually change. State transition
				// will be triggered only by RPC activity, regardless of
				// whether there is an actual state change or not.
				d.changeState(unConnected) // 虽然状态没变，但还是要手动触发一下
				continue
			} else {
				d.err = err
				d.changeState(aborted)
			}
			continue
		case queueingRetryable:
			fallthrough
		case queueingUnretryable:
			last, err := d.q.peekLast()
			if err != nil {
				// Only the case that receiving queue is empty could cause
				// peekLast to return error and in such case, we should try to
				// receive from stream.  队列空，尝试收取新数据
				d.tryRecv(retryer)
				continue
			}
			if d.isNewResumeToken(last.ResumeToken) {
				// Got new resume token, return buffered tspb.PartialResultSets to caller.
				// 收到新的 token 直接返回
				d.np = d.q.pop()
				if d.q.empty() {
					d.bytesBetweenResumeTokens = 0
					d.resumeToken = d.np.ResumeToken // The new resume token was just popped out from queue, record it.
					d.changeState(queueingRetryable)
				}
				return true
			}
			if d.bytesBetweenResumeTokens >= d.maxBytesBetweenResumeTokens && d.state == queueingRetryable {
				d.changeState(queueingUnretryable)
				continue
			}
			if d.state == queueingUnretryable {
				// When there is no resume token observed, only yield tspb.PartialResultSets to caller under queueingUnretryable state.
				d.np = d.q.pop()
				return true
			}
			// Needs to receive more from gRPC stream till a new resume token is observed.
			d.tryRecv(retryer)
			continue
		case aborted:
			// Discard all pending items because none of them should be yield to caller.
			d.q.clear()
			return false
		case finished:
			// If query has finished, check if there are still buffered messages.
			if d.q.empty() { /* 读取完毕 */
				return false // No buffered PartialResultSet.
			}
			// Although query has finished, there are still buffered PartialResultSets.
			d.np = d.q.pop() /* 还有缓冲结果集 */
			return true

		default:
			log.Printf("Unexpected resumableStreamDecoder.state: %v", d.state)
			return false
		}

	}
}

/* 尝试从 gRPC 流式收取 PartialResultSet */
// tryRecv attempts to receive a PartialResultSet from gRPC stream.
func (d *resumableStreamDecoder) tryRecv(retryer gax.Retryer) {
	var res *tspb.PartialResultSet
	res, d.err = d.stream.Recv()
	if d.err == nil {
		d.q.push(res) // 将读取到的 set 入队列
		if d.state == queueingRetryable && !d.isNewResumeToken(res.ResumeToken) {
			// adjusting d.bytesBetweenResumeTokens
			d.bytesBetweenResumeTokens += int32(proto.Size(res))
		}
		d.changeState(d.state)
		return
	}

	if d.err == io.EOF { /* 读取完毕 */
		d.err = nil
		d.changeState(finished)
		return
	}
	delay, shouldRetry := retryer.Retry(d.err)
	if !shouldRetry || d.state != queueingRetryable {
		d.changeState(aborted)
		return
	}
	if err := gax.Sleep(d.ctx, delay); err != nil {
		d.err = err
		d.changeState(aborted)
		return
	}
	// Clear error and retry the stream.
	d.err = nil
	// Discard all queue items (none have resume tokens).
	d.q.clear()
	d.stream = nil
	d.changeState(unConnected)
	return
}

/* 返回上次调用 next() 的 prs */
// get returns the most recent PartialResultSet generated by a call to next.
func (d *resumableStreamDecoder) get() *tspb.PartialResultSet {
	return d.np
}

// lastErr returns the last non-EOF error encountered.
func (d *resumableStreamDecoder) lastErr() error {
	return d.err
}

//
// prs decoder 将 prs 解码组装成行
// partialResultSetDecoder assembles PartialResultSet(s) into Cloud Spanner Rows.
//
//
type partialResultSetDecoder struct {
	row     Row               // 当前行
	tx      *tspb.Transaction // tid
	chunked bool              // if true, next value should be merged with last values entry.
	ts      time.Time         // read timestamp
}

/* 检查当前行是否完整，不完整情况：
 * 1. 列数不足
 * 2. chunked 块响应但没有更多要处理的数据了
 */
// yield checks we have a complete row, and if so returns it.  A row is not
// complete if it doesn't have enough columns, or if this is a chunked response
// and there are no further values to process.
func (p *partialResultSetDecoder) yield(chunked, last bool) *Row {
	// 列和值数量上匹配，并且不是 chunked，或者就是 chunked 但不是最后一份
	// 才视为完整
	if len(p.row.vals) == len(p.row.fields) && (!chunked || !last) {

		/*
		* 当 psr decoder 持有足够数量的列值，以下 2 种情况需验证:
		* 1. prs 不是块数据
		* 2. prs 是块数据，但被合并的 proto value 并不是最后一个
		* p.rows 在下次读取会被覆盖，copy 后返回
		 */
		// When partialResultSetDecoder gets enough number of
		// Column values, There are two cases that a new Row
		// should be yield:
		//   1. The incoming PartialResultSet is not chunked;
		//   2. The incoming PartialResultSet is chunked, but the
		//      tspb.Value being merged is not the last one in
		//      the PartialResultSet.
		//
		// Use a fresh Row to simplify clients that want to use yielded results
		// after the next row is retrieved. Note that fields is never changed
		// so it doesn't need to be copied.
		fresh := Row{
			fields: p.row.fields,
			vals:   make([]*tspb.Value, len(p.row.vals)),
		}
		copy(fresh.vals, p.row.vals)
		p.row.vals = p.row.vals[:0] // empty and reuse slice
		return &fresh
	}
	return nil
}

// yieldTx returns transaction information via caller supplied callback.
func errChunkedEmptyRow() error {
	return wrapError(codes.FailedPrecondition, "partialResultSetDecoder gets chunked empty row")
}

/* 尝试将 prs 合并到缓冲行，返回合并结果 */
// add tries to merge a new PartialResultSet into buffered Row. It returns
// any rows that have been completed as a result.
func (p *partialResultSetDecoder) add(r *tspb.PartialResultSet) ([]*Row, error) {
	var rows []*Row
	if r.Metadata != nil { /* 如果没有 meta 则补全 */
		// Metadata should only be returned in the first result.
		if p.row.fields == nil {
			p.row.fields = r.Metadata.RowType.Fields
		}
		if p.tx == nil && r.Metadata.Transaction != nil {
			p.tx = r.Metadata.Transaction
			if p.tx.ReadTimestamp != nil {
				p.ts = time.Unix(p.tx.ReadTimestamp.Seconds, int64(p.tx.ReadTimestamp.Nanos))
			}
		}
	}
	// if len(r.Values) == 0 {
	// 	return nil, nil
	// }
	if p.chunked {
		p.chunked = false
		// Try to merge first value in r.Values into uncompleted row.
		last := len(p.row.vals) - 1
		if last < 0 { // sanity check
			return nil, errChunkedEmptyRow()
		}
		var err error
		// If p is chunked, then we should always try to merge p.last with r.first.
		/* 如果是块数据，则将 r 的第一个值合并到 p 的最后一个值中 */
		if p.row.vals[last], err = p.merge(p.row.vals[last], r.Values[0]); err != nil {
			return nil, err
		}
		/* 剔除 r 头部数据 */
		r.Values = r.Values[1:]
		// Merge is done, try to yield a complete Row.
		if row := p.yield(r.ChunkedValue, len(r.Values) == 0); row != nil {
			rows = append(rows, row) /* p.row 完整 */
		}
	}
	for i, v := range r.Values {
		// The rest values in r can be appened into p directly.
		/* chunk 剩下的值能直接 append */
		p.row.vals = append(p.row.vals, v)
		// Again, check to see if a complete Row can be yielded because of
		// the newly added value.
		if row := p.yield(r.ChunkedValue, i == len(r.Values)-1); row != nil {
			rows = append(rows, row)
		}
	}
	if r.ChunkedValue {
		// After dealing with all values in r, if r is chunked then p must be also chunked.
		/* p 和 r 的 chunk 状态保持一致 */
		p.chunked = true
	}

	// primaryKeys = r.RowCells
	if r.RowCells != nil {
		row := &Row{
			primaryKeys: r.RowCells.PrimaryKeys,
			cells:       r.RowCells.Cells,
		}
		rows = append(rows, row)
	}

	return rows, nil
}

/* 只有 string 和 list 才能 merge */
// isMergeable returns if a protobuf Value can be potentially merged with
// other protobuf Values.
func (p *partialResultSetDecoder) isMergeable(a *tspb.Value) bool {
	switch a.Kind.(type) {
	case *tspb.Value_StringValue:
		return true
	case *tspb.Value_ListValue:
		return true
	default:
		return false
	}
}
func errIncompatibleMergeTypes(a, b *tspb.Value) error {
	return wrapError(codes.FailedPrecondition, "partialResultSetDecoder merge(%T,%T) - incompatible types", a.Kind, b.Kind)
}

func errUnsupportedMergeType(a *tspb.Value) error {
	return wrapError(codes.FailedPrecondition, "unsupported type merge (%T)", a.Kind)
}

/* 将 2 个 protobuf 值合并 */
// merge tries to combine two protobuf Values if possible.
func (p *partialResultSetDecoder) merge(a, b *tspb.Value) (*tspb.Value, error) {
	var err error
	typeErr := errIncompatibleMergeTypes(a, b)
	switch t := a.Kind.(type) {
	case *tspb.Value_StringValue:
		s, ok := b.Kind.(*tspb.Value_StringValue)
		if !ok {
			return nil, typeErr
		}
		return &tspb.Value{
			Kind: &tspb.Value_StringValue{StringValue: t.StringValue + s.StringValue}, /* string 类型的直接连接 merge */
		}, nil
	case *tspb.Value_ListValue:
		l, ok := b.Kind.(*tspb.Value_ListValue)
		if !ok {
			return nil, typeErr
		}
		if l.ListValue == nil || len(l.ListValue.Values) <= 0 {
			// b is an empty list, just return a.
			return a, nil
		}
		if t.ListValue == nil || len(t.ListValue.Values) <= 0 {
			// a is an empty list, just return b.
			return b, nil
		}

		/* 取列表 a 最后一个值，将其和列表 b 的第一个值 merge  */
		if la := len(t.ListValue.Values) - 1; p.isMergeable(t.ListValue.Values[la]) {
			// When the last item in a is of type String,
			// List or Struct(encoded into List by Cloud Spanner),
			// try to Merge last item in a and first item in b.
			t.ListValue.Values[la], err = p.merge(t.ListValue.Values[la], l.ListValue.Values[0])
			if err != nil {
				return nil, err
			}
			l.ListValue.Values = l.ListValue.Values[1:]
		}
		return &tspb.Value{
			Kind: &tspb.Value_ListValue{
				ListValue: &tspb.ListValue{
					Values: append(t.ListValue.Values, l.ListValue.Values...), /* merge 后列表 2 剩下部分直接 merge */
				},
			},
		}, nil
	default:
		return nil, errUnsupportedMergeType(a)
	}

}

// Done returns if partialResultSetDecoder has already done with all buffered values.
func (p *partialResultSetDecoder) done() bool {
	// There is no explicit end of stream marker, but ending part way
	// through a row is obviously bad, or ending with the last column still
	// awaiting completion.
	return len(p.row.vals) == 0 && !p.chunked && len(p.row.cells) == 0
}
