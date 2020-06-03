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
Copyright 2016 Google Inc. All Rights Reserved.

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
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zhihu/zetta-client-go/internal/testutil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

var (
	// Mocked transaction timestamp.
	trxTs = time.Unix(1, 2)
	// Metadata for mocked KV table, its rows are returned by SingleUse transactions.
	kvMeta = func() *tspb.ResultSetMetadata {
		meta := testutil.KvMeta
		meta.Transaction = &tspb.Transaction{
			ReadTimestamp: timestampProto(trxTs),
		}
		return &meta
	}()
	// Metadata for mocked ListKV table, which uses List for its key and value.
	// Its rows are returned by snapshot readonly transactions, as indicated in the transaction metadata.
	kvListMeta = &tspb.ResultSetMetadata{
		RowType: &tspb.StructType{
			Fields: []*tspb.StructType_Field{
				{
					Name: "Key",
					Type: &tspb.Type{
						Code: tspb.TypeCode_ARRAY,
						ArrayElementType: &tspb.Type{
							Code: tspb.TypeCode_STRING,
						},
					},
				},
				{
					Name: "Value",
					Type: &tspb.Type{
						Code: tspb.TypeCode_ARRAY,
						ArrayElementType: &tspb.Type{
							Code: tspb.TypeCode_STRING,
						},
					},
				},
			},
		},
		Transaction: &tspb.Transaction{
			Id:            transactionID{5, 6, 7, 8, 9},
			ReadTimestamp: timestampProto(trxTs),
		},
	}
	// Metadata for mocked schema of a query result set, which has two struct
	// columns named "Col1" and "Col2", the struct's schema is like the
	// following:
	//
	//	STRUCT {
	//		INT
	//		LIST<STRING>
	//	}
	//
	// Its rows are returned in readwrite transaction, as indicated in the transaction metadata.
	kvObjectMeta = &tspb.ResultSetMetadata{
		RowType: &tspb.StructType{
			Fields: []*tspb.StructType_Field{
				{
					Name: "Col1",
					Type: &tspb.Type{
						Code: tspb.TypeCode_STRUCT,
						StructType: &tspb.StructType{
							Fields: []*tspb.StructType_Field{
								{
									Name: "foo-f1",
									Type: &tspb.Type{
										Code: tspb.TypeCode_INT64,
									},
								},
								{
									Name: "foo-f2",
									Type: &tspb.Type{
										Code: tspb.TypeCode_ARRAY,
										ArrayElementType: &tspb.Type{
											Code: tspb.TypeCode_STRING,
										},
									},
								},
							},
						},
					},
				},
				{
					Name: "Col2",
					Type: &tspb.Type{
						Code: tspb.TypeCode_STRUCT,
						StructType: &tspb.StructType{
							Fields: []*tspb.StructType_Field{
								{
									Name: "bar-f1",
									Type: &tspb.Type{
										Code: tspb.TypeCode_INT64,
									},
								},
								{
									Name: "bar-f2",
									Type: &tspb.Type{
										Code: tspb.TypeCode_ARRAY,
										ArrayElementType: &tspb.Type{
											Code: tspb.TypeCode_STRING,
										},
									},
								},
							},
						},
					},
				},
			},
		},
		Transaction: &tspb.Transaction{
			Id: transactionID{1, 2, 3, 4, 5},
		},
	}
)

// String implements fmt.stringer.
func (r *Row) String() string {
	return fmt.Sprintf("{fields: %s, val: %s}", r.fields, r.vals)
}

func describeRows(l []*Row) string {
	// generate a nice test failure description
	var s = "["
	for i, r := range l {
		if i != 0 {
			s += ",\n "
		}
		s += fmt.Sprint(r)
	}
	s += "]"
	return s
}

// Helper for generating types Value_ListValue instances, making
// test code shorter and readable.
func genProtoListValue(v ...string) *tspb.Value_ListValue {
	r := &tspb.Value_ListValue{
		ListValue: &tspb.ListValue{
			Values: []*tspb.Value{},
		},
	}
	for _, e := range v {
		r.ListValue.Values = append(
			r.ListValue.Values,
			&tspb.Value{
				Kind: &tspb.Value_StringValue{StringValue: e},
			},
		)
	}
	return r
}

// Test Row generation logics of partialResultSetDecoder.
func TestPartialResultSetDecoder(t *testing.T) {
	restore := setMaxBytesBetweenResumeTokens()
	defer restore()
	var tests = []struct {
		input    []*tspb.PartialResultSet
		wantF    []*Row
		wantTxID transactionID
		wantTs   time.Time
		wantD    bool
	}{
		{
			// Empty input.
			wantD: true,
		},
		// String merging examples.
		{
			// Single KV result.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvMeta,
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "foo"}},
						{Kind: &tspb.Value_StringValue{StringValue: "bar"}},
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "foo"}},
						{Kind: &tspb.Value_StringValue{StringValue: "bar"}},
					},
				},
			},
			wantTs: trxTs,
			wantD:  true,
		},
		{
			// Incomplete partial result.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvMeta,
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "foo"}},
					},
				},
			},
			wantTs: trxTs,
			wantD:  false,
		},
		{
			// Complete splitted result.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvMeta,
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "foo"}},
					},
				},
				{
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "bar"}},
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "foo"}},
						{Kind: &tspb.Value_StringValue{StringValue: "bar"}},
					},
				},
			},
			wantTs: trxTs,
			wantD:  true,
		},
		{
			// Multi-row example with splitted row in the middle.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvMeta,
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "foo"}},
						{Kind: &tspb.Value_StringValue{StringValue: "bar"}},
						{Kind: &tspb.Value_StringValue{StringValue: "A"}},
					},
				},
				{
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "1"}},
						{Kind: &tspb.Value_StringValue{StringValue: "B"}},
						{Kind: &tspb.Value_StringValue{StringValue: "2"}},
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "foo"}},
						{Kind: &tspb.Value_StringValue{StringValue: "bar"}},
					},
				},
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "A"}},
						{Kind: &tspb.Value_StringValue{StringValue: "1"}},
					},
				},
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "B"}},
						{Kind: &tspb.Value_StringValue{StringValue: "2"}},
					},
				},
			},
			wantTs: trxTs,
			wantD:  true,
		},
		{
			// Merging example in result_set.proto.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvMeta,
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "Hello"}},
						{Kind: &tspb.Value_StringValue{StringValue: "W"}},
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "orl"}},
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "d"}},
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "Hello"}},
						{Kind: &tspb.Value_StringValue{StringValue: "World"}},
					},
				},
			},
			wantTs: trxTs,
			wantD:  true,
		},
		{
			// More complex example showing completing a merge and
			// starting a new merge in the same partialResultSet.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvMeta,
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "Hello"}},
						{Kind: &tspb.Value_StringValue{StringValue: "W"}}, // start split in value
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "orld"}}, // complete value
						{Kind: &tspb.Value_StringValue{StringValue: "i"}},    // start split in key
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "s"}}, // complete key
						{Kind: &tspb.Value_StringValue{StringValue: "not"}},
						{Kind: &tspb.Value_StringValue{StringValue: "a"}},
						{Kind: &tspb.Value_StringValue{StringValue: "qu"}}, // split in value
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "estion"}}, // complete value
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "Hello"}},
						{Kind: &tspb.Value_StringValue{StringValue: "World"}},
					},
				},
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "is"}},
						{Kind: &tspb.Value_StringValue{StringValue: "not"}},
					},
				},
				{
					fields: kvMeta.RowType.Fields,
					vals: []*tspb.Value{
						{Kind: &tspb.Value_StringValue{StringValue: "a"}},
						{Kind: &tspb.Value_StringValue{StringValue: "question"}},
					},
				},
			},
			wantTs: trxTs,
			wantD:  true,
		},
		// List merging examples.
		{
			// Non-splitting Lists.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvListMeta,
					Values: []*tspb.Value{
						{
							Kind: genProtoListValue("foo-1", "foo-2"),
						},
					},
				},
				{
					Values: []*tspb.Value{
						{
							Kind: genProtoListValue("bar-1", "bar-2"),
						},
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvListMeta.RowType.Fields,
					vals: []*tspb.Value{
						{
							Kind: genProtoListValue("foo-1", "foo-2"),
						},
						{
							Kind: genProtoListValue("bar-1", "bar-2"),
						},
					},
				},
			},
			wantTxID: transactionID{5, 6, 7, 8, 9},
			wantTs:   trxTs,
			wantD:    true,
		},
		{
			// Simple List merge case: splitted string element.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvListMeta,
					Values: []*tspb.Value{
						{
							Kind: genProtoListValue("foo-1", "foo-"),
						},
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{
							Kind: genProtoListValue("2"),
						},
					},
				},
				{
					Values: []*tspb.Value{
						{
							Kind: genProtoListValue("bar-1", "bar-2"),
						},
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvListMeta.RowType.Fields,
					vals: []*tspb.Value{
						{
							Kind: genProtoListValue("foo-1", "foo-2"),
						},
						{
							Kind: genProtoListValue("bar-1", "bar-2"),
						},
					},
				},
			},
			wantTxID: transactionID{5, 6, 7, 8, 9},
			wantTs:   trxTs,
			wantD:    true,
		},
		{
			// Struct merging is also implemented by List merging. Note that
			// Cloud Spanner uses proto.ListValue to encode Structs as well.
			input: []*tspb.PartialResultSet{
				{
					Metadata: kvObjectMeta,
					Values: []*tspb.Value{
						{
							Kind: &tspb.Value_ListValue{
								ListValue: &tspb.ListValue{
									Values: []*tspb.Value{
										{Kind: &tspb.Value_NumberValue{NumberValue: 23}},
										{Kind: genProtoListValue("foo-1", "fo")},
									},
								},
							},
						},
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{
							Kind: &tspb.Value_ListValue{
								ListValue: &tspb.ListValue{
									Values: []*tspb.Value{
										{Kind: genProtoListValue("o-2", "f")},
									},
								},
							},
						},
					},
					ChunkedValue: true,
				},
				{
					Values: []*tspb.Value{
						{
							Kind: &tspb.Value_ListValue{
								ListValue: &tspb.ListValue{
									Values: []*tspb.Value{
										{Kind: genProtoListValue("oo-3")},
									},
								},
							},
						},
						{
							Kind: &tspb.Value_ListValue{
								ListValue: &tspb.ListValue{
									Values: []*tspb.Value{
										{Kind: &tspb.Value_NumberValue{NumberValue: 45}},
										{Kind: genProtoListValue("bar-1")},
									},
								},
							},
						},
					},
				},
			},
			wantF: []*Row{
				{
					fields: kvObjectMeta.RowType.Fields,
					vals: []*tspb.Value{
						{
							Kind: &tspb.Value_ListValue{
								ListValue: &tspb.ListValue{
									Values: []*tspb.Value{
										{Kind: &tspb.Value_NumberValue{NumberValue: 23}},
										{Kind: genProtoListValue("foo-1", "foo-2", "foo-3")},
									},
								},
							},
						},
						{
							Kind: &tspb.Value_ListValue{
								ListValue: &tspb.ListValue{
									Values: []*tspb.Value{
										{Kind: &tspb.Value_NumberValue{NumberValue: 45}},
										{Kind: genProtoListValue("bar-1")},
									},
								},
							},
						},
					},
				},
			},
			wantTxID: transactionID{1, 2, 3, 4, 5},
			wantD:    true,
		},
	}

nextTest:
	for i, test := range tests {
		var rows []*Row
		p := &partialResultSetDecoder{}
		for j, v := range test.input {
			rs, err := p.add(v)
			if err != nil {
				t.Errorf("test %d.%d: partialResultSetDecoder.add(%v) = %v; want nil", i, j, v, err)
				continue nextTest
			}
			rows = append(rows, rs...)
		}
		if !reflect.DeepEqual(p.ts, test.wantTs) {
			t.Errorf("got transaction(%v), want %v", p.ts, test.wantTs)
		}
		if !reflect.DeepEqual(rows, test.wantF) {
			t.Errorf("test %d: rows=\n%v\n; want\n%v\n; p.row:\n%v\n", i, describeRows(rows), describeRows(test.wantF), p.row)
		}
		if got := p.done(); got != test.wantD {
			t.Errorf("test %d: partialResultSetDecoder.done() = %v", i, got)
		}
	}
}

const (
	maxBuffers = 16 // max number of PartialResultSets that will be buffered in tests.
)

// setMaxBytesBetweenResumeTokens sets the global maxBytesBetweenResumeTokens to a smaller
// value more suitable for tests. It returns a function which should be called to restore
// the maxBytesBetweenResumeTokens to its old value
func setMaxBytesBetweenResumeTokens() func() {
	o := atomic.LoadInt32(&maxBytesBetweenResumeTokens)
	atomic.StoreInt32(&maxBytesBetweenResumeTokens, int32(maxBuffers*proto.Size(&tspb.PartialResultSet{
		Metadata: kvMeta,
		Values: []*tspb.Value{
			{Kind: &tspb.Value_StringValue{StringValue: keyStr(0)}},
			{Kind: &tspb.Value_StringValue{StringValue: valStr(0)}},
		},
	})))
	return func() {
		atomic.StoreInt32(&maxBytesBetweenResumeTokens, o)
	}
}

// keyStr generates key string for kvMeta schema.
func keyStr(i int) string {
	return fmt.Sprintf("foo-%02d", i)
}

// valStr generates value string for kvMeta schema.
func valStr(i int) string {
	return fmt.Sprintf("bar-%02d", i)
}
