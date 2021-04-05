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
	"fmt"
	"time"

	"cloud.google.com/go/civil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"google.golang.org/grpc/codes"
)

// struct Key is primary key / secondary index key, as interface{} array
// usages:
// 1. primary key -> unique row
// 2. secondary index key -> match set of rows
// 3. use in KeyRange type
//
//
// key rows are Read operations / Delete mutations targets
// but note:
// column list for Insert / Update mutations must comprise a pk
//
//
// Go Type => Key Type
// int, int8, int16, int32, int64, NullInt64 	-> INT64
// float32, float64, NullFloat64             	-> FLOAT64
// bool and NullBool       						-> BOOL
// []byte                  						-> BYTES
// string and NullString   						-> STRING
// time.Time and NullTime 						-> TIMESTAMP
// civil.Date and NullDate						-> DATE
//
type Key []interface{}

// errInvdKeyPartType returns error for unsupported key part type.
func errInvdKeyPartType(part interface{}) error {
	return wrapError(codes.InvalidArgument, "key part has unsupported type %T", part)
}

// keyPartValue converts a part of the Key into a tspb.Value. Used for encoding Key type into protobuf.
func keyPartValue(part interface{}) (pb *tspb.Value, err error) {
	switch v := part.(type) {
	case int:
		pb, _, err = encodeValue(int64(v))
	case int8:
		pb, _, err = encodeValue(int64(v))
	case int16:
		pb, _, err = encodeValue(int64(v))
	case int32:
		pb, _, err = encodeValue(int64(v))
	case uint8:
		pb, _, err = encodeValue(int64(v))
	case uint16:
		pb, _, err = encodeValue(int64(v))
	case uint32:
		pb, _, err = encodeValue(int64(v))
	case float32:
		pb, _, err = encodeValue(float64(v))
	case int64, float64, NullInt64, NullFloat64, bool, NullBool, []byte, string, NullString, time.Time, civil.Date, NullTime, NullDate:
		pb, _, err = encodeValue(v)
	default:
		return nil, errInvdKeyPartType(v)
	}
	return pb, err
}

// proto converts a Key into a tspb.ListValue.
func (key Key) proto() (*tspb.ListValue, error) {
	lv := &tspb.ListValue{}
	lv.Values = make([]*tspb.Value, 0, len(key))
	for _, part := range key {
		v, err := keyPartValue(part)
		if err != nil {
			return nil, err
		}
		lv.Values = append(lv.Values, v)
	}
	return lv, nil
}

// keySetProto lets a single Key act as a KeySet.
func (key Key) keySetProto() (*tspb.KeySet, error) {
	kp, err := key.proto()
	if err != nil {
		return nil, err
	}
	return &tspb.KeySet{Keys: []*tspb.ListValue{kp}}, nil
}

func (key Key) String() string {
	b := &bytes.Buffer{}
	fmt.Fprint(b, "(")
	for i, part := range []interface{}(key) {
		if i != 0 {
			fmt.Fprint(b, ",")
		}
		switch v := part.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, float32, float64, bool:
			fmt.Fprintf(b, "%v", v)
		case string:
			fmt.Fprintf(b, "%q", v)
		case []byte:
			if v != nil {
				fmt.Fprintf(b, "%q", v)
			} else {
				fmt.Fprint(b, "<null>")
			}
		case NullInt64, NullFloat64, NullBool, NullString, NullTime, NullDate:
			// The above types implement fmt.Stringer.
			fmt.Fprintf(b, "%s", v)
		case civil.Date:
			fmt.Fprintf(b, "%q", v)
		case time.Time:
			fmt.Fprintf(b, "%q", v.Format(time.RFC3339Nano))
		default:
			fmt.Fprintf(b, "%v", v)
		}
	}
	fmt.Fprint(b, ")")
	return b.String()
}

// AsPrefix returns a KeyRange for all keys where k is the prefix.
func (key Key) AsPrefix() KeyRange {
	return KeyRange{
		Start: key,
		End:   key,
		Kind:  ClosedClosed,
	}
}

// mark boundary open state of left and right
type KeyRangeKind int

const (
	ClosedOpen   KeyRangeKind = iota // [l, r)
	ClosedClosed                     // [l, r]
	OpenClosed                       // (l, r]
	OpenOpen                         // (l, r)
)

//
// KeyRange is range of rows in table / index
// use start key and end key to identify borders and they two could be close or open
// direction of Key is same as filed
//
type KeyRange struct {
	// left and right boundary
	Start, End Key

	// boundaries are included or not
	Kind KeyRangeKind
}

func (r KeyRange) String() string {
	var left, right string
	switch r.Kind {
	case ClosedClosed:
		left, right = "[", "]"
	case ClosedOpen:
		left, right = "[", ")"
	case OpenClosed:
		left, right = "(", "]"
	case OpenOpen:
		left, right = "(", ")"
	default:
		left, right = "?", "?"
	}
	return fmt.Sprintf("%s%s,%s%s", left, r.Start, r.End, right)
}

// keySetProto lets a KeyRange act as a KeySet.
func (r KeyRange) keySetProto() (*tspb.KeySet, error) {
	rp, err := r.proto()
	if err != nil {
		return nil, err
	}
	return &tspb.KeySet{Ranges: []*tspb.KeyRange{rp}}, nil
}

// proto converts KeyRange into tspb.KeyRange.
func (r KeyRange) proto() (*tspb.KeyRange, error) {
	var err error
	var start, end *tspb.ListValue
	pb := &tspb.KeyRange{}
	if start, err = r.Start.proto(); err != nil {
		return nil, err
	}
	if end, err = r.End.proto(); err != nil {
		return nil, err
	}
	if r.Kind == ClosedClosed || r.Kind == ClosedOpen {
		pb.StartKeyType = &tspb.KeyRange_StartClosed{StartClosed: start}
	} else {
		pb.StartKeyType = &tspb.KeyRange_StartOpen{StartOpen: start}
	}
	if r.Kind == ClosedClosed || r.Kind == OpenClosed {
		pb.EndKeyType = &tspb.KeyRange_EndClosed{EndClosed: end}
	} else {
		pb.EndKeyType = &tspb.KeyRange_EndOpen{EndOpen: end}
	}
	return pb, nil
}
