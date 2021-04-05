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
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"github.com/golang/protobuf/proto"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"google.golang.org/grpc/resolver"
)

func decodeSparseValue(v *tspb.Value, t *tspb.Type, ptr interface{}) error {
	code := t.Code
	acode := tspb.TypeCode_TYPE_CODE_UNSPECIFIED
	if code == tspb.TypeCode_ARRAY {
		if t.ArrayElementType == nil {
			return errNilArrElemType(t)
		}
		acode = t.ArrayElementType.Code
	}
	typeErr := func() error {
		if code == tspb.TypeCode_ARRAY {
			return errTypeMismatch(acode, true, ptr)
		}
		return errTypeMismatch(code, false, ptr)
	}
	_, isNull := v.Kind.(*tspb.Value_NullValue)

	// Do the decoding based on the type of ptr.
	switch p := ptr.(type) {
	case nil:
		return errNilDst(nil)
	case *string:
		if p == nil {
			return errNilDst(p)
		}
		if isNull {
			return errDstNotForNull(ptr)
		}
		x, err := getStringValue(v)
		if err != nil {
			return err
		}
		*p = x
	case *NullString:
		if p == nil {
			return errNilDst(p)
		}
		if isNull {
			*p = NullString{}
			break
		}
		x, err := getStringValue(v)
		if err != nil {
			return err
		}
		p.Valid = true
		p.StringVal = x
	case *[]NullString:
		if p == nil {
			return errNilDst(p)
		}
		if acode != tspb.TypeCode_STRING {
			return typeErr()
		}
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeStringArray(x)
		if err != nil {
			return err
		}
		*p = y
	case *[]byte:
		if p == nil {
			return errNilDst(p)
		}
		if isNull {
			*p = nil
			break
		}

		x, err := getBytesValue(v)
		if err != nil {
			return err
		}
		*p = x
	case *[][]byte:
		if p == nil {
			return errNilDst(p)
		}
		// if acode != tspb.TypeCode_BYTES {
		// 	return typeErr
		// }
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeByteArray(x)
		if err != nil {
			return err
		}
		*p = y
	case *int64:
		if p == nil {
			return errNilDst(p)
		}

		if isNull {
			return errDstNotForNull(ptr)
		}
		x, err := getInteger64Value(v)
		if err != nil {
			return err
		}
		*p = x
	case *NullInt64:
		if p == nil {
			return errNilDst(p)
		}

		if isNull {
			*p = NullInt64{}
			break
		}
		x, err := getInteger64Value(v)
		if err != nil {
			return err
		}

		p.Valid = true
		p.Int64 = x
	case *[]NullInt64:
		if p == nil {
			return errNilDst(p)
		}
		// if acode != tspb.TypeCode_INT64 {
		// 	return typeErr
		// }
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeIntArray(x)
		if err != nil {
			return err
		}
		*p = y
	case *bool:
		if p == nil {
			return errNilDst(p)
		}
		if code != tspb.TypeCode_BOOL {
			return typeErr()
		}
		if isNull {
			return errDstNotForNull(ptr)
		}
		x, err := getBoolValue(v)
		if err != nil {
			return err
		}
		*p = x
	case *NullBool:
		if p == nil {
			return errNilDst(p)
		}
		if code != tspb.TypeCode_BOOL {
			return typeErr()
		}
		if isNull {
			*p = NullBool{}
			break
		}
		x, err := getBoolValue(v)
		if err != nil {
			return err
		}
		p.Valid = true
		p.Bool = x
	case *[]NullBool:
		if p == nil {
			return errNilDst(p)
		}
		if acode != tspb.TypeCode_BOOL {
			return typeErr()
		}
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeBoolArray(x)
		if err != nil {
			return err
		}
		*p = y
	case *float64:
		if p == nil {
			return errNilDst(p)
		}
		if code != tspb.TypeCode_FLOAT64 {
			return typeErr()
		}
		if isNull {
			return errDstNotForNull(ptr)
		}
		x, err := getFloat64Value(v)
		if err != nil {
			return err
		}
		*p = x
	case *NullFloat64:
		if p == nil {
			return errNilDst(p)
		}
		if code != tspb.TypeCode_FLOAT64 {
			return typeErr()
		}
		if isNull {
			*p = NullFloat64{}
			break
		}
		x, err := getFloat64Value(v)
		if err != nil {
			return err
		}
		p.Valid = true
		p.Float64 = x
	case *[]NullFloat64:
		if p == nil {
			return errNilDst(p)
		}
		if acode != tspb.TypeCode_FLOAT64 {
			return typeErr()
		}
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeFloat64Array(x)
		if err != nil {
			return err
		}
		*p = y
	case *time.Time:
		var nt NullTime
		if isNull {
			return errDstNotForNull(ptr)
		}
		err := parseNullTime(v, &nt, code, isNull)
		if err != nil {
			return nil
		}
		*p = nt.Time
	case *NullTime:
		err := parseNullTime(v, p, code, isNull)
		if err != nil {
			return err
		}
	case *[]NullTime:
		if p == nil {
			return errNilDst(p)
		}
		if acode != tspb.TypeCode_TIMESTAMP {
			return typeErr()
		}
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeTimeArray(x)
		if err != nil {
			return err
		}
		*p = y
	case *civil.Date:
		if p == nil {
			return errNilDst(p)
		}
		if code != tspb.TypeCode_DATE {
			return typeErr()
		}
		if isNull {
			return errDstNotForNull(ptr)
		}
		x, err := getStringValue(v)
		if err != nil {
			return err
		}
		y, err := civil.ParseDate(x)
		if err != nil {
			return errBadEncoding(v, err)
		}
		*p = y
	case *NullDate:
		if p == nil {
			return errNilDst(p)
		}
		if code != tspb.TypeCode_DATE {
			return typeErr()
		}
		if isNull {
			*p = NullDate{}
			break
		}
		x, err := getStringValue(v)
		if err != nil {
			return err
		}
		y, err := civil.ParseDate(x)
		if err != nil {
			return errBadEncoding(v, err)
		}
		p.Valid = true
		p.Date = y
	case *[]NullDate:
		if p == nil {
			return errNilDst(p)
		}
		if acode != tspb.TypeCode_DATE {
			return typeErr()
		}
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeDateArray(x)
		if err != nil {
			return err
		}
		*p = y
	case *[]NullRow:
		if p == nil {
			return errNilDst(p)
		}
		if acode != tspb.TypeCode_STRUCT {
			return typeErr()
		}
		if isNull {
			*p = nil
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		y, err := decodeRowArray(t.ArrayElementType.StructType, x)
		if err != nil {
			return err
		}
		*p = y
	case *GenericColumnValue:
		*p = GenericColumnValue{
			// Deep clone to ensure subsequent changes to t or v
			// don't affect our decoded value.
			Type:  proto.Clone(t).(*tspb.Type),
			Value: proto.Clone(v).(*tspb.Value),
		}
	default:
		// Check if the proto encoding is for an array of structs.
		if !(code == tspb.TypeCode_ARRAY && acode == tspb.TypeCode_STRUCT) {
			return typeErr()
		}
		vp := reflect.ValueOf(p)
		if !vp.IsValid() {
			return errNilDst(p)
		}
		if !isPtrStructPtrSlice(vp.Type()) {
			// The container is not a pointer to a struct pointer slice.
			return typeErr()
		}
		// Only use reflection for nil detection on slow path.
		// Also, IsNil panics on many types, so check it after the type check.
		if vp.IsNil() {
			return errNilDst(p)
		}
		if isNull {
			// The proto Value is encoding NULL, set the pointer to struct
			// slice to nil as well.
			vp.Elem().Set(reflect.Zero(vp.Elem().Type()))
			break
		}
		x, err := getListValue(v)
		if err != nil {
			return err
		}
		if err = decodeStructArray(t.ArrayElementType.StructType, x, p); err != nil {
			return err
		}
	}
	return nil
}

// split2 returns the values from strings.SplitN(s, sep, 2).
// If sep is not found, it returns ("", "", false) instead.
func split2(s, sep string) (string, string, bool) {
	spl := strings.SplitN(s, sep, 2)
	if len(spl) < 2 {
		return "", "", false
	}
	return spl[0], spl[1], true
}

// ParseTarget splits target into a resolver.Target struct containing scheme,
// authority and endpoint.
//
// If target is not a valid scheme://authority/endpoint, it returns {Endpoint:
// target}.
func ParseTarget(target string) (ret resolver.Target) {
	var ok bool
	ret.Scheme, ret.Endpoint, ok = split2(target, "://")
	if !ok {
		return resolver.Target{Endpoint: target}
	}
	ret.Authority, ret.Endpoint, ok = split2(ret.Endpoint, "/")
	if !ok {
		return resolver.Target{Endpoint: target}
	}
	return ret
}
