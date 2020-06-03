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
	"encoding/base64"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	proto "github.com/golang/protobuf/proto"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

var (
	tm    = time.Now().UTC()
	dt, _ = civil.ParseDate("2016-11-15")
	// row contains a column for each unique Cloud Spanner type.
	row = Row{
		[]*tspb.StructType_Field{
			// STRING / STRING ARRAY
			{Name: "STRING", Type: stringType()},
			{Name: "NULL_STRING", Type: stringType()},
			{Name: "STRING_ARRAY", Type: listType(stringType())},
			{Name: "NULL_STRING_ARRAY", Type: listType(stringType())},
			// BYTES / BYTES ARRAY
			{Name: "BYTES", Type: bytesType()},
			{Name: "NULL_BYTES", Type: bytesType()},
			{Name: "BYTES_ARRAY", Type: listType(bytesType())},
			{Name: "NULL_BYTES_ARRAY", Type: listType(bytesType())},
			// INT64 / INT64 ARRAY
			{Name: "INT64", Type: intType()},
			{Name: "NULL_INT64", Type: intType()},
			{Name: "INT64_ARRAY", Type: listType(intType())},
			{Name: "NULL_INT64_ARRAY", Type: listType(intType())},
			// BOOL / BOOL ARRAY
			{Name: "BOOL", Type: boolType()},
			{Name: "NULL_BOOL", Type: boolType()},
			{Name: "BOOL_ARRAY", Type: listType(boolType())},
			{Name: "NULL_BOOL_ARRAY", Type: listType(boolType())},
			// FLOAT64 / FLOAT64 ARRAY
			{Name: "FLOAT64", Type: floatType()},
			{Name: "NULL_FLOAT64", Type: floatType()},
			{Name: "FLOAT64_ARRAY", Type: listType(floatType())},
			{Name: "NULL_FLOAT64_ARRAY", Type: listType(floatType())},
			// TIMESTAMP / TIMESTAMP ARRAY
			{Name: "TIMESTAMP", Type: timeType()},
			{Name: "NULL_TIMESTAMP", Type: timeType()},
			{Name: "TIMESTAMP_ARRAY", Type: listType(timeType())},
			{Name: "NULL_TIMESTAMP_ARRAY", Type: listType(timeType())},
			// DATE / DATE ARRAY
			{Name: "DATE", Type: dateType()},
			{Name: "NULL_DATE", Type: dateType()},
			{Name: "DATE_ARRAY", Type: listType(dateType())},
			{Name: "NULL_DATE_ARRAY", Type: listType(dateType())},

			// STRUCT ARRAY
			{
				Name: "STRUCT_ARRAY",
				Type: listType(
					structType(
						mkField("Col1", intType()),
						mkField("Col2", floatType()),
						mkField("Col3", stringType()),
					),
				),
			},
			{
				Name: "NULL_STRUCT_ARRAY",
				Type: listType(
					structType(
						mkField("Col1", intType()),
						mkField("Col2", floatType()),
						mkField("Col3", stringType()),
					),
				),
			},
		},
		[]*tspb.Value{
			// STRING / STRING ARRAY
			stringProto("value"),
			nullProto(),
			listProto(stringProto("value1"), nullProto(), stringProto("value3")),
			nullProto(),
			// BYTES / BYTES ARRAY
			bytesProto([]byte("value")),
			nullProto(),
			listProto(bytesProto([]byte("value1")), nullProto(), bytesProto([]byte("value3"))),
			nullProto(),
			// INT64 / INT64 ARRAY
			intProto(17),
			nullProto(),
			listProto(intProto(1), intProto(2), nullProto()),
			nullProto(),
			// BOOL / BOOL ARRAY
			boolProto(true),
			nullProto(),
			listProto(nullProto(), boolProto(true), boolProto(false)),
			nullProto(),
			// FLOAT64 / FLOAT64 ARRAY
			floatProto(1.7),
			nullProto(),
			listProto(nullProto(), nullProto(), floatProto(1.7)),
			nullProto(),
			// TIMESTAMP / TIMESTAMP ARRAY
			timeProto(tm),
			nullProto(),
			listProto(nullProto(), timeProto(tm)),
			nullProto(),
			// DATE / DATE ARRAY
			dateProto(dt),
			nullProto(),
			listProto(nullProto(), dateProto(dt)),
			nullProto(),
			// STRUCT ARRAY
			listProto(
				nullProto(),
				listProto(intProto(3), floatProto(33.3), stringProto("three")),
				nullProto(),
			),
			nullProto(),
		},
		nil,
		nil,
	}
)

// Test helpers for getting column values.
func TestColumnValues(t *testing.T) {
	var vals []interface{}
	var wantVals []interface{}

	// Test getting column values.
	for i, wants := range [][]interface{}{
		// STRING / STRING ARRAY
		{"value", NullString{"value", true}},
		{NullString{}},
		{[]NullString{{"value1", true}, {}, {"value3", true}}},
		{[]NullString(nil)},
		// BYTES / BYTES ARRAY
		{[]byte("value")},
		{[]byte(nil)},
		{[][]byte{[]byte("value1"), nil, []byte("value3")}},
		{[][]byte(nil)},
		// INT64 / INT64 ARRAY
		{int64(17), NullInt64{17, true}},
		{NullInt64{}},
		{[]NullInt64{{1, true}, {2, true}, {}}},
		{[]NullInt64(nil)},
		// BOOL / BOOL ARRAY
		{true, NullBool{true, true}},
		{NullBool{}},
		{[]NullBool{{}, {true, true}, {false, true}}},
		{[]NullBool(nil)},
		// FLOAT64 / FLOAT64 ARRAY
		{1.7, NullFloat64{1.7, true}},
		{NullFloat64{}},
		{[]NullFloat64{{}, {}, {1.7, true}}},
		{[]NullFloat64(nil)},
		// TIMESTAMP / TIMESTAMP ARRAY
		{tm, NullTime{tm, true}},
		{NullTime{}},
		{[]NullTime{{}, {tm, true}}},
		{[]NullTime(nil)},
		// DATE / DATE ARRAY
		{dt, NullDate{dt, true}},
		{NullDate{}},
		{[]NullDate{{}, {dt, true}}},
		{[]NullDate(nil)},
		// STRUCT ARRAY
		{
			[]*struct {
				Col1 NullInt64
				Col2 NullFloat64
				Col3 string
			}{
				nil,
				&struct {
					Col1 NullInt64
					Col2 NullFloat64
					Col3 string
				}{
					NullInt64{3, true},
					NullFloat64{33.3, true},
					"three",
				},
				nil,
			},
			[]NullRow{
				{},
				{
					Row: Row{
						fields: []*tspb.StructType_Field{
							mkField("Col1", intType()),
							mkField("Col2", floatType()),
							mkField("Col3", stringType()),
						},
						vals: []*tspb.Value{
							intProto(3),
							floatProto(33.3),
							stringProto("three"),
						},
					},
					Valid: true,
				},
				{},
			},
		},
		{
			[]*struct {
				Col1 NullInt64
				Col2 NullFloat64
				Col3 string
			}(nil),
			[]NullRow(nil),
		},
	} {
		t.Logf("Testing Column(%v): %v", i, row.fields[i])
		for j, want := range wants {
			// Prepare Value vector to test Row.Columns.
			if j == 0 {
				vals = append(vals, reflect.New(reflect.TypeOf(want)).Interface())
				wantVals = append(wantVals, want)
			}
			// Column
			gotp := reflect.New(reflect.TypeOf(want))
			err := row.Column(i, gotp.Interface())
			if err != nil {
				t.Errorf("\t row.Column(%v, %T) returns error: %v, want nil", i, gotp.Interface(), err)
			}
			if got := reflect.Indirect(gotp).Interface(); !reflect.DeepEqual(got, want) {
				t.Errorf("\t row.Column(%v, %T) retrives %v, want %v", i, gotp.Interface(), got, want)
			}
			// ColumnByName
			gotp = reflect.New(reflect.TypeOf(want))
			err = row.ColumnByName(row.fields[i].Name, gotp.Interface())
			if err != nil {
				t.Errorf("\t row.ColumnByName(%v, %T) returns error: %v, want nil", row.fields[i].Name, gotp.Interface(), err)
			}
			if got := reflect.Indirect(gotp).Interface(); !reflect.DeepEqual(got, want) {
				t.Errorf("\t row.ColumnByName(%v, %T) retrives %v, want %v", row.fields[i].Name, gotp.Interface(), got, want)
			}
		}
	}
	// Test Row.Columns.
	if err := row.Columns(vals...); err != nil {
		t.Errorf("row.Columns() returns error: %v, want nil", err)
	}
	for i, want := range wantVals {
		if got := reflect.Indirect(reflect.ValueOf(vals[i])).Interface(); !reflect.DeepEqual(got, want) {
			t.Errorf("\t got %v(%T) for column[%v], want %v(%T)", got, got, row.fields[i].Name, want, want)
		}
	}
}

// Test decoding into nil destination.
func TestNilDst(t *testing.T) {
	for i, test := range []struct {
		r               *Row
		dst             interface{}
		wantErr         error
		structDst       interface{}
		wantToStructErr error
	}{
		{
			&Row{
				[]*tspb.StructType_Field{
					{Name: "Col0", Type: stringType()},
				},
				[]*tspb.Value{stringProto("value")},
				nil,
				nil,
			},
			nil,
			errDecodeColumn(0, errNilDst(nil)),
			nil,
			errToStructArgType(nil),
		},
		{
			&Row{
				[]*tspb.StructType_Field{
					{Name: "Col0", Type: stringType()},
				},
				[]*tspb.Value{stringProto("value")},
				nil,
				nil,
			},
			(*string)(nil),
			errDecodeColumn(0, errNilDst((*string)(nil))),
			(*struct{ STRING string })(nil),
			errNilDst((*struct{ STRING string })(nil)),
		},
		{
			&Row{
				[]*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
							),
						),
					},
				},
				[]*tspb.Value{listProto(
					listProto(intProto(3), floatProto(33.3)),
				)},
				nil,
				nil,
			},
			(*[]*struct {
				Col1 int
				Col2 float64
			})(nil),
			errDecodeColumn(0, errNilDst((*[]*struct {
				Col1 int
				Col2 float64
			})(nil))),
			(*struct {
				StructArray []*struct {
					Col1 int
					Col2 float64
				} `spanner:"STRUCT_ARRAY"`
			})(nil),
			errNilDst((*struct {
				StructArray []*struct {
					Col1 int
					Col2 float64
				} `spanner:"STRUCT_ARRAY"`
			})(nil)),
		},
	} {
		if gotErr := test.r.Column(0, test.dst); !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("%v: test.r.Column() returns error %v, want %v", i, gotErr, test.wantErr)
		}
		if gotErr := test.r.ColumnByName("Col0", test.dst); !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("%v: test.r.ColumnByName() returns error %v, want %v", i, gotErr, test.wantErr)
		}
		// Row.Columns(T) should return nil on T == nil, otherwise, it should return test.wantErr.
		wantColumnsErr := test.wantErr
		if test.dst == nil {
			wantColumnsErr = nil
		}
		if gotErr := test.r.Columns(test.dst); !reflect.DeepEqual(gotErr, wantColumnsErr) {
			t.Errorf("%v: test.r.Columns() returns error %v, want %v", i, gotErr, wantColumnsErr)
		}
		if gotErr := test.r.ToStruct(test.structDst); !reflect.DeepEqual(gotErr, test.wantToStructErr) {
			t.Errorf("%v: test.r.ToStruct() returns error %v, want %v", i, gotErr, test.wantToStructErr)
		}
	}
}

// Test decoding NULL columns using Go types that don't support NULL.
func TestNullTypeErr(t *testing.T) {
	var tm time.Time
	ntoi := func(n string) int {
		for i, f := range row.fields {
			if f.Name == n {
				return i
			}
		}
		t.Errorf("cannot find column name %q in row", n)
		return 0
	}
	for _, test := range []struct {
		colName string
		dst     interface{}
	}{
		{
			"NULL_STRING",
			proto.String(""),
		},
		{
			"NULL_INT64",
			proto.Int64(0),
		},
		{
			"NULL_BOOL",
			proto.Bool(false),
		},
		{
			"NULL_FLOAT64",
			proto.Float64(0.0),
		},
		{
			"NULL_TIMESTAMP",
			&tm,
		},
		{
			"NULL_DATE",
			&dt,
		},
	} {
		wantErr := errDecodeColumn(ntoi(test.colName), errDstNotForNull(test.dst))
		if gotErr := row.ColumnByName(test.colName, test.dst); !reflect.DeepEqual(gotErr, wantErr) {
			t.Errorf("row.ColumnByName(%v) returns error %v, want %v", test.colName, gotErr, wantErr)
		}
	}
}

// Test using wrong destination type in column decoders.
func TestColumnTypeErr(t *testing.T) {
	// badDst cannot hold any of the column values.
	badDst := &struct{}{}
	for i, f := range row.fields { // For each of the columns, try to decode it into badDst.
		tc := f.Type.Code
		isArray := strings.Contains(f.Name, "ARRAY")
		if isArray {
			tc = f.Type.ArrayElementType.Code
		}
		wantErr := errDecodeColumn(i, errTypeMismatch(tc, isArray, badDst))
		if gotErr := row.Column(i, badDst); !reflect.DeepEqual(gotErr, wantErr) {
			t.Errorf("Column(%v): decoding into destination with wrong type %T returns error %v, want %v",
				i, badDst, gotErr, wantErr)
		}
		if gotErr := row.ColumnByName(f.Name, badDst); !reflect.DeepEqual(gotErr, wantErr) {
			t.Errorf("ColumnByName(%v): decoding into destination with wrong type %T returns error %v, want %v",
				f.Name, badDst, gotErr, wantErr)
		}
	}
	wantErr := errDecodeColumn(1, errTypeMismatch(tspb.TypeCode_STRING, false, badDst))
	// badDst is used to receive column 1.
	vals := []interface{}{nil, badDst} // Row.Column() is expected to fail at column 1.
	// Skip decoding the rest columns by providing nils as the destinations.
	for i := 2; i < len(row.fields); i++ {
		vals = append(vals, nil)
	}
	if gotErr := row.Columns(vals...); !reflect.DeepEqual(gotErr, wantErr) {
		t.Errorf("Columns(): decoding column 1 with wrong type %T returns error %v, want %v",
			badDst, gotErr, wantErr)
	}
}

// Test the handling of invalid column decoding requests which cannot be mapped to correct column(s).
func TestInvalidColumnRequest(t *testing.T) {
	for _, test := range []struct {
		desc    string
		f       func() error
		wantErr error
	}{
		{
			"Request column index is out of range",
			func() error {
				return row.Column(10000, &struct{}{})
			},
			errColIdxOutOfRange(10000, &row),
		},
		{
			"Cannot find the named column",
			func() error {
				return row.ColumnByName("string", &struct{}{})
			},
			errColNotFound("string"),
		},
		{
			"Not enough arguments to call row.Columns()",
			func() error {
				return row.Columns(nil, nil)
			},
			errNumOfColValue(2, &row),
		},
		{
			"Call ColumnByName on row with duplicated column names",
			func() error {
				var s string
				r := &Row{
					[]*tspb.StructType_Field{
						{Name: "Val", Type: stringType()},
						{Name: "Val", Type: stringType()},
					},
					[]*tspb.Value{stringProto("value1"), stringProto("value2")},
					nil,
					nil,
				}
				return r.ColumnByName("Val", &s)
			},
			errDupColName("Val"),
		},
		{
			"Call ToStruct on row with duplicated column names",
			func() error {
				s := &struct {
					Val string
				}{}
				r := &Row{
					[]*tspb.StructType_Field{
						{Name: "Val", Type: stringType()},
						{Name: "Val", Type: stringType()},
					},
					[]*tspb.Value{stringProto("value1"), stringProto("value2")},
					nil,
					nil,
				}
				return r.ToStruct(s)
			},
			errDupSpannerField("Val", &tspb.StructType{
				Fields: []*tspb.StructType_Field{
					{Name: "Val", Type: stringType()},
					{Name: "Val", Type: stringType()},
				},
			}),
		},
		{
			"Call ToStruct on a row with unnamed field",
			func() error {
				s := &struct {
					Val string
				}{}
				r := &Row{
					fields: []*tspb.StructType_Field{
						{Name: "", Type: stringType()},
					},
					vals: []*tspb.Value{stringProto("value1")},
				}
				return r.ToStruct(s)
			},
			errUnnamedField(&tspb.StructType{Fields: []*tspb.StructType_Field{{Name: "", Type: stringType()}}}, 0),
		},
	} {
		if gotErr := test.f(); !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("%v: test.f() returns error %v, want %v", test.desc, gotErr, test.wantErr)
		}
	}
}

// Test decoding the row with row.ToStruct into an invalid destination.
func TestToStructInvalidDst(t *testing.T) {
	for _, test := range []struct {
		desc    string
		dst     interface{}
		wantErr error
	}{
		{
			"Decode row as STRUCT into int32",
			proto.Int(1),
			errToStructArgType(proto.Int(1)),
		},
		{
			"Decode row as STRUCT to nil Go struct",
			(*struct{})(nil),
			errNilDst((*struct{})(nil)),
		},
		{
			"Decode row as STRUCT to Go struct with duplicated fields for the PK column",
			&struct {
				PK1 string `spanner:"STRING"`
				PK2 string `spanner:"STRING"`
			}{},
			errNoOrDupGoField(&struct {
				PK1 string `spanner:"STRING"`
				PK2 string `spanner:"STRING"`
			}{}, "STRING"),
		},
		{
			"Decode row as STRUCT to Go struct with no field for the PK column",
			&struct {
				PK1 string `spanner:"_STRING"`
			}{},
			errNoOrDupGoField(&struct {
				PK1 string `spanner:"_STRING"`
			}{}, "STRING"),
		},
		{
			"Decode row as STRUCT to Go struct with wrong type for the PK column",
			&struct {
				PK1 int64 `spanner:"STRING"`
			}{},
			errDecodeStructField(&tspb.StructType{Fields: row.fields}, "STRING",
				errTypeMismatch(tspb.TypeCode_STRING, false, proto.Int64(0))),
		},
	} {
		if gotErr := row.ToStruct(test.dst); !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("%v: decoding:\ngot  %v\nwant %v", test.desc, gotErr, test.wantErr)
		}
	}
}

// Test decoding a broken row.
func TestBrokenRow(t *testing.T) {
	for i, test := range []struct {
		row     *Row
		dst     interface{}
		wantErr error
	}{
		{
			// A row with no field.
			&Row{
				fields: []*tspb.StructType_Field{},
				vals:   []*tspb.Value{stringProto("value")},
			},
			&NullString{"value", true},
			errFieldsMismatchVals(&Row{
				fields: []*tspb.StructType_Field{},
				vals:   []*tspb.Value{stringProto("value")},
			}),
		},
		{
			// A row with nil field.
			&Row{
				fields: []*tspb.StructType_Field{nil},
				vals:   []*tspb.Value{stringProto("value")},
			},
			&NullString{"value", true},
			errNilColType(0),
		},
		{
			// Field is not nil, but its type is nil.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: nil,
					},
				},
				vals: []*tspb.Value{listProto(stringProto("value1"), stringProto("value2"))},
			},
			&[]NullString{},
			errDecodeColumn(0, errNilSpannerType()),
		},
		{
			// Field is not nil, field type is not nil, but it is an array and its array element type is nil.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: &tspb.Type{
							Code: tspb.TypeCode_ARRAY,
						},
					},
				},
				vals: []*tspb.Value{listProto(stringProto("value1"), stringProto("value2"))},
			},
			&[]NullString{},
			errDecodeColumn(0, errNilArrElemType(&tspb.Type{Code: tspb.TypeCode_ARRAY})),
		},
		{
			// Field specifies valid type, value is nil.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: intType(),
					},
				},
				vals: []*tspb.Value{nil},
			},
			&NullInt64{1, true},
			errDecodeColumn(0, errNilSrc()),
		},
		{
			// Field specifies INT64 type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: intType(),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_StringValue)(nil)}},
			},
			&NullInt64{1, true},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_StringValue)(nil)}, "String")),
		},
		{
			// Field specifies INT64 type, but value is for Number type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: intType(),
					},
				},
				vals: []*tspb.Value{floatProto(1.0)},
			},
			&NullInt64{1, true},
			errDecodeColumn(0, errSrcVal(floatProto(1.0), "String")),
		},
		{
			// Field specifies INT64 type, but value is wrongly encoded.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: intType(),
					},
				},
				vals: []*tspb.Value{stringProto("&1")},
			},
			proto.Int64(0),
			errDecodeColumn(0, errBadEncoding(stringProto("&1"), func() error {
				_, err := strconv.ParseInt("&1", 10, 64)
				return err
			}())),
		},
		{
			// Field specifies INT64 type, but value is wrongly encoded.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: intType(),
					},
				},
				vals: []*tspb.Value{stringProto("&1")},
			},
			&NullInt64{},
			errDecodeColumn(0, errBadEncoding(stringProto("&1"), func() error {
				_, err := strconv.ParseInt("&1", 10, 64)
				return err
			}())),
		},
		{
			// Field specifies STRING type, but value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: stringType(),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_StringValue)(nil)}},
			},
			&NullString{"value", true},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_StringValue)(nil)}, "String")),
		},
		{
			// Field specifies STRING type, but value is for ARRAY type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: stringType(),
					},
				},
				vals: []*tspb.Value{listProto(stringProto("value"))},
			},
			&NullString{"value", true},
			errDecodeColumn(0, errSrcVal(listProto(stringProto("value")), "String")),
		},
		{
			// Field specifies FLOAT64 type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: floatType(),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_NumberValue)(nil)}},
			},
			&NullFloat64{1.0, true},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_NumberValue)(nil)}, "Number")),
		},
		{
			// Field specifies FLOAT64 type, but value is for BOOL type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: floatType(),
					},
				},
				vals: []*tspb.Value{boolProto(true)},
			},
			&NullFloat64{1.0, true},
			errDecodeColumn(0, errSrcVal(boolProto(true), "Number")),
		},
		{
			// Field specifies FLOAT64 type, but value is wrongly encoded.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: floatType(),
					},
				},
				vals: []*tspb.Value{stringProto("nan")},
			},
			&NullFloat64{},
			errDecodeColumn(0, errUnexpectedNumStr("nan")),
		},
		{
			// Field specifies FLOAT64 type, but value is wrongly encoded.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: floatType(),
					},
				},
				vals: []*tspb.Value{stringProto("nan")},
			},
			proto.Float64(0),
			errDecodeColumn(0, errUnexpectedNumStr("nan")),
		},
		{
			// Field specifies BYTES type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: bytesType(),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_StringValue)(nil)}},
			},
			&[]byte{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_StringValue)(nil)}, "String")),
		},
		{
			// Field specifies BYTES type, but value is for BOOL type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: bytesType(),
					},
				},
				vals: []*tspb.Value{boolProto(false)},
			},
			&[]byte{},
			errDecodeColumn(0, errSrcVal(boolProto(false), "String")),
		},
		{
			// Field specifies BYTES type, but value is wrongly encoded.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: bytesType(),
					},
				},
				vals: []*tspb.Value{stringProto("&&")},
			},
			&[]byte{},
			errDecodeColumn(0, errBadEncoding(stringProto("&&"), func() error {
				_, err := base64.StdEncoding.DecodeString("&&")
				return err
			}())),
		},
		{
			// Field specifies BOOL type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: boolType(),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_BoolValue)(nil)}},
			},
			&NullBool{false, true},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_BoolValue)(nil)}, "Bool")),
		},
		{
			// Field specifies BOOL type, but value is for STRING type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: boolType(),
					},
				},
				vals: []*tspb.Value{stringProto("false")},
			},
			&NullBool{false, true},
			errDecodeColumn(0, errSrcVal(stringProto("false"), "Bool")),
		},
		{
			// Field specifies TIMESTAMP type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: timeType(),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_StringValue)(nil)}},
			},
			&NullTime{time.Now(), true},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_StringValue)(nil)}, "String")),
		},
		{
			// Field specifies TIMESTAMP type, but value is for BOOL type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: timeType(),
					},
				},
				vals: []*tspb.Value{boolProto(false)},
			},
			&NullTime{time.Now(), true},
			errDecodeColumn(0, errSrcVal(boolProto(false), "String")),
		},
		{
			// Field specifies TIMESTAMP type, but value is invalid timestamp.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: timeType(),
					},
				},
				vals: []*tspb.Value{stringProto("junk")},
			},
			&NullTime{time.Now(), true},
			errDecodeColumn(0, errBadEncoding(stringProto("junk"), func() error {
				_, err := time.Parse(time.RFC3339Nano, "junk")
				return err
			}())),
		},
		{
			// Field specifies DATE type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: dateType(),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_StringValue)(nil)}},
			},
			&NullDate{civil.Date{}, true},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_StringValue)(nil)}, "String")),
		},
		{
			// Field specifies DATE type, but value is for BOOL type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: dateType(),
					},
				},
				vals: []*tspb.Value{boolProto(false)},
			},
			&NullDate{civil.Date{}, true},
			errDecodeColumn(0, errSrcVal(boolProto(false), "String")),
		},
		{
			// Field specifies DATE type, but value is invalid timestamp.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: dateType(),
					},
				},
				vals: []*tspb.Value{stringProto("junk")},
			},
			&NullDate{civil.Date{}, true},
			errDecodeColumn(0, errBadEncoding(stringProto("junk"), func() error {
				_, err := civil.ParseDate("junk")
				return err
			}())),
		},

		{
			// Field specifies ARRAY<INT64> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(intType()),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[]NullInt64{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<INT64> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(intType()),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]NullInt64{},
			errDecodeColumn(0, errNilListValue("INT64")),
		},
		{
			// Field specifies ARRAY<INT64> type, but value is for BYTES type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(intType()),
					},
				},
				vals: []*tspb.Value{bytesProto([]byte("value"))},
			},
			&[]NullInt64{},
			errDecodeColumn(0, errSrcVal(bytesProto([]byte("value")), "List")),
		},
		{
			// Field specifies ARRAY<INT64> type, but value is for ARRAY<BOOL> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(intType()),
					},
				},
				vals: []*tspb.Value{listProto(boolProto(true))},
			},
			&[]NullInt64{},
			errDecodeColumn(0, errDecodeArrayElement(0, boolProto(true),
				"INT64", errSrcVal(boolProto(true), "String"))),
		},
		{
			// Field specifies ARRAY<STRING> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(stringType()),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[]NullString{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<STRING> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(stringType()),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]NullString{},
			errDecodeColumn(0, errNilListValue("STRING")),
		},
		{
			// Field specifies ARRAY<STRING> type, but value is for BOOL type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(stringType()),
					},
				},
				vals: []*tspb.Value{boolProto(true)},
			},
			&[]NullString{},
			errDecodeColumn(0, errSrcVal(boolProto(true), "List")),
		},
		{
			// Field specifies ARRAY<STRING> type, but value is for ARRAY<BOOL> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(stringType()),
					},
				},
				vals: []*tspb.Value{listProto(boolProto(true))},
			},
			&[]NullString{},
			errDecodeColumn(0, errDecodeArrayElement(0, boolProto(true),
				"STRING", errSrcVal(boolProto(true), "String"))),
		},
		{
			// Field specifies ARRAY<FLOAT64> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(floatType()),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[]NullFloat64{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<FLOAT64> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(floatType()),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]NullFloat64{},
			errDecodeColumn(0, errNilListValue("FLOAT64")),
		},
		{
			// Field specifies ARRAY<FLOAT64> type, but value is for STRING type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(floatType()),
					},
				},
				vals: []*tspb.Value{stringProto("value")},
			},
			&[]NullFloat64{},
			errDecodeColumn(0, errSrcVal(stringProto("value"), "List")),
		},
		{
			// Field specifies ARRAY<FLOAT64> type, but value is for ARRAY<BOOL> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(floatType()),
					},
				},
				vals: []*tspb.Value{listProto(boolProto(true))},
			},
			&[]NullFloat64{},
			errDecodeColumn(0, errDecodeArrayElement(0, boolProto(true),
				"FLOAT64", errSrcVal(boolProto(true), "Number"))),
		},
		{
			// Field specifies ARRAY<BYTES> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(bytesType()),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[][]byte{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<BYTES> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(bytesType()),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[][]byte{},
			errDecodeColumn(0, errNilListValue("BYTES")),
		},
		{
			// Field specifies ARRAY<BYTES> type, but value is for FLOAT64 type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(bytesType()),
					},
				},
				vals: []*tspb.Value{floatProto(1.0)},
			},
			&[][]byte{},
			errDecodeColumn(0, errSrcVal(floatProto(1.0), "List")),
		},
		{
			// Field specifies ARRAY<BYTES> type, but value is for ARRAY<FLOAT64> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(bytesType()),
					},
				},
				vals: []*tspb.Value{listProto(floatProto(1.0))},
			},
			&[][]byte{},
			errDecodeColumn(0, errDecodeArrayElement(0, floatProto(1.0),
				"BYTES", errSrcVal(floatProto(1.0), "String"))),
		},
		{
			// Field specifies ARRAY<BOOL> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(boolType()),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[]NullBool{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<BOOL> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(boolType()),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]NullBool{},
			errDecodeColumn(0, errNilListValue("BOOL")),
		},
		{
			// Field specifies ARRAY<BOOL> type, but value is for FLOAT64 type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(boolType()),
					},
				},
				vals: []*tspb.Value{floatProto(1.0)},
			},
			&[]NullBool{},
			errDecodeColumn(0, errSrcVal(floatProto(1.0), "List")),
		},
		{
			// Field specifies ARRAY<BOOL> type, but value is for ARRAY<FLOAT64> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(boolType()),
					},
				},
				vals: []*tspb.Value{listProto(floatProto(1.0))},
			},
			&[]NullBool{},
			errDecodeColumn(0, errDecodeArrayElement(0, floatProto(1.0),
				"BOOL", errSrcVal(floatProto(1.0), "Bool"))),
		},
		{
			// Field specifies ARRAY<TIMESTAMP> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(timeType()),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[]NullTime{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<TIMESTAMP> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(timeType()),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]NullTime{},
			errDecodeColumn(0, errNilListValue("TIMESTAMP")),
		},
		{
			// Field specifies ARRAY<TIMESTAMP> type, but value is for FLOAT64 type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(timeType()),
					},
				},
				vals: []*tspb.Value{floatProto(1.0)},
			},
			&[]NullTime{},
			errDecodeColumn(0, errSrcVal(floatProto(1.0), "List")),
		},
		{
			// Field specifies ARRAY<TIMESTAMP> type, but value is for ARRAY<FLOAT64> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(timeType()),
					},
				},
				vals: []*tspb.Value{listProto(floatProto(1.0))},
			},
			&[]NullTime{},
			errDecodeColumn(0, errDecodeArrayElement(0, floatProto(1.0),
				"TIMESTAMP", errSrcVal(floatProto(1.0), "String"))),
		},
		{
			// Field specifies ARRAY<DATE> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(dateType()),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[]NullDate{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<DATE> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(dateType()),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]NullDate{},
			errDecodeColumn(0, errNilListValue("DATE")),
		},
		{
			// Field specifies ARRAY<DATE> type, but value is for FLOAT64 type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(dateType()),
					},
				},
				vals: []*tspb.Value{floatProto(1.0)},
			},
			&[]NullDate{},
			errDecodeColumn(0, errSrcVal(floatProto(1.0), "List")),
		},
		{
			// Field specifies ARRAY<DATE> type, but value is for ARRAY<FLOAT64> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(dateType()),
					},
				},
				vals: []*tspb.Value{listProto(floatProto(1.0))},
			},
			&[]NullDate{},
			errDecodeColumn(0, errDecodeArrayElement(0, floatProto(1.0),
				"DATE", errSrcVal(floatProto(1.0), "String"))),
		},
		{
			// Field specifies ARRAY<STRUCT> type, value is having a nil Kind.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							),
						),
					},
				},
				vals: []*tspb.Value{{Kind: (*tspb.Value_ListValue)(nil)}},
			},
			&[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}{},
			errDecodeColumn(0, errSrcVal(&tspb.Value{Kind: (*tspb.Value_ListValue)(nil)}, "List")),
		},
		{
			// Field specifies ARRAY<STRUCT> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							),
						),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}{},
			errDecodeColumn(0, errNilListValue("STRUCT")),
		},
		{
			// Field specifies ARRAY<STRUCT> type, value is having a nil ListValue.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							),
						),
					},
				},
				vals: []*tspb.Value{{Kind: &tspb.Value_ListValue{}}},
			},
			&[]NullRow{},
			errDecodeColumn(0, errNilListValue("STRUCT")),
		},
		{
			// Field specifies ARRAY<STRUCT> type, value is for BYTES type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							),
						),
					},
				},
				vals: []*tspb.Value{bytesProto([]byte("value"))},
			},
			&[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}{},
			errDecodeColumn(0, errSrcVal(bytesProto([]byte("value")), "List")),
		},
		{
			// Field specifies ARRAY<STRUCT> type, value is for BYTES type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							),
						),
					},
				},
				vals: []*tspb.Value{listProto(bytesProto([]byte("value")))},
			},
			&[]NullRow{},
			errDecodeColumn(0, errNotStructElement(0, bytesProto([]byte("value")))),
		},
		{
			// Field specifies ARRAY<STRUCT> type, value is for ARRAY<BYTES> type.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							),
						),
					},
				},
				vals: []*tspb.Value{listProto(bytesProto([]byte("value")))},
			},
			&[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}{},
			errDecodeColumn(0, errDecodeArrayElement(0, bytesProto([]byte("value")),
				"STRUCT", errSrcVal(bytesProto([]byte("value")), "List"))),
		},
		{
			// Field specifies ARRAY<STRUCT>, but is having nil StructType.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							&tspb.Type{Code: tspb.TypeCode_STRUCT},
						),
					},
				},
				vals: []*tspb.Value{listProto(listProto(intProto(1), floatProto(2.0), stringProto("3")))},
			},
			&[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}{},
			errDecodeColumn(0, errDecodeArrayElement(0, listProto(intProto(1), floatProto(2.0), stringProto("3")),
				"STRUCT", errNilSpannerStructType())),
		},
		{
			// Field specifies ARRAY<STRUCT>, but the second struct value is for BOOL type instead of FLOAT64.
			&Row{
				fields: []*tspb.StructType_Field{
					{
						Name: "Col0",
						Type: listType(
							structType(
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							),
						),
					},
				},
				vals: []*tspb.Value{listProto(listProto(intProto(1), boolProto(true), stringProto("3")))},
			},
			&[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}{},
			errDecodeColumn(
				0,
				errDecodeArrayElement(
					0, listProto(intProto(1), boolProto(true), stringProto("3")), "STRUCT",
					errDecodeStructField(
						&tspb.StructType{
							Fields: []*tspb.StructType_Field{
								mkField("Col1", intType()),
								mkField("Col2", floatType()),
								mkField("Col3", stringType()),
							},
						},
						"Col2",
						errSrcVal(boolProto(true), "Number"),
					),
				),
			),
		},
	} {
		if gotErr := test.row.Column(0, test.dst); !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("%v: test.row.Column(0) got error %v, want %v", i, gotErr, test.wantErr)
		}
		if gotErr := test.row.ColumnByName("Col0", test.dst); !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("%v: test.row.ColumnByName(%q) got error %v, want %v", i, "Col0", gotErr, test.wantErr)
		}
		if gotErr := test.row.Columns(test.dst); !reflect.DeepEqual(gotErr, test.wantErr) {
			t.Errorf("%v: test.row.Columns(%T) got error %v, want %v", i, test.dst, gotErr, test.wantErr)
		}
	}
}

// Test Row.ToStruct().
func TestToStruct(t *testing.T) {
	s := []struct {
		// STRING / STRING ARRAY
		PrimaryKey      string       `spanner:"STRING"`
		NullString      NullString   `spanner:"NULL_STRING"`
		StringArray     []NullString `spanner:"STRING_ARRAY"`
		NullStringArray []NullString `spanner:"NULL_STRING_ARRAY"`
		// BYTES / BYTES ARRAY
		Bytes          []byte   `spanner:"BYTES"`
		NullBytes      []byte   `spanner:"NULL_BYTES"`
		BytesArray     [][]byte `spanner:"BYTES_ARRAY"`
		NullBytesArray [][]byte `spanner:"NULL_BYTES_ARRAY"`
		// INT64 / INT64 ARRAY
		Int64          int64       `spanner:"INT64"`
		NullInt64      NullInt64   `spanner:"NULL_INT64"`
		Int64Array     []NullInt64 `spanner:"INT64_ARRAY"`
		NullInt64Array []NullInt64 `spanner:"NULL_INT64_ARRAY"`
		// BOOL / BOOL ARRAY
		Bool          bool       `spanner:"BOOL"`
		NullBool      NullBool   `spanner:"NULL_BOOL"`
		BoolArray     []NullBool `spanner:"BOOL_ARRAY"`
		NullBoolArray []NullBool `spanner:"NULL_BOOL_ARRAY"`
		// FLOAT64 / FLOAT64 ARRAY
		Float64          float64       `spanner:"FLOAT64"`
		NullFloat64      NullFloat64   `spanner:"NULL_FLOAT64"`
		Float64Array     []NullFloat64 `spanner:"FLOAT64_ARRAY"`
		NullFloat64Array []NullFloat64 `spanner:"NULL_FLOAT64_ARRAY"`
		// TIMESTAMP / TIMESTAMP ARRAY
		Timestamp          time.Time  `spanner:"TIMESTAMP"`
		NullTimestamp      NullTime   `spanner:"NULL_TIMESTAMP"`
		TimestampArray     []NullTime `spanner:"TIMESTAMP_ARRAY"`
		NullTimestampArray []NullTime `spanner:"NULL_TIMESTAMP_ARRAY"`
		// DATE / DATE ARRAY
		Date          civil.Date `spanner:"DATE"`
		NullDate      NullDate   `spanner:"NULL_DATE"`
		DateArray     []NullDate `spanner:"DATE_ARRAY"`
		NullDateArray []NullDate `spanner:"NULL_DATE_ARRAY"`

		// STRUCT ARRAY
		StructArray []*struct {
			Col1 int64
			Col2 float64
			Col3 string
		} `spanner:"STRUCT_ARRAY"`
		NullStructArray []*struct {
			Col1 int64
			Col2 float64
			Col3 string
		} `spanner:"NULL_STRUCT_ARRAY"`
	}{
		{}, // got
		{
			// STRING / STRING ARRAY
			"value",
			NullString{},
			[]NullString{{"value1", true}, {}, {"value3", true}},
			[]NullString(nil),
			// BYTES / BYTES ARRAY
			[]byte("value"),
			[]byte(nil),
			[][]byte{[]byte("value1"), nil, []byte("value3")},
			[][]byte(nil),
			// INT64 / INT64 ARRAY
			int64(17),
			NullInt64{},
			[]NullInt64{{int64(1), true}, {int64(2), true}, {}},
			[]NullInt64(nil),
			// BOOL / BOOL ARRAY
			true,
			NullBool{},
			[]NullBool{{}, {true, true}, {false, true}},
			[]NullBool(nil),
			// FLOAT64 / FLOAT64 ARRAY
			1.7,
			NullFloat64{},
			[]NullFloat64{{}, {}, {1.7, true}},
			[]NullFloat64(nil),
			// TIMESTAMP / TIMESTAMP ARRAY
			tm,
			NullTime{},
			[]NullTime{{}, {tm, true}},
			[]NullTime(nil),
			// DATE / DATE ARRAY
			dt,
			NullDate{},
			[]NullDate{{}, {dt, true}},
			[]NullDate(nil),
			// STRUCT ARRAY
			[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}{
				nil,
				&struct {
					Col1 int64
					Col2 float64
					Col3 string
				}{3, 33.3, "three"},
				nil,
			},
			[]*struct {
				Col1 int64
				Col2 float64
				Col3 string
			}(nil),
		}, // want
	}
	err := row.ToStruct(&s[0])
	if err != nil {
		t.Errorf("row.ToStruct() returns error: %v, want nil", err)
	}
	if !reflect.DeepEqual(s[0], s[1]) {
		t.Errorf("row.ToStruct() fetches struct %v, want %v", s[0], s[1])
	}
}

// Test helpers for getting column names.
func TestColumnNameAndIndex(t *testing.T) {
	// Test Row.Size().
	if rs := row.Size(); rs != len(row.fields) {
		t.Errorf("row.Size() returns %v, want %v", rs, len(row.fields))
	}
	// Test Row.Size() on empty Row.
	if rs := (&Row{}).Size(); rs != 0 {
		t.Errorf("empty_row.Size() returns %v, want %v", rs, 0)
	}
	// Test Row.ColumnName()
	for i, col := range row.fields {
		if cn := row.ColumnName(i); cn != col.Name {
			t.Errorf("row.ColumnName(%v) returns %q, want %q", i, cn, col.Name)
		}
		goti, err := row.ColumnIndex(col.Name)
		if err != nil {
			t.Errorf("ColumnIndex(%q) error %v", col.Name, err)
			continue
		}
		if goti != i {
			t.Errorf("ColumnIndex(%q) = %d, want %d", col.Name, goti, i)
		}
	}
	// Test Row.ColumnName on empty Row.
	if cn := (&Row{}).ColumnName(0); cn != "" {
		t.Errorf("empty_row.ColumnName(%v) returns %q, want %q", 0, cn, "")
	}
	// Test Row.ColumnIndex on empty Row.
	if _, err := (&Row{}).ColumnIndex(""); err == nil {
		t.Error("empty_row.ColumnIndex returns nil, want error")
	}
}

func TestNewRow(t *testing.T) {
	for _, test := range []struct {
		names   []string
		values  []interface{}
		want    *Row
		wantErr error
	}{
		{
			want: &Row{fields: []*tspb.StructType_Field{}, vals: []*tspb.Value{}},
		},
		{
			names:  []string{},
			values: []interface{}{},
			want:   &Row{fields: []*tspb.StructType_Field{}, vals: []*tspb.Value{}},
		},
		{
			names:   []string{"a", "b"},
			values:  []interface{}{},
			want:    nil,
			wantErr: errNamesValuesMismatch([]string{"a", "b"}, []interface{}{}),
		},
		{
			names:  []string{"a", "b", "c"},
			values: []interface{}{5, "abc", GenericColumnValue{listType(intType()), listProto(intProto(91), nullProto(), intProto(87))}},
			want: &Row{
				fields: []*tspb.StructType_Field{
					{Name: "a", Type: intType()},
					{Name: "b", Type: stringType()},
					{Name: "c", Type: listType(intType())},
				},
				vals: []*tspb.Value{
					intProto(5),
					stringProto("abc"),
					listProto(intProto(91), nullProto(), intProto(87)),
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			got, err := NewRow(test.names, test.values)
			if !reflect.DeepEqual(err, test.wantErr) {
				t.Fatalf("NewRow(%v,%v).err = %s, want %s", test.names, test.values, err, test.wantErr)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("NewRow(%v,%v) = %s, want %s", test.names, test.values, got, test.want)
			}
		})
	}
}
