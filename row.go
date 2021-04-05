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
	"fmt"
	"reflect"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"google.golang.org/grpc/codes"
)

// Row 结构描述读取到的一行
// A Row is a view of a row of data produced by a Cloud Spanner read.
//
// 一行包含多列，列数取决于构造读的结构
// A row consists of a number of columns; the number depends on the columns
// used to construct the read.
//
// 列值可以通过索引读取
// The column values can be accessed by index, where the indices are with
// respect to the columns. For instance, if the read specified
// []string{"photo_id", "caption", "metadata"}, then each row will
// contain three columns: the 0th column corresponds to "photo_id", the
// 1st column corresponds to "caption", etc.
//
//
// 列的值通过 Column(), Columns() 等方法将指定索引的列解码到 Go 变量中
// Column values are decoded by using one of the Column, ColumnByName, or
// Columns methods. The valid values passed to these methods depend on the
// column type. For example:
//
//	var photoID int64
//	err := row.Column(0, &photoID) // Decode column 0 as an integer.
//
//	var caption string
//	err := row.Column(1, &caption) // Decode column 1 as a string.
//
//	// The above two operations at once.
//	err := row.Columns(&photoID, &caption)
//
// 已支持的类型有：
// Supported types and their corresponding Cloud Spanner column type(s) are:
//
//	*string(not NULL), *NullString - STRING
//	*[]NullString - STRING ARRAY
//	*[]byte - BYTES
//	*[][]byte - BYTES ARRAY
//	*int64(not NULL), *NullInt64 - INT64
//	*[]NullInt64 - INT64 ARRAY
//	*bool(not NULL), *NullBool - BOOL
//	*[]NullBool - BOOL ARRAY
//	*float64(not NULL), *NullFloat64 - FLOAT64
//	*[]NullFloat64 - FLOAT64 ARRAY
//	*time.Time(not NULL), *NullTime - TIMESTAMP
//	*[]NullTime - TIMESTAMP ARRAY
//	*Date(not NULL), *NullDate - DATE
//	*[]NullDate - DATE ARRAY
//	*[]*some_go_struct, *[]NullRow - STRUCT ARRAY
//	*GenericColumnValue - any Cloud Spanner type
//
// For TIMESTAMP columns, returned time.Time object will be in UTC.
//
// To fetch an array of BYTES, pass a *[][]byte. To fetch an array of
// (sub)rows, pass a *[]spanner.NullRow or a *[]*some_go_struct where
// some_go_struct holds all information of the subrow, see spannr.Row.ToStruct
// for the mapping between Cloud Spanner row and Go struct. To fetch an array of
// other types, pass a *[]spanner.Null* type of the appropriate type.  Use
// *GenericColumnValue when you don't know in advance what column type to
// expect.
//
// Row decodes the row contents lazily; as a result, each call to a getter has
// a chance of returning an error.
//
// A column value may be NULL if the corresponding value is not present in
// Cloud Spanner. The spanner.Null* types (spanner.NullInt64 et al.) allow fetching
// values that may be null. A NULL BYTES can be fetched into a *[]byte as nil.
// It is an error to fetch a NULL value into any other type.
type Row struct {
	fields      []*tspb.StructType_Field // 列名
	vals        []*tspb.Value            // 列值
	cells       []*tspb.Cell
	primaryKeys []*tspb.Value
}

// errNamesValuesMismatch returns error for when columnNames count is not equal
// to columnValues count.
func errNamesValuesMismatch(columnNames []string, columnValues []interface{}) error {
	return wrapError(codes.FailedPrecondition,
		"different number of names(%v) and values(%v)", len(columnNames), len(columnValues))
}

// 测试用
// NewRow returns a Row containing the supplied data.  This can be useful for
// mocking Cloud Spanner Read and Query responses for unit testing.
func NewRow(columnNames []string, columnValues []interface{}) (*Row, error) {
	if len(columnValues) != len(columnNames) {
		return nil, errNamesValuesMismatch(columnNames, columnValues)
	}
	r := Row{
		fields: make([]*tspb.StructType_Field, len(columnValues)),
		vals:   make([]*tspb.Value, len(columnValues)),
	}
	for i := range columnValues {
		val, typ, err := encodeValue(columnValues[i])
		if err != nil {
			return nil, err
		}
		r.fields[i] = &tspb.StructType_Field{
			Name: columnNames[i],
			Type: typ,
		}
		r.vals[i] = val
	}
	return &r, nil
}

// Size is the number of columns in the row.
func (r *Row) Size() int {
	return len(r.fields)
}

// 返回列名
// ColumnName returns the name of column i, or empty string for invalid column.
func (r *Row) ColumnName(i int) string {
	if i < 0 || i >= len(r.cells) {
		return ""
	}
	return getColumnName(r.cells[i].Family, r.cells[i].Column)
}

// 大小写敏感地返回列名索引
// ColumnIndex returns the index of the column with the given name. The
// comparison is case-sensitive.
func (r *Row) ColumnIndex(name string) (int, error) {
	found := false
	var index int
	for i, cell := range r.cells {
		coln := getColumnName(cell.Family, cell.Column)
		if name == coln {
			if found {
				return 0, errDupColName(name)
			}
			found = true
			index = i
		}
	}
	if !found {
		return 0, errColNotFound(name)
	}
	return index, nil
}

// 返回所有的列名
// ColumnNames returns all column names of the row.
func (r *Row) ColumnNames() []string {
	var n []string
	// for _, c := range r.fields {
	// 	n = append(n, c.Name)
	// }
	for _, cell := range r.cells {
		coln := getColumnName(cell.Family, cell.Column)
		n = append(n, coln)
	}
	return n
}

func (r *Row) SparseQualifierIndex(name string) (int, error) {
	found := false
	var index int
	for i, cell := range r.cells {
		coln := cell.Column
		if name == coln {
			if found {
				return 0, errDupColName(name)
			}
			found = true
			index = i
		}
	}
	if !found {
		return 0, errColNotFound(name)
	}
	return index, nil
}

func (r *Row) SparseQualifiers() []string {
	var n []string
	for _, cell := range r.cells {
		n = append(n, cell.Column)
	}
	return n
}

func (r *Row) SparseRowKeys() []interface{} {
	keys := []interface{}{}
	for _, pkey := range r.primaryKeys {
		switch pkey.Kind.(type) {
		case *tspb.Value_BoolValue:
			keys = append(keys, pkey.GetBoolValue())
		case *tspb.Value_BytesValue:
			keys = append(keys, pkey.GetBytesValue())
		case *tspb.Value_NumberValue:
			keys = append(keys, pkey.GetNumberValue())
		case *tspb.Value_IntegerValue:
			keys = append(keys, pkey.GetIntegerValue())
		case *tspb.Value_StringValue:
			keys = append(keys, pkey.GetStringValue())
		}
	}
	return keys
}

// errColIdxOutOfRange returns error for requested column index is out of the
// range of the target Row's columns.
func errColIdxOutOfRange(i int, r *Row) error {
	return wrapError(codes.OutOfRange, "column index %d out of range [0,%d)", i, len(r.vals))
}

// errDecodeColumn returns error for not being able to decode a indexed column.
func errDecodeColumn(i int, err error) error {
	if err == nil {
		return nil
	}
	se, ok := err.(*Error)
	if !ok {
		return wrapError(codes.InvalidArgument, "failed to decode column %v, error = <%v>", i, err)
	}
	se.decorate(fmt.Sprintf("failed to decode column %v", i))
	return se
}

// errFieldsMismatchVals returns error for field count isn't equal to value count in a Row.
func errFieldsMismatchVals(r *Row) error {
	return wrapError(codes.FailedPrecondition, "row has different number of fields(%v) and values(%v)",
		len(r.fields), len(r.vals))
}

// errNilColType returns error for column type for column i being nil in the row.
func errNilColType(i int) error {
	return wrapError(codes.FailedPrecondition, "column(%v)'s type is nil", i)
}

// 将 row 的第 i 行 decode 到 ptr 指针变量中
// Column fetches the value from the ith column, decoding it into ptr.
func (r *Row) Column(i int, ptr interface{}) error {
	// if len(r.vals) != len(r.fields) {
	// 	return errFieldsMismatchVals(r)
	// }
	if i < 0 || i >= len(r.cells) {
		return errColIdxOutOfRange(i, r)
	}
	// if r.fields[i] == nil {
	// 	return errNilColType(i)
	// }
	if err := decodeValue(r.cells[i].Value, r.cells[i].Type, ptr); err != nil {
		return errDecodeColumn(i, err)
	}
	return nil
}

// errDupColName returns error for duplicated column name in the same row.
func errDupColName(n string) error {
	return wrapError(codes.FailedPrecondition, "ambiguous column name %q", n)
}

// errColNotFound returns error for not being able to find a named column.
func errColNotFound(n string) error {
	return wrapError(codes.NotFound, "column %q not found", n)
}

// 将 row 中指定列名的值 decode 到 ptr 指针中
// ColumnByName fetches the value from the named column, decoding it into ptr.
func (r *Row) ColumnByName(name string, ptr interface{}) error {

	index, err := r.ColumnIndex(name)
	if err != nil {
		return err
	}
	return r.Column(index, ptr)
}

// errNumOfColValue returns error for providing wrong number of values to Columns.
func errNumOfColValue(n int, r *Row) error {
	return wrapError(codes.InvalidArgument,
		"Columns(): number of arguments (%d) does not match row size (%d)", n, len(r.vals))
}

//
// 一次性将 row 中尽可能多的列 decode 到对应的 ptr 指针中
// Columns fetches all the columns in the row at once.
//
// The value of the kth column will be decoded into the kth argument to
// Columns.  See above for the list of acceptable argument tspb. The number of
// arguments must be equal to the number of columns. Pass nil to specify that a
// column should be ignored.
func (r *Row) Columns(ptrs ...interface{}) error {
	if len(ptrs) != len(r.cells) {
		return errNumOfColValue(len(ptrs), r)
	}
	// if len(r.vals) != len(r.fields) {
	// 	return errFieldsMismatchVals(r)
	// }
	for i, p := range ptrs {
		if p == nil {
			continue
		}
		if err := r.Column(i, p); err != nil {
			return err
		}
	}
	return nil
}

// errToStructArgType returns error for p not having the correct data type(pointer to Go struct) to
// be the argument of Row.ToStruct.
func errToStructArgType(p interface{}) error {
	return wrapError(codes.InvalidArgument, "ToStruct(): type %T is not a valid pointer to Go struct", p)
}

// 将一整行直接忽略大小写顺序 decode 到某个 struct 中，注意匹配 spanner 的 tag
// ToStruct fetches the columns in a row into the fields of a struct.
// The rules for mapping a row's columns into a struct's exported fields
// are as the following:
//   1. If a field has a `spanner: "column_name"` tag, then decode column
//      'column_name' into the field. A special case is the `spanner: "-"`
//      tag, which instructs ToStruct to ignore the field during decoding.
//   2. Otherwise, if the name of a field matches the name of a column (ignoring case),
//      decode the column into the field.
//
// The fields of the destination struct can be of any type that is acceptable
// to (*spanner.Row).Column.
//
// Slice and pointer fields will be set to nil if the source column
// is NULL, and a non-nil value if the column is not NULL. To decode NULL
// values of other types, use one of the spanner.Null* as the type of the
// destination field.
func (r *Row) ToStruct(p interface{}) error {
	// Check if p is a pointer to a struct
	if t := reflect.TypeOf(p); t == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return errToStructArgType(p)
	}
	if len(r.vals) != len(r.fields) {
		return errFieldsMismatchVals(r)
	}
	// Call decodeStruct directly to decode the row as a typed proto.ListValue.
	return decodeStruct(
		&tspb.StructType{Fields: r.fields},
		&tspb.ListValue{Values: r.vals},
		p,
	)
}

func (r *Row) ConvertToStruct(p interface{}) error {
	// Check if p is a pointer to a struct
	if t := reflect.TypeOf(p); t == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return errToStructArgType(p)
	}
	return decodeCellStruct(r.cells, p)
}

func decodeCellStruct(cells []*tspb.Cell, ptr interface{}) error {
	if reflect.ValueOf(ptr).IsNil() {
		return errNilDst(ptr)
	}
	if cells == nil {
		return errNilSpannerStructType()
	}
	// t holds the structual information of ptr.
	t := reflect.TypeOf(ptr).Elem()
	// v is the actual value that ptr points to.
	v := reflect.ValueOf(ptr).Elem()

	fields, err := fieldCache.Fields(t)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for i, f := range cells {
		column := getColumnName(f.Family, f.Column)
		if column == "" {
			return errUnnamedCellField(f, i)

		}
		sf := fields.Match(column)
		if sf == nil {
			return errNoOrDupGoField(ptr, column)
		}
		if seen[column] {
			// We don't allow duplicated field name.
			return errDupCellField(column, f)
		}
		// Try to decode a single field.
		if err := decodeValue(f.Value, f.Type, v.FieldByIndex(sf.Index).Addr().Interface()); err != nil {
			return errDecodeCellField(f, column, err)
		}
		// Mark field f.Name as processed.
		seen[column] = true
	}
	return nil
}

func getColumnName(family, qualifier string) string {
	column := qualifier
	if family != "" && family != "default" {
		column = family + ":" + column
	}
	return column
}
