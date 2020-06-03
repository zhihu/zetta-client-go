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

import tspb "github.com/zhihu/zetta-proto/pkg/tablestore"

type SparseRow struct {
	Keys      Key
	Qualifier []string
}

func (sr *SparseRow) proto() (*tspb.Row, error) {
	keys, err := sr.Keys.proto()
	if err != nil {
		return nil, err
	}
	row := &tspb.Row{
		Keys:       keys,
		Qualifiers: sr.Qualifier,
	}
	return row, nil
}

type SparseResultSet struct {
	Rows []*Row
}

func BuildSparseResultSet(r *tspb.ResultSet) *SparseResultSet {
	sr := &SparseResultSet{
		Rows: []*Row{},
	}
	for _, srow := range r.SliceRows {
		row := &Row{
			primaryKeys: []*tspb.Value{},
			cells:       []*tspb.Cell{},
		}
		for _, pkey := range srow.PrimaryKeys {
			row.primaryKeys = append(row.primaryKeys, pkey)
		}
		for _, cell := range srow.Cells {
			row.cells = append(row.cells, cell)
		}
		sr.Rows = append(sr.Rows, row)
	}
	return sr
}
