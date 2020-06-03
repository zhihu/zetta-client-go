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

	"github.com/zhihu/zetta-client-go/utils/retry"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

/*
 * table operations
 */

// Create a table
func (ac *AdminClient) CreateTable(ctx context.Context, db string, tableMeta *tspb.TableMeta, indexMetas []*tspb.IndexMeta) error {
	tableMeta.Database = db
	in := &tspb.CreateTableRequest{
		Database:  db,
		TableMeta: tableMeta,
		Indexes:   indexMetas,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.CreateTable(ctx, in)
		return err
	})
}

// Drop a table
func (ac *AdminClient) DropTable(ctx context.Context, db, table string) error {
	in := &tspb.DropTableRequest{
		Database: db,
		Table:    table,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.DropTable(ctx, in)
		return err
	})
}

// Truncate a table
func (ac *AdminClient) TruncateTable(ctx context.Context, db, table string) error {
	in := &tspb.TruncateTableRequest{
		Database: db,
		Table:    table,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.TruncateTable(ctx, in)
		return err
	})
}

// Modify a table's metadata
func (ac *AdminClient) AlterTable(ctx context.Context, db string, newTableMeta *tspb.TableMeta) error {
	in := &tspb.AlterTableRequest{
		Database:  db,
		TableMeta: newTableMeta,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.AlterTable(ctx, in)
		return err
	})
}

/*
 * table fields operations
 */

// Adds a column to the specified table.
func (ac *AdminClient) AddColumn(ctx context.Context, table string, columnFamilies []*tspb.ColumnFamilyMeta, columns []*tspb.ColumnMeta) error {
	in := &tspb.AddColumnRequest{
		Table:          table,
		ColumnFamilies: columnFamilies,
		Columns:        columns,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.AddColumn(ctx, in)
		return err
	})
}

// Deletes a column from the specified table. Table must be disabled.
func (ac *AdminClient) DeleteColumn(ctx context.Context, db, table string, columnFamilies, columns []string) error {
	in := &tspb.DeleteColumnRequest{
		Database:       db,
		Table:          table,
		ColumnFamilies: columnFamilies,
		Columns:        columns,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.DeleteColumn(ctx, in)
		return err
	})
}

// Modifies an existing column on the specified table.
func (ac *AdminClient) UpdateColumn(ctx context.Context, db, table string, columnFamilies *tspb.ColumnFamilyMeta, columns *tspb.ColumnMeta) error {
	in := &tspb.UpdateColumnRequest{
		Database:       db,
		Table:          table,
		ColumnFamilies: columnFamilies,
		Columns:        columns,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.UpdateColumn(ctx, in)
		return err
	})
}

/*
 * table index operations
 */
// Create a index on table
func (ac *AdminClient) CreateIndex(ctx context.Context, db, table string, indexMeta *tspb.IndexMeta) error {
	in := &tspb.CreateIndexRequest{
		Database: db,
		Table:    table,
		Indexes:  indexMeta,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.CreateIndex(ctx, in)
		return err
	})
}

// List table indexes
func (ac *AdminClient) ListIndex(ctx context.Context, db, table string) ([]*tspb.IndexMeta, error) {
	var (
		resp *tspb.ListIndexResponse
		err  error
	)
	in := &tspb.ListIndexRequest{
		Database: db,
		Table:    table,
	}
	retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		resp, err = ac.pbCli.ListIndex(ctx, in)
		return err
	}, idempotentRetryOption)

	if err != nil {
		return nil, err
	}
	return resp.Indexes, err
}

// Get table index
func (ac *AdminClient) GetIndex(ctx context.Context, db, table, index string) (*tspb.IndexMeta, error) {
	var (
		resp *tspb.GetIndexResponse
		err  error
	)
	in := &tspb.GetIndexRequest{
		Database: db,
		Table:    table,
		Index:    index,
	}
	retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		resp, err = ac.pbCli.GetIndex(ctx, in)
		return err
	}, idempotentRetryOption)

	if err != nil {
		return nil, err
	}
	return resp.Index, err
}

// Drop table index
func (ac *AdminClient) DropIndex(ctx context.Context, db, table, index string) error {
	in := &tspb.DropIndexRequest{
		Database: db,
		Table:    table,
		Index:    index,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.DropIndex(ctx, in)
		return err
	})
}
