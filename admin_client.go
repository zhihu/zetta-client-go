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
	"time"

	"github.com/zhihu/zetta-client-go/utils/retry"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	idempotentRetryOption = retry.CallOption(retry.WithRetry(func() retry.Retryer {
		validCodes := []codes.Code{codes.Unavailable, codes.DeadlineExceeded}
		backOff := retry.Backoff{
			Init:       20 * time.Millisecond,
			Max:        32 * time.Second,
			Multiplier: 1.3,
		}
		return retry.GetBackoffRetryer(validCodes, backOff)
	}))
)

type AdminClient struct {
	conn  *grpc.ClientConn
	pbCli tspb.TablestoreAdminClient
}

func NewAdminClient(addr string, opts ...grpc.DialOption) (*AdminClient, error) {
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &AdminClient{
		conn:  conn,
		pbCli: tspb.NewTablestoreAdminClient(conn),
	}, nil
}

/*
 * database operations
 */
func (ac *AdminClient) CreateDatabase(ctx context.Context, dbName string, attributes map[string]string) (*tspb.DatabaseMeta, error) {
	var (
		resp *tspb.CreateDatabaseResponse
		err  error
	)
	in := &tspb.CreateDatabaseRequest{
		Database:   dbName,
		Attributes: attributes,
	}
	retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		resp, err = ac.pbCli.CreateDatabase(ctx, in)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.DatabaseMeta, nil
}

// Modify a Database's metadata
func (ac *AdminClient) UpdateDatabase(ctx context.Context, dbName string, newAttributes map[string]string) (*tspb.DatabaseMeta, error) {
	var (
		resp *tspb.UpdateDatabaseResponse
		err  error
	)
	in := &tspb.UpdateDatabaseRequest{
		Database:   dbName,
		Attributes: newAttributes,
	}
	retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		resp, err = ac.pbCli.UpdateDatabase(ctx, in)
		return err
	})
	if err != nil {
		return nil, err
	}

	return resp.DatabaseMeta, err
}

// Deletes Database
func (ac *AdminClient) DeleteDatabase(ctx context.Context, dbID int64, dbName string) error {
	in := &tspb.DeleteDatabaseRequest{
		Id:       dbID,
		Database: dbName,
	}
	return retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		_, err := ac.pbCli.DeleteDatabase(ctx, in)
		return err
	})
}

// Get a Database descriptor by name
func (ac *AdminClient) GetDatabase(ctx context.Context, dbID int64, dbName string) (*tspb.DatabaseMeta, error) {
	var (
		resp *tspb.GetDatabaseResponse
		err  error
	)
	in := &tspb.GetDatabaseRequest{
		Id:       dbID,
		Database: dbName,
	}
	retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		resp, err = ac.pbCli.GetDatabase(ctx, in)
		return err
	}, idempotentRetryOption)
	if err != nil {
		return nil, err
	}

	return resp.DatabaseMeta, nil
}

// returns a list of Databases
func (ac *AdminClient) ListDatabase(ctx context.Context, parent, pageToken string, pageSize int32) ([]*tspb.DatabaseMeta, string, error) {
	var (
		resp *tspb.ListDatabaseResponse
		err  error
	)
	in := &tspb.ListDatabaseRequest{
		Parent:    parent,
		PageSize:  pageSize,
		PageToken: pageToken,
	}
	retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		resp, err = ac.pbCli.ListDatabase(ctx, in)
		return err
	}, idempotentRetryOption)
	if err != nil {
		return nil, "", err
	}

	return resp.Databases, resp.NextPageToken, err
}

// returns a list of Databases
func (ac *AdminClient) ListTables(ctx context.Context, database, pageToken string, pageSize int32) ([]*tspb.TableMeta, string, error) {
	var (
		resp *tspb.ListTableByDatabaseResponse
		err  error
	)
	in := &tspb.ListTableByDatabaseRequest{
		Database:  database,
		PageSize:  pageSize,
		PageToken: pageToken,
	}
	retry.Invoke(ctx, func(ctx context.Context, settings retry.CallSettings) error {
		resp, err = ac.pbCli.ListTables(ctx, in)
		// errlog.Debug(err)

		return err
	}, idempotentRetryOption)
	if err != nil {
		return nil, "", err
	}

	return resp.Tables, resp.NextPageToken, err
}
