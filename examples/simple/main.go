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

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"

	zetta "github.com/zhihu/zetta-client-go"

	"github.com/k0kubun/pp"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

var (
	adminClient *zetta.AdminClient
	dataClient  *zetta.DataClient

	zettaServerAddr = "127.0.0.1:4000"
	DB_NAME         = "zhihu"
	TABLE_NAME      = "users"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	// DB_NAME = fmt.Sprintf("%s-%d", DB_NAME, time.Now().Unix())

	var err error
	adminClient, err = zetta.NewAdminClient(zettaServerAddr)
	if err != nil {
		panic(err)
	}
	dataClient, err = zetta.NewDataClient(context.Background(), zettaServerAddr, DB_NAME, zetta.DataClientConfig{})

	if err != nil {
		panic(err)
	}
}

func main() {
	createDatabase()
	fmt.Println("--------1")
	createTable()
	fmt.Println("--------2")
	writeTable(dataClient)
	fmt.Println("--------3")
	readTable(dataClient)
	fmt.Println("--------4")

}

func writeTable(client *zetta.DataClient) {
	cols := []string{"name", "age"}
	vals := []interface{}{"user-01", "18"}
	rawMS := zetta.InsertOrUpdate(TABLE_NAME, cols, vals)
	err := client.Mutate(context.Background(), rawMS)
	if err != nil {
		panic(err)
	}
	fmt.Println("write done")
}

func readTable2() {
	// client.Single().Read(context.Background(), TABLE_NAME, []interface{"name"}, []string{"name", agent}
}

func readTable(client *zetta.DataClient) {

	in := &tspb.ReadRequest{
		Session:     "",
		Transaction: nil,
		Table:       TABLE_NAME,
		Index:       "",
		Columns:     []string{"name", "age"},
		Limit:       10,
		KeySet: &tspb.KeySet{
			Keys: []*tspb.ListValue{
				&tspb.ListValue{
					Values: []*tspb.Value{
						&tspb.Value{
							Kind: &tspb.Value_StringValue{
								StringValue: "user-01",
							},
						},
					},
				},
			},
		},
		ResumeToken:    nil,
		PartitionToken: nil,
	}

	keys := zetta.KeySet{
		Keys: []zetta.Key{[]interface{}{"user-01"}},
	}
	resp, err := client.Read(context.Background(), in.Table, keys, "", in.Columns, 10)
	if err != nil {
		panic(err)
	}
	for _, row := range resp.SliceRows {
		if len(row.GetCells()) != 2 {
			panic("invalid values")
		}
		name := row.Cells[0].GetValue()
		age := row.Cells[1].GetValue()

		fmt.Println("read name:", name.GetStringValue())
		fmt.Println("read age:", age.GetStringValue())
	}
}

func decodeStr(encodedStr string) string {
	if len(encodedStr) == 0 {
		return ""
	}
	v, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		panic(err)
	}
	return string(v)
}

func createDatabase() {
	ctx := context.Background()
	_, err := adminClient.CreateDatabase(ctx, DB_NAME, nil)
	if err != nil {
		panic(err)
	}
	dbMetas, _, err := adminClient.ListDatabase(ctx, "", "", 0)
	if err != nil {
		panic(err)
	}
	for _, db := range dbMetas {
		log.Printf("db: %+v\n", db.String())
	}
	log.Println("created database", DB_NAME)
}

func createTable() {
	ctx := context.Background()
	cfs := []*tspb.ColumnFamilyMeta{
		{
			Id:         0,
			Name:       "default",
			Attributes: nil,
		},
	}
	cs := []*tspb.ColumnMeta{
		{
			Id:         0,
			Name:       "name",
			ColumnType: &tspb.Type{Code: tspb.TypeCode_STRING},
			IsPrimary:  true,
			NotNull:    false,
			Family:     "default",
		},
		{
			Id:         1,
			Name:       "age",
			ColumnType: &tspb.Type{Code: tspb.TypeCode_STRING},
			IsPrimary:  false,
			NotNull:    false,
			Family:     "default",
		},
	}
	tableMeta := &tspb.TableMeta{
		TableName:       TABLE_NAME,
		ColumnFamilies:  cfs,
		Columns:         cs,
		PrimaryKey:      []string{"name"}, // there must be a PK
		Attributes:      nil,
		Interleave:      nil,
		ExtraStatements: nil,
	}
	if err := adminClient.CreateTable(ctx, DB_NAME, tableMeta, nil); err != nil {
		panic(err)
	}
	fmt.Println("created table", TABLE_NAME)
}

func writeMutations() {
	cols := []string{"id", "age"}
	vs := []interface{}{1, 20}
	mutations := []*zetta.Mutation{
		zetta.Insert(DB_NAME, cols, vs),
	}
	ctx := context.Background()
	t, err := dataClient.Apply(ctx, mutations)
	if err != nil {
		panic(err)
	}
	pp.Println("writeMutations: ", t)
}
