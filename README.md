# Zetta TableStore Go Client

## Installation

```bash
$ go get github.com/zhihu/zetta-client-go
```

## Example Usage

### Create Table

[snip]:# (zetta-1)
```go
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
    TableName:       "users",
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
```

### Simple Writes 
[snip]:# (zetta-2)
```go 
TABLE_NAME := "users"
cols := []string{"name", "age"}
vals := []interface{}{"user-01", "18"}
rawMS := zetta.InsertOrUpdate(TABLE_NAME, cols, vals)
err := client.Mutate(context.Background(), rawMS)
if err != nil {
    panic(err)
}
```

### Simple Reads
[snip]:# (zetta-3)
```go
keys := zetta.KeySet{
    Keys: []zetta.Key{[]interface{}{"user-01"}},
}
resp, err := client.Read(context.Background(), "users", keys, "", []string{"name", "age"}, 10)
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
```


