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

// A Statement is a SQL query with named parameters.
//
// A parameter placeholder consists of '@' followed by the parameter name.
// Parameter names consist of any combination of letters, numbers, and
// underscores. Names may be entirely numeric (e.g., "WHERE m.id = @5").
// Parameters may appear anywhere that a literal value is expected. The same
// parameter name may be used more than once.  It is an error to execute a
// statement with unbound parameters. On the other hand, it is allowable to
// bind parameter names that are not used.
//
// See the documentation of the Row type for how Go types are mapped to Cloud
// Spanner types.
type Statement struct {
	SQL    string
	Params map[string]interface{}
}

// NewStatement returns a Statement with the given SQL and an empty Params map.
func NewStatement(sql string) Statement {
	return Statement{SQL: sql, Params: map[string]interface{}{}}
}
