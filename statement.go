package gohive

import (
	"database/sql/driver"
)

type hiveStmt struct {
	hc    *hiveConnection
	query string
}

func (stmt *hiveStmt) Close() error {
	panic("not implemented")
}

func (stmt *hiveStmt) NumInput() int {
	panic("not implemented")
}

// Exec accepts stmt like: "INSERT INTO `TABLE` (f1, f2) VALUES(1.3, false)"
func (stmt *hiveStmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("not implemented")
}

func (stmt *hiveStmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("not implemented")
}
