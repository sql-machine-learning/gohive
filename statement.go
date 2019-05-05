package gohive

import "database/sql/driver"

type hiveStmt struct {
	hc *hiveConnection
}

// TODO: We implements these methods in another pr

func (stmt *hiveStmt) Close() error {
	return nil
}

func (stmt *hiveStmt) NumInput() int {
	return 0
}

func (stmt *hiveStmt) ColumnConverter(idx int) driver.ValueConverter {
	return nil
}

func (stmt *hiveStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (stmt *hiveStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}
