package gohive

import (
	"context"
	"database/sql/driver"
	"fmt"

	"sqlflow.org/gohive/hiveserver2"
)

// Options for opened Hive sessions.
type Options struct {
	PollIntervalSeconds int64
	BatchSize           int64
}

type Connection struct {
	thrift  *hiveserver2.TCLIServiceClient
	session *hiveserver2.TSessionHandle
	options Options
}

func (c *Connection) Begin() (driver.Tx, error) {
	return nil, nil
}

func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (c *Connection) isOpen() bool {
	return c.session != nil
}

func (c *Connection) Close() error {
	if c.isOpen() {
		closeReq := hiveserver2.NewTCloseSessionReq()
		closeReq.SessionHandle = c.session
		resp, err := c.thrift.CloseSession(closeReq)
		if err != nil {
			return fmt.Errorf("Error closing session %s %s", resp, err)
		}

		c.session = nil
	}
	return nil
}

func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	executeReq := hiveserver2.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.session
	executeReq.Statement = query

	resp, err := c.thrift.ExecuteStatement(executeReq)
	if err != nil {
		return nil, fmt.Errorf("Error in ExecuteStatement: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("Error from server: %s", resp.Status.String())
	}

	return newRows(c.thrift, resp.OperationHandle, c.options), nil
}

func isSuccessStatus(p *hiveserver2.TStatus) bool {
	status := p.GetStatusCode()
	return status == hiveserver2.TStatusCode_SUCCESS_STATUS ||
		status == hiveserver2.TStatusCode_SUCCESS_WITH_INFO_STATUS
}
