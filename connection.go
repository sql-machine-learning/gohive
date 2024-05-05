package gohive

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	hiveserver2 "sqlflow.org/gohive/hiveserver2/gen-go/tcliservice"
)

// hiveOptions for opened Hive sessions.
type hiveOptions struct {
	PollIntervalSeconds     int64
	BatchSize               int64
	ColumnsWithoutTableName bool // column names not contains table name
}

type hiveConnection struct {
	thrift             *hiveserver2.TCLIServiceClient
	session            *hiveserver2.TSessionHandle
	options            hiveOptions
	ctx                context.Context
	paramsInterpolator *ParamsInterpolator
}

func (c *hiveConnection) Begin() (driver.Tx, error) {
	return nil, nil
}

func (c *hiveConnection) Prepare(qry string) (driver.Stmt, error) {
	if !c.isOpen() {
		return nil, fmt.Errorf("driver: bad connection")
	}
	return &hiveStmt{hc: c, query: qry}, nil
}

func (c *hiveConnection) isOpen() bool {
	return c.session != nil
}

// As hiveserver2 thrift api does not provide Ping method,
// we use GetInfo instead to check the health of hiveserver2.
func (c *hiveConnection) Ping(ctx context.Context) (err error) {
	getInfoReq := hiveserver2.NewTGetInfoReq()
	getInfoReq.SessionHandle = c.session
	getInfoReq.InfoType = hiveserver2.TGetInfoType_CLI_SERVER_NAME

	resp, err := c.thrift.GetInfo(ctx, getInfoReq)

	if err != nil {
		return fmt.Errorf("Error in GetInfo: %v", err)
	}

	if !isSuccessStatus(resp.Status) {
		return fmt.Errorf("Error from server: %s", resp.Status.String())
	}

	return nil
}

func (c *hiveConnection) Close() error {
	if c.isOpen() {
		closeReq := hiveserver2.NewTCloseSessionReq()
		closeReq.SessionHandle = c.session
		resp, err := c.thrift.CloseSession(c.ctx, closeReq)
		if err != nil {
			return fmt.Errorf("Error closing session %s %s", resp, err)
		}

		c.session = nil
	}
	return nil
}

func removeLastSemicolon(s string) string {
	s = strings.TrimSpace(s)
	n := len(s)
	if n > 0 && s[n-1] == ';' {
		return s[0 : n-1]
	}
	return s
}

func (c *hiveConnection) execute(ctx context.Context, query string, args []driver.NamedValue) (*hiveserver2.TExecuteStatementResp, error) {
	var err error
	if len(args) != 0 {
		query, err = c.paramsInterpolator.InterpolateNamedValue(query, args)
		if err != nil {
			return nil, err
		}
	}
	executeReq := hiveserver2.NewTExecuteStatementReq()
	executeReq.SessionHandle = c.session
	executeReq.Statement = removeLastSemicolon(query)

	resp, err := c.thrift.ExecuteStatement(c.ctx, executeReq)
	if err != nil {
		return nil, fmt.Errorf("Error in ExecuteStatement: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("Error from server: %s", resp.Status.String())
	}
	return resp, nil
}

func (c *hiveConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	resp, err := c.execute(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return newRows(c.thrift, resp.OperationHandle, c.options, ctx), nil
}

func (c *hiveConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	resp, err := c.execute(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return newHiveResult(resp.OperationHandle), nil
}

func isSuccessStatus(p *hiveserver2.TStatus) bool {
	status := p.GetStatusCode()
	return status == hiveserver2.TStatusCode_SUCCESS_STATUS ||
		status == hiveserver2.TStatusCode_SUCCESS_WITH_INFO_STATUS
}
