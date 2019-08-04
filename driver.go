package gohive

import (
	"database/sql"
	"database/sql/driver"
	"errors"

	"sqlflow.org/gohive/hiveserver2"
	"sqlflow.org/gohive/thrift"
)

type drv struct{}

func (d drv) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	transport, err := thrift.NewTSocket(cfg.Addr)
	if err != nil {
		return nil, err
	}

	if err := transport.Open(); err != nil {
		return nil, err
	}

	if transport == nil {
		return nil, errors.New("nil thrift transport")
	}

	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	client := hiveserver2.NewTCLIServiceClientFactory(transport, protocol)
	s := hiveserver2.NewTOpenSessionReq()
	s.ClientProtocol = 6
	if cfg.User != "" {
		s.Username = &cfg.User
		if cfg.Passwd != "" {
			s.Password = &cfg.Passwd
		}
	}
	if cfg.DBName != "" {
		config := make(map[string]string)
		config["use:database"] = cfg.DBName
		s.Configuration = config
	}
	session, err := client.OpenSession(s)
	if err != nil {
		return nil, err
	}

	options := hiveOptions{PollIntervalSeconds: 5, BatchSize: 100000}
	conn := &hiveConnection{client, session.SessionHandle, options}
	return conn, nil
}

func init() {
	sql.Register("hive", &drv{})
}
