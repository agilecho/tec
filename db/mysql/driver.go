package mysql

import (
	"context"
	"database/sql/driver"
	"net"
	"sync"
)

var (
	dialsLock sync.RWMutex
	dials     map[string]DialContextFunc
)

type DialFunc func(addr string) (net.Conn, error)
type DialContextFunc func(ctx context.Context, addr string) (net.Conn, error)

type MySQLDriver struct {}

func (d MySQLDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)

	if err != nil {
		return nil, err
	}

	c := &connector{
		cfg: cfg,
	}

	return c.Connect(context.Background())
}