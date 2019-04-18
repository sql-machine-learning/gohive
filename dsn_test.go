package gohive

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDSN(t *testing.T) {
	cfg, e := ParseDSN("root:root@127.0.0.1")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "root")
	assert.Equal(t, cfg.Passwd, "root")
	assert.Equal(t, cfg.Addr, "127.0.0.1")

	// cfg, e = parseDSN("root@127.0.0.1")
	// assert.Nil(t, e)
	// assert.Equal(t, cfg.User, "root")
	// assert.Equal(t, cfg.Passwd, "")
	// assert.Equal(t, cfg.Addr, "127.0.0.1")

	// cfg, e = parseDSN("127.0.0.1")
	// assert.Nil(t, e)
	// assert.Equal(t, cfg.User, "")
	// assert.Equal(t, cfg.Passwd, "")
	// assert.Equal(t, cfg.Addr, "127.0.0.1")
}

func TestFormatDSN(t *testing.T) {
	ds := "user:passwd@127.0.0.1"
	cfg, e := ParseDSN(ds)
	assert.Nil(t, e)

	ds2 := cfg.FormatDSN()
	assert.Equal(t, ds2, ds)
}
