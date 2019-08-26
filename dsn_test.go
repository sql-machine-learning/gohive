package gohive

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDSNWithSessionConf(t *testing.T) {
	sc := make(map[string]string)
	sc["mapreduce_job_quenename"] = "mr"
	cfg := &Config{
		User:       "usr",
		Passwd:     "pswd",
		Addr:       "hiveserver",
		DBName:     "mydb",
		Auth:       "PLAIN",
		SessionCfg: sc,
	}
	dsn := cfg.FormatDSN()
	assert.Equal(t, dsn, "usr:pswd@hiveserver/mydb?auth=PLAIN&session.mapreduce_job_quenename=mr")

	cfg2, e := ParseDSN(dsn)
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, cfg2.User)
	assert.Equal(t, cfg.Passwd, cfg2.Passwd)
	assert.Equal(t, cfg.Addr, cfg2.Addr)
	assert.Equal(t, cfg.DBName, cfg2.DBName)
	assert.Equal(t, cfg.Auth, cfg2.Auth)
	sc, sc2 := cfg.SessionCfg, cfg2.SessionCfg
	assert.Equal(t, len(sc), len(sc2))
	for k, v := range sc {
		v2, found := sc2[k]
		assert.True(t, found)
		assert.Equal(t, v, v2)
	}
}

func TestParseDSNWithAuth(t *testing.T) {
	cfg, e := ParseDSN("root:root@127.0.0.1/mnist?auth=PLAIN")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "root")
	assert.Equal(t, cfg.Passwd, "root")
	assert.Equal(t, cfg.Addr, "127.0.0.1")
	assert.Equal(t, cfg.DBName, "mnist")
	assert.Equal(t, cfg.Auth, "PLAIN")

	cfg, e = ParseDSN("root@127.0.0.1/mnist")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "root")
	assert.Equal(t, cfg.Passwd, "")
	assert.Equal(t, cfg.Addr, "127.0.0.1")
	assert.Equal(t, cfg.DBName, "mnist")
	assert.Equal(t, cfg.Auth, "NOSASL")
}

func TestParseDSNWithDBName(t *testing.T) {
	cfg, e := ParseDSN("root:root@127.0.0.1/mnist")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "root")
	assert.Equal(t, cfg.Passwd, "root")
	assert.Equal(t, cfg.Addr, "127.0.0.1")
	assert.Equal(t, cfg.DBName, "mnist")

	cfg, e = ParseDSN("root@127.0.0.1/mnist")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "root")
	assert.Equal(t, cfg.Passwd, "")
	assert.Equal(t, cfg.Addr, "127.0.0.1")
	assert.Equal(t, cfg.DBName, "mnist")

	cfg, e = ParseDSN("127.0.0.1/mnist")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "")
	assert.Equal(t, cfg.Passwd, "")
	assert.Equal(t, cfg.Addr, "127.0.0.1")
	assert.Equal(t, cfg.DBName, "mnist")
}

func TestParseDSNWithoutDBName(t *testing.T) {
	cfg, e := ParseDSN("root:root@127.0.0.1")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "root")
	assert.Equal(t, cfg.Passwd, "root")
	assert.Equal(t, cfg.Addr, "127.0.0.1")

	cfg, e = ParseDSN("root@127.0.0.1")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "root")
	assert.Equal(t, cfg.Passwd, "")
	assert.Equal(t, cfg.Addr, "127.0.0.1")

	cfg, e = ParseDSN("127.0.0.1")
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, "")
	assert.Equal(t, cfg.Passwd, "")
	assert.Equal(t, cfg.Addr, "127.0.0.1")
}

func TestFormatDSNWithDBName(t *testing.T) {
	ds := "user:passwd@127.0.0.1/mnist?auth=NOSASL"
	cfg, e := ParseDSN(ds)
	assert.Nil(t, e)

	ds2 := cfg.FormatDSN()
	assert.Equal(t, ds2, ds)
}

func TestFormatDSNWithoutDBName(t *testing.T) {
	ds := "user:passwd@127.0.0.1?auth=NOSASL"
	cfg, e := ParseDSN(ds)
	assert.Nil(t, e)

	ds2 := cfg.FormatDSN()
	assert.Equal(t, ds2, ds)
}
