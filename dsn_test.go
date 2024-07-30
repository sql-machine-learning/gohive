package gohive

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// This certificate was sourced from the examples in go's documentation
	// for [x509 Certificate.Verify].
	//
	// [x509 Certificate.Verify]: https://pkg.go.dev/crypto/x509#example-Certificate.Verify
	rootPEM = `-----BEGIN CERTIFICATE-----
MIIEBDCCAuygAwIBAgIDAjppMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNVBAYTAlVT
MRYwFAYDVQQKEw1HZW9UcnVzdCBJbmMuMRswGQYDVQQDExJHZW9UcnVzdCBHbG9i
YWwgQ0EwHhcNMTMwNDA1MTUxNTU1WhcNMTUwNDA0MTUxNTU1WjBJMQswCQYDVQQG
EwJVUzETMBEGA1UEChMKR29vZ2xlIEluYzElMCMGA1UEAxMcR29vZ2xlIEludGVy
bmV0IEF1dGhvcml0eSBHMjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJwqBHdc2FCROgajguDYUEi8iT/xGXAaiEZ+4I/F8YnOIe5a/mENtzJEiaB0C1NP
VaTOgmKV7utZX8bhBYASxF6UP7xbSDj0U/ck5vuR6RXEz/RTDfRK/J9U3n2+oGtv
h8DQUB8oMANA2ghzUWx//zo8pzcGjr1LEQTrfSTe5vn8MXH7lNVg8y5Kr0LSy+rE
ahqyzFPdFUuLH8gZYR/Nnag+YyuENWllhMgZxUYi+FOVvuOAShDGKuy6lyARxzmZ
EASg8GF6lSWMTlJ14rbtCMoU/M4iarNOz0YDl5cDfsCx3nuvRTPPuj5xt970JSXC
DTWJnZ37DhF5iR43xa+OcmkCAwEAAaOB+zCB+DAfBgNVHSMEGDAWgBTAephojYn7
qwVkDBF9qn1luMrMTjAdBgNVHQ4EFgQUSt0GFhu89mi1dvWBtrtiGrpagS8wEgYD
VR0TAQH/BAgwBgEB/wIBADAOBgNVHQ8BAf8EBAMCAQYwOgYDVR0fBDMwMTAvoC2g
K4YpaHR0cDovL2NybC5nZW90cnVzdC5jb20vY3Jscy9ndGdsb2JhbC5jcmwwPQYI
KwYBBQUHAQEEMTAvMC0GCCsGAQUFBzABhiFodHRwOi8vZ3RnbG9iYWwtb2NzcC5n
ZW90cnVzdC5jb20wFwYDVR0gBBAwDjAMBgorBgEEAdZ5AgUBMA0GCSqGSIb3DQEB
BQUAA4IBAQA21waAESetKhSbOHezI6B1WLuxfoNCunLaHtiONgaX4PCVOzf9G0JY
/iLIa704XtE7JW4S615ndkZAkNoUyHgN7ZVm2o6Gb4ChulYylYbc3GrKBIxbf/a/
zG+FA1jDaFETzf3I93k9mTXwVqO94FntT0QJo544evZG0R0SnU++0ED8Vf4GXjza
HFa9llF7b1cq26KqltyMdMKVvvBulRP/F/A8rLIQjcxz++iPAsbw+zOzlTvjwsto
WHPbqCRiOwY1nQ2pM714A5AuTHhdUDqB1O6gyHA43LL5Z/qHQF1hwFGPa4NrzQU6
yuGnBXj8ytqU0CwIPX4WecigUCAkVDNx
-----END CERTIFICATE-----`
)

func TestParseDSNWithSessionConf(t *testing.T) {
	sc := make(map[string]string)
	sc["mapreduce_job_quenename"] = "mr"
	cfg := &Config{
		User:       "usr",
		Passwd:     "ps@wd",
		Addr:       "hiveserver",
		DBName:     "mydb",
		Auth:       "PLAIN",
		Batch:      200,
		SessionCfg: sc,
	}
	dsn := cfg.FormatDSN()
	assert.Equal(t, dsn, "usr:ps@wd@hiveserver/mydb?batch=200&auth=PLAIN&session.mapreduce_job_quenename=mr")

	cfg2, e := ParseDSN(dsn)
	assert.Nil(t, e)
	assert.Equal(t, cfg.User, cfg2.User)
	assert.Equal(t, cfg.Passwd, cfg2.Passwd)
	assert.Equal(t, cfg.Addr, cfg2.Addr)
	assert.Equal(t, cfg.DBName, cfg2.DBName)
	assert.Equal(t, cfg.Auth, cfg2.Auth)
	assert.Equal(t, cfg.Batch, cfg2.Batch)
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
	assert.Equal(t, cfg.Batch, 10000)

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

func TestParseDSNWithTLSConfig(t *testing.T) {
	b64 := base64.URLEncoding.EncodeToString([]byte(rootPEM))
	t.Run("no tls", func(t *testing.T) {
		cfg, e := ParseDSN("127.0.0.1")
		require.NoError(t, e)
		require.Nil(t, cfg.TLSCfg)
	})

	t.Run("one ca", func(t *testing.T) {
		cfg, e := ParseDSN(fmt.Sprintf("127.0.0.1?tls.root_ca=%s", b64))
		require.NoError(t, e)
		require.NotNil(t, cfg.TLSCfg)
		require.Len(t, cfg.TLSCfg.RootCAs, 1)
		require.Equal(t, cfg.TLSCfg.RootCAs[0], rootPEM)
	})

	t.Run("two cas", func(t *testing.T) {
		cfg, e := ParseDSN(fmt.Sprintf("127.0.0.1?tls.root_ca=%s&tls.root_ca=%s", b64, b64))
		require.NoError(t, e)
		require.NotNil(t, cfg.TLSCfg)
		require.Len(t, cfg.TLSCfg.RootCAs, 2)
		require.Equal(t, cfg.TLSCfg.RootCAs[0], rootPEM)
		require.Equal(t, cfg.TLSCfg.RootCAs[1], rootPEM)
	})

	t.Run("one ca fiel", func(t *testing.T) {
		file := "cert.pem"
		cfg, e := ParseDSN(fmt.Sprintf("127.0.0.1?tls.root_ca_file=%s", file))
		require.NoError(t, e)
		require.NotNil(t, cfg.TLSCfg)
		require.Len(t, cfg.TLSCfg.RootCAFiles, 1)
		require.Equal(t, cfg.TLSCfg.RootCAFiles[0], file)
	})

	t.Run("two ca files", func(t *testing.T) {
		file := "cert.pem"
		file2 := "cert2.pem"
		cfg, e := ParseDSN(fmt.Sprintf("127.0.0.1?tls.root_ca_file=%s&tls.root_ca_file=%s", file, file2))
		require.NoError(t, e)
		require.NotNil(t, cfg.TLSCfg)
		require.Len(t, cfg.TLSCfg.RootCAFiles, 2)
		require.Equal(t, cfg.TLSCfg.RootCAFiles[0], file)
		require.Equal(t, cfg.TLSCfg.RootCAFiles[1], file2)
	})

	t.Run("insecure skip verify", func(t *testing.T) {
		cfg, e := ParseDSN("127.0.0.1?tls.insecure_skip_verify=true")
		require.NoError(t, e)
		require.NotNil(t, cfg.TLSCfg)
		require.True(t, cfg.TLSCfg.InsecureSkipVerify)
	})
}

func TestFormatDSNWithDBName(t *testing.T) {
	ds := "user:passwd@127.0.0.1/mnist?batch=100000&auth=NOSASL"
	cfg, e := ParseDSN(ds)
	assert.Nil(t, e)

	ds2 := cfg.FormatDSN()
	assert.Equal(t, ds2, ds)
}

func TestFormatDSNWithoutDBName(t *testing.T) {
	ds := "user:passwd@127.0.0.1?batch=100&auth=NOSASL"
	cfg, e := ParseDSN(ds)
	assert.Nil(t, e)

	ds2 := cfg.FormatDSN()
	assert.Equal(t, ds2, ds)
}

func TestFormatDSNWithTLSConfig(t *testing.T) {
	b64 := base64.URLEncoding.EncodeToString([]byte(rootPEM))
	t.Run("no tls", func(t *testing.T) {
		require.Equal(t,
			":@127.0.0.1?batch=0",
			(&Config{
				Addr: "127.0.0.1",
			}).FormatDSN())
	})

	t.Run("one ca", func(t *testing.T) {
		require.Equal(t,
			fmt.Sprintf(":@127.0.0.1?batch=0&tls.root_ca=%s", b64),
			(&Config{
				Addr: "127.0.0.1",
				TLSCfg: &TLSConfig{
					RootCAs: []string{rootPEM},
				},
			}).FormatDSN())
	})

	t.Run("two cas", func(t *testing.T) {
		require.Equal(t,
			fmt.Sprintf(":@127.0.0.1?batch=0&tls.root_ca=%s&tls.root_ca=%s", b64, b64),
			(&Config{
				Addr: "127.0.0.1",
				TLSCfg: &TLSConfig{
					RootCAs: []string{rootPEM, rootPEM},
				},
			}).FormatDSN())
	})

	t.Run("one ca file", func(t *testing.T) {
		file := "cert.pem"
		require.Equal(t,
			fmt.Sprintf(":@127.0.0.1?batch=0&tls.root_ca_file=%s", file),
			(&Config{
				Addr: "127.0.0.1",
				TLSCfg: &TLSConfig{
					RootCAFiles: []string{file},
				},
			}).FormatDSN())
	})

	t.Run("two ca files", func(t *testing.T) {
		file := "cert.pem"
		file2 := "cert2.pem"
		require.Equal(t,
			fmt.Sprintf(":@127.0.0.1?batch=0&tls.root_ca_file=%s&tls.root_ca_file=%s", file, file2),
			(&Config{
				Addr: "127.0.0.1",
				TLSCfg: &TLSConfig{
					RootCAFiles: []string{file, file2},
				},
			}).FormatDSN())
	})

	t.Run("insecure skip verify", func(t *testing.T) {
		require.Equal(t,
			":@127.0.0.1?batch=0&tls.insecure_skip_verify=true",
			(&Config{
				Addr: "127.0.0.1",
				TLSCfg: &TLSConfig{
					InsecureSkipVerify: true,
				},
			}).FormatDSN())
	})
}
