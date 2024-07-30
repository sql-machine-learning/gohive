package gohive

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	User       string
	Passwd     string
	Addr       string
	DBName     string
	Auth       string
	Batch      int
	SessionCfg map[string]string
	TLSCfg     *TLSConfig
}

type TLSConfig struct {
	InsecureSkipVerify bool
	RootCAs            []string
	RootCAFiles        []string
}

var (
	// Regexp syntax: https://github.com/google/re2/wiki/Syntax
	reDSN        = regexp.MustCompile(`(.+@)?([^@|^?]+)\\?(.*)`)
	reUserPasswd = regexp.MustCompile(`([^:@]+)(:[^:]+)?@`)
)

const (
	sessionConfPrefix     = "session."
	tlsConfPrefix         = "tls."
	tlsInsecureSkipVerify = "insecure_skip_verify"
	tlsRootCA             = "root_ca"
	tlsRootCAFile         = "root_ca_file"
	authConfName          = "auth"
	defaultAuth           = "NOSASL"
	batchSizeName         = "batch"
	defaultBatchSize      = 10000
)

// ParseDSN requires DSN names in the format [user[:password]@]addr/dbname.
func ParseDSN(dsn string) (*Config, error) {
	// Please read https://play.golang.org/p/_CSLvl1AxOX before code review.
	sub := reDSN.FindStringSubmatch(dsn)
	if len(sub) != 4 {
		return nil, fmt.Errorf("The DSN %s doesn't match [user[:password]@]addr[/dbname][?auth=AUTH_MECHANISM]", dsn)
	}
	addr := ""
	dbname := ""
	loc := strings.IndexRune(sub[2], '/')
	if loc > -1 {
		addr = sub[2][:loc]
		dbname = sub[2][loc+1:]
	} else {
		addr = sub[2]
	}
	user := ""
	passwd := ""
	up := reUserPasswd.FindStringSubmatch(sub[1])
	if len(up) == 3 {
		user = up[1]
		if len(up[2]) > 0 {
			passwd = up[2][1:]
		}
	}

	auth := defaultAuth
	batch := defaultBatchSize
	sc := make(map[string]string)
	var tls *TLSConfig
	var err error
	if len(sub[3]) > 0 && sub[3][0] == '?' {
		qry, _ := url.ParseQuery(sub[3][1:])

		if v, found := qry[authConfName]; found {
			auth = v[0]
		}
		if v, found := qry[batchSizeName]; found {
			bch, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, err
			}
			batch = bch
		}

		for k, v := range qry {
			if strings.HasPrefix(k, sessionConfPrefix) {
				sc[k[len(sessionConfPrefix):]] = v[0]
			}
			if strings.HasPrefix(k, tlsConfPrefix) {
				if tls == nil {
					tls = &TLSConfig{}
				}

				key := k[len(tlsConfPrefix):]
				switch key {
				case tlsInsecureSkipVerify:
					tls.InsecureSkipVerify, err = strconv.ParseBool(v[0])
					if err != nil {
						return nil, fmt.Errorf("parse insecure_skip_verify: %w", err)
					}
				case tlsRootCA:
					for _, val := range v {
						pem, err := base64.URLEncoding.DecodeString(val)
						if err != nil {
							return nil, fmt.Errorf("decode root ca: %w", err)
						}
						tls.RootCAs = append(tls.RootCAs, string(pem))
					}
				case tlsRootCAFile:
					for _, val := range v {
						tls.RootCAFiles = append(tls.RootCAFiles, val)
					}
				default:
					return nil, fmt.Errorf("unsupported tls option: [%s]", key)
				}
				sc[k[len(sessionConfPrefix):]] = v[0]
			}
		}
	}

	return &Config{
		User:       user,
		Passwd:     passwd,
		Addr:       addr,
		DBName:     dbname,
		Auth:       auth,
		Batch:      batch,
		SessionCfg: sc,
		TLSCfg:     tls,
	}, nil
}

// FormatDSN outputs a string in the format "user:password@address?auth=xxx"
func (cfg *Config) FormatDSN() string {
	dsn := fmt.Sprintf("%s:%s@%s", cfg.User, cfg.Passwd, cfg.Addr)
	if len(cfg.DBName) > 0 {
		dsn = fmt.Sprintf("%s/%s", dsn, cfg.DBName)
	}
	dsn += fmt.Sprintf("?batch=%d", cfg.Batch)
	if len(cfg.Auth) > 0 {
		dsn += fmt.Sprintf("&auth=%s", cfg.Auth)
	}
	if len(cfg.SessionCfg) > 0 {
		for k, v := range cfg.SessionCfg {
			dsn += fmt.Sprintf("&%s%s=%s", sessionConfPrefix, k, v)
		}
	}
	if cfg.TLSCfg != nil {
		if cfg.TLSCfg.InsecureSkipVerify {
			dsn += fmt.Sprintf(
				"&%s%s=%t",
				tlsConfPrefix,
				tlsInsecureSkipVerify,
				cfg.TLSCfg.InsecureSkipVerify)
		}
		for _, ca := range cfg.TLSCfg.RootCAs {
			dsn += fmt.Sprintf(
				"&%s%s=%s",
				tlsConfPrefix,
				tlsRootCA,
				base64.URLEncoding.EncodeToString([]byte(ca)))
		}
		for _, caFile := range cfg.TLSCfg.RootCAFiles {
			dsn += fmt.Sprintf(
				"&%s%s=%s",
				tlsConfPrefix,
				tlsRootCAFile,
				caFile)
		}
	}
	return dsn
}

func (c *TLSConfig) Load() (*tls.Config, error) {
	if c == nil {
		return nil, nil
	}

	cfg := tls.Config{
		InsecureSkipVerify: c.InsecureSkipVerify,
	}

	var err error
	cfg.RootCAs, err = x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system certs: %w", err)
	}
	for _, f := range c.RootCAFiles {
		pem, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f, err)
		}
		cfg.RootCAs.AppendCertsFromPEM(pem)
	}
	for _, pem := range c.RootCAs {
		cfg.RootCAs.AppendCertsFromPEM([]byte(pem))
	}

	return &cfg, nil
}
