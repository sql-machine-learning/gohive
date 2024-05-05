package gohive

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	User                    string
	Passwd                  string
	Addr                    string
	DBName                  string
	Auth                    string
	Batch                   int
	ColumnsWithoutTableName bool // column names not contains table name
	SessionCfg              map[string]string
}

var (
	// Regexp syntax: https://github.com/google/re2/wiki/Syntax
	reDSN        = regexp.MustCompile(`(.+@)?([^@|^?]+)\\?(.*)`)
	reUserPasswd = regexp.MustCompile(`([^:@]+)(:[^:@]+)?@`)
)

const (
	sessionConfPrefix           = "session."
	authConfName                = "auth"
	defaultAuth                 = "NOSASL"
	batchSizeName               = "batch"
	columnsWithoutTableNameName = "columns_without_table_name"
	defaultBatchSize            = 10000
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
	columnsWithoutTableName := false
	var err error
	sc := make(map[string]string)
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
		if v, found := qry[columnsWithoutTableNameName]; found {
			columnsWithoutTableName, err = strconv.ParseBool(v[0])
			if err != nil {
				return nil, err
			}
		}

		for k, v := range qry {
			if strings.HasPrefix(k, sessionConfPrefix) {
				sc[k[len(sessionConfPrefix):]] = v[0]
			}
		}
	}

	return &Config{
		User:                    user,
		Passwd:                  passwd,
		Addr:                    addr,
		DBName:                  dbname,
		Auth:                    auth,
		Batch:                   batch,
		ColumnsWithoutTableName: columnsWithoutTableName,
		SessionCfg:              sc,
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
	if cfg.ColumnsWithoutTableName {
		dsn += "&columns_without_table_name=true"
	}
	if len(cfg.SessionCfg) > 0 {
		for k, v := range cfg.SessionCfg {
			dsn += fmt.Sprintf("&%s%s=%s", sessionConfPrefix, k, v)
		}
	}
	return dsn
}
