package gohive

import (
	"fmt"
	"regexp"
	"strings"
)

type Config struct {
	User   string
	Passwd string
	Addr   string
	DBName string
}

var (
	// Regexp syntax: https://github.com/google/re2/wiki/Syntax
	reDSN        = regexp.MustCompile(`(.+@)?([^@]+)`)
	reUserPasswd = regexp.MustCompile(`([^:@]+)(:[^:@]+)?@`)
)

// ParseDSN requires DSN names in the format [user[:password]@]addr/dbname.
func ParseDSN(dsn string) (*Config, error) {
	// Please read https://play.golang.org/p/_CSLvl1AxOX before code review.
	sub := reDSN.FindStringSubmatch(dsn)
	if len(sub) != 3 {
		return nil, fmt.Errorf("The DSN %s doesn't match [user[:password]@]addr[/dbname]", dsn)
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
	up := reUserPasswd.FindStringSubmatch(sub[1])
	if len(up) == 3 {
		if len(up[2]) > 0 {
			return &Config{User: up[1], Passwd: up[2][1:], Addr: addr, DBName: dbname}, nil
		}
		return &Config{User: up[1], Addr: addr, DBName: dbname}, nil
	}
	return &Config{Addr: addr, DBName: dbname}, nil
}

// FormatDSN outputs a string in the format "user:password@address"
func (cfg *Config) FormatDSN() string {
	if len(cfg.DBName) > 0 {
		return fmt.Sprintf("%s:%s@%s/%s", cfg.User, cfg.Passwd, cfg.Addr, cfg.DBName)
	} else {
		return fmt.Sprintf("%s:%s@%s", cfg.User, cfg.Passwd, cfg.Addr)
	}
}
