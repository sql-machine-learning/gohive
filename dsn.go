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
	Auth   string
}

var (
	// Regexp syntax: https://github.com/google/re2/wiki/Syntax
	reDSN        = regexp.MustCompile(`(.+@)?([^@|^?]+)\\?(.*)`)
	reUserPasswd = regexp.MustCompile(`([^:@]+)(:[^:@]+)?@`)
	reArguments  = regexp.MustCompile(`(\w+)=(\w+)`)
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
	auth := "NOSASL"
	up := reUserPasswd.FindStringSubmatch(sub[1])
	if len(up) == 3 {
		user = up[1]
		if len(up[2]) > 0 {
			passwd = up[2][1:]
		}
	}

	args := reArguments.FindAllStringSubmatch(sub[3], -1)
	if len(args) > 1 {
		return nil, fmt.Errorf("The DSN %s doesn't match [user[:password]@]addr[/dbname][?auth=AUTH_MECHANISM]", dsn)
	}

	if len(args) == 1 {
		if args[0][1] != "auth" {
			return nil, fmt.Errorf("The DSN %s doesn't match [user[:password]@]addr[/dbname][?auth=AUTH_MECHANISM]", dsn)
		}
		auth = args[0][2]
	}
	return &Config{
		User:   user,
		Passwd: passwd,
		Addr:   addr,
		DBName: dbname,
		Auth:   auth,
	}, nil
}

// FormatDSN outputs a string in the format "user:password@address?auth=xxx"
func (cfg *Config) FormatDSN() string {
	dsn := fmt.Sprintf("%s:%s@%s", cfg.User, cfg.Passwd, cfg.Addr)
	if len(cfg.DBName) > 0 {
		dsn = fmt.Sprintf("%s/%s", dsn, cfg.DBName)
	}
	if len(cfg.Auth) > 0 {
		dsn = fmt.Sprintf("%s?auth=%s", dsn, cfg.Auth)
	}
	return dsn
}
