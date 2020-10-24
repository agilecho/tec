package mysql

import (
	"bytes"
	"errors"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidDSNUnescaped       = errors.New("invalid DSN: did you forget to escape a param value?")
	errInvalidDSNAddr            = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	errInvalidDSNNoSlash         = errors.New("invalid DSN: missing the slash separating the database name")
	errInvalidDSNUnsafeCollation = errors.New("invalid DSN: interpolateParams can not be used with unsafe collations")
)

type Config struct {
	User string
	Passwd string
	Net string
	Addr string
	DBName string
	Params map[string]string
	Collation string
	Loc *time.Location
	MaxAllowedPacket int
	Timeout time.Duration
	ReadTimeout time.Duration
	WriteTimeout time.Duration

	ClientFoundRows bool
	ColumnsWithAlias bool
	InterpolateParams bool
	MultiStatements bool
	ParseTime bool
	RejectReadOnly bool
}

func NewConfig() *Config {
	return &Config{
		Collation:            defaultCollation,
		Loc:                  time.UTC,
		MaxAllowedPacket:     defaultMaxAllowedPacket,
	}
}

func (cfg *Config) Clone() *Config {
	cp := *cfg

	if len(cp.Params) > 0 {
		cp.Params = make(map[string]string, len(cfg.Params))
		for k, v := range cfg.Params {
			cp.Params[k] = v
		}
	}

	return &cp
}

func (cfg *Config) normalize() error {
	if cfg.InterpolateParams && unsafeCollations[cfg.Collation] {
		return errInvalidDSNUnsafeCollation
	}

	if cfg.Net == "" {
		cfg.Net = "tcp"
	}

	if cfg.Addr == "" {
		switch cfg.Net {
		case "tcp":
			cfg.Addr = "127.0.0.1:3306"
		case "unix":
			cfg.Addr = "/tmp/mysql.sock"
		default:
			return errors.New("default addr for network '" + cfg.Net + "' unknown")
		}

	} else if cfg.Net == "tcp" {
		cfg.Addr = ensureHavePort(cfg.Addr)
	}

	return nil
}

func (cfg *Config) FormatDSN() string {
	var buf bytes.Buffer

	if len(cfg.User) > 0 {
		buf.WriteString(cfg.User)
		if len(cfg.Passwd) > 0 {
			buf.WriteByte(':')
			buf.WriteString(cfg.Passwd)
		}
		buf.WriteByte('@')
	}

	if len(cfg.Net) > 0 {
		buf.WriteString(cfg.Net)
		if len(cfg.Addr) > 0 {
			buf.WriteByte('(')
			buf.WriteString(cfg.Addr)
			buf.WriteByte(')')
		}
	}

	buf.WriteByte('/')
	buf.WriteString(cfg.DBName)

	hasParam := false

	if cfg.ClientFoundRows {
		if hasParam {
			buf.WriteString("&clientFoundRows=true")
		} else {
			hasParam = true
			buf.WriteString("?clientFoundRows=true")
		}
	}

	if col := cfg.Collation; col != defaultCollation && len(col) > 0 {
		if hasParam {
			buf.WriteString("&collation=")
		} else {
			hasParam = true
			buf.WriteString("?collation=")
		}
		buf.WriteString(col)
	}

	if cfg.ColumnsWithAlias {
		if hasParam {
			buf.WriteString("&columnsWithAlias=true")
		} else {
			hasParam = true
			buf.WriteString("?columnsWithAlias=true")
		}
	}

	if cfg.InterpolateParams {
		if hasParam {
			buf.WriteString("&interpolateParams=true")
		} else {
			hasParam = true
			buf.WriteString("?interpolateParams=true")
		}
	}

	if cfg.Loc != time.UTC && cfg.Loc != nil {
		if hasParam {
			buf.WriteString("&loc=")
		} else {
			hasParam = true
			buf.WriteString("?loc=")
		}
		buf.WriteString(url.QueryEscape(cfg.Loc.String()))
	}

	if cfg.MultiStatements {
		if hasParam {
			buf.WriteString("&multiStatements=true")
		} else {
			hasParam = true
			buf.WriteString("?multiStatements=true")
		}
	}

	if cfg.ParseTime {
		if hasParam {
			buf.WriteString("&parseTime=true")
		} else {
			hasParam = true
			buf.WriteString("?parseTime=true")
		}
	}

	if cfg.ReadTimeout > 0 {
		if hasParam {
			buf.WriteString("&readTimeout=")
		} else {
			hasParam = true
			buf.WriteString("?readTimeout=")
		}
		buf.WriteString(cfg.ReadTimeout.String())
	}

	if cfg.RejectReadOnly {
		if hasParam {
			buf.WriteString("&rejectReadOnly=true")
		} else {
			hasParam = true
			buf.WriteString("?rejectReadOnly=true")
		}
	}

	if cfg.Timeout > 0 {
		if hasParam {
			buf.WriteString("&timeout=")
		} else {
			hasParam = true
			buf.WriteString("?timeout=")
		}
		buf.WriteString(cfg.Timeout.String())
	}

	if cfg.WriteTimeout > 0 {
		if hasParam {
			buf.WriteString("&writeTimeout=")
		} else {
			hasParam = true
			buf.WriteString("?writeTimeout=")
		}
		buf.WriteString(cfg.WriteTimeout.String())
	}

	if cfg.MaxAllowedPacket != defaultMaxAllowedPacket {
		if hasParam {
			buf.WriteString("&maxAllowedPacket=")
		} else {
			hasParam = true
			buf.WriteString("?maxAllowedPacket=")
		}
		buf.WriteString(strconv.Itoa(cfg.MaxAllowedPacket))

	}

	if cfg.Params != nil {
		var params []string
		for param := range cfg.Params {
			params = append(params, param)
		}
		sort.Strings(params)
		for _, param := range params {
			if hasParam {
				buf.WriteByte('&')
			} else {
				hasParam = true
				buf.WriteByte('?')
			}

			buf.WriteString(param)
			buf.WriteByte('=')
			buf.WriteString(url.QueryEscape(cfg.Params[param]))
		}
	}

	return buf.String()
}

func ParseDSN(dsn string) (cfg *Config, err error) {
	cfg = NewConfig()

	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			if i > 0 {
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Passwd = dsn[k+1 : j]
								break
							}
						}
						cfg.User = dsn[:k]

						break
					}
				}

				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							return nil, errInvalidDSNAddr
						}
						cfg.Addr = dsn[k+1 : i-1]
						break
					}
				}
				cfg.Net = dsn[j+1 : k]
			}

			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.DBName = dsn[i+1 : j]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	if err = cfg.normalize(); err != nil {
		return nil, err
	}

	return
}

func parseDSNParams(cfg *Config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)

		if len(param) != 2 {
			continue
		}

		switch value := param[1]; param[0] {
		case "clientFoundRows":
			var isBool bool
			cfg.ClientFoundRows, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}
		case "collation":
			cfg.Collation = value
			break
		case "columnsWithAlias":
			var isBool bool
			cfg.ColumnsWithAlias, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}
		case "compress":
			return errors.New("compression not implemented yet")
		case "interpolateParams":
			var isBool bool
			cfg.InterpolateParams, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}
		case "multiStatements":
			var isBool bool
			cfg.MultiStatements, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}
		case "parseTime":
			var isBool bool
			cfg.ParseTime, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}
		case "readTimeout":
			cfg.ReadTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		case "rejectReadOnly":
			var isBool bool
			cfg.RejectReadOnly, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}
		case "strict":
			panic("strict mode has been removed. See https://github.com/go-sql-driver/mysql/wiki/strict-mode")
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		case "writeTimeout":
			cfg.WriteTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		case "maxAllowedPacket":
			cfg.MaxAllowedPacket, err = strconv.Atoi(value)
			if err != nil {
				return
			}
		default:
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}

			if cfg.Params[param[0]], err = url.QueryUnescape(value); err != nil {
				return
			}
		}
	}

	return
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "3306")
	}
	return addr
}
