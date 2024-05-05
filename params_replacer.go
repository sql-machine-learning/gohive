package gohive

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	TimeStampLayout = "2006-01-02 15:04:05.999999999"
	DateLayout      = "2006-01-02"
)

type ParamsInterpolator struct {
	Local *time.Location
}

func NewParamsInterpolator() *ParamsInterpolator {
	return &ParamsInterpolator{
		Local: time.Local,
	}
}

func (p *ParamsInterpolator) InterpolateNamedValue(query string, namedArgs []driver.NamedValue) (string, error) {
	args, err := namedValueToValue(namedArgs)
	if err != nil {
		return "", err
	}
	return p.Interpolate(query, args)
}

func (p *ParamsInterpolator) Interpolate(query string, args []driver.Value) (string, error) {
	if strings.Count(query, "?") != len(args) {
		return "", fmt.Errorf("gohive driver: number of ? [%d] must be equal to len(args): [%d]",
			strings.Count(query, "?"), len(args))
	}

	var err error

	argIdx := 0
	var buf = make([]byte, 0, len(query)+len(args)*15)
	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argIdx]
		argIdx++

		buf, err = p.interpolateOne(buf, arg)
		if err != nil {
			return "", fmt.Errorf("gohive driver: failed to interpolate failed: %w, args[%d]: [%v]",
				err, argIdx, arg)
		}

	}
	if argIdx != len(args) {
		return "", fmt.Errorf("gohive driver: args are not all filled into SQL, argIdx: %d, total: %d",
			argIdx, len(args))
	}
	return string(buf), nil

}

func (p *ParamsInterpolator) interpolateOne(buf []byte, arg driver.Value) ([]byte, error) {
	if arg == nil {
		buf = append(buf, "NULL"...)
		return buf, nil
	}

	switch v := arg.(type) {
	case int64:
		buf = strconv.AppendInt(buf, v, 10)
	case uint64:
		// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
		buf = strconv.AppendUint(buf, v, 10)
	case float64:
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
	case bool:
		if v {
			buf = append(buf, "'true'"...)
		} else {
			buf = append(buf, "'false'"...)
		}
	case time.Time:
		if v.IsZero() {
			buf = append(buf, "'0000-00-00'"...)
		} else {
			buf = append(buf, '\'')
			buf = appendDateTime(buf, v.In(p.Local))
			buf = append(buf, '\'')
		}
	case json.RawMessage:
		buf = append(buf, '\'')
		buf = appendBytes(buf, v)
		buf = append(buf, '\'')
	case []byte:
		if v == nil {
			buf = append(buf, "NULL"...)
		} else {
			buf = append(buf, "X'"...)
			buf = appendBytes(buf, v)
			buf = append(buf, '\'')
		}
	case string:
		buf = append(buf, '\'')
		buf = escapeStringBackslash(buf, v)
		buf = append(buf, '\'')
	default:
		return nil, fmt.Errorf("gohive driver: unexpected args type: %T", arg)
	}
	return buf, nil
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	args := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, fmt.Errorf("gohive driver: driver does not support the use of Named Parameters")
		}
		args[n] = param.Value
	}
	return args, nil
}

func appendBytes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)+hex.EncodedLen(len(v)))
	pos += hex.Encode(buf[pos:], v)
	return buf[:pos]
}

func appendDateTime(buf []byte, t time.Time) []byte {
	buf = t.AppendFormat(buf, TimeStampLayout)
	return buf
}

func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos+1] = '0'
			buf[pos] = '\\'
			pos += 2
		case '\n':
			buf[pos+1] = 'n'
			buf[pos] = '\\'
			pos += 2
		case '\r':
			buf[pos+1] = 'r'
			buf[pos] = '\\'
			pos += 2
		case '\x1a':
			buf[pos+1] = 'Z'
			buf[pos] = '\\'
			pos += 2
		case '\'':
			buf[pos+1] = '\''
			buf[pos] = '\\'
			pos += 2
		case '"':
			buf[pos+1] = '"'
			buf[pos] = '\\'
			pos += 2
		case '\\':
			buf[pos+1] = '\\'
			buf[pos] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}
