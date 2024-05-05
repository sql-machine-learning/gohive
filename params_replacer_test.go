package gohive

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParamsInterpolator_Interpolate(t *testing.T) {
	shanghaiLoc, err := time.LoadLocation("Asia/Shanghai")
	assert.NoError(t, err)
	type fields struct {
		Local *time.Location
	}
	type args struct {
		query string
		args  []driver.Value
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "number of ? [1] must be equal to len(args): [2]",
			fields: fields{
				Local: time.Local,
			},
			args: args{
				query: "SELECT * FROM table_name WHERE id = ?;",
				args:  []driver.Value{int64(1), string("123")},
			},
			want:    "",
			wantErr: assert.Error,
		},
		{
			name: "int",
			fields: fields{
				Local: time.Local,
			},
			args: args{
				query: "SELECT * FROM table_name WHERE id = ?;",
				args:  []driver.Value{int64(1)},
			},
			want:    "SELECT * FROM table_name WHERE id = 1;",
			wantErr: assert.NoError,
		},
		{
			name: "string bytes time zone",
			fields: fields{
				Local: shanghaiLoc,
			},
			args: args{
				query: "INSERT INTO table_name (field1, field2, field3) VALUES (?, ?, ?,?);",
				args:  []driver.Value{int64(1), string("\"hello\""), []byte("123abc&()"), time.Date(2024, 5, 5, 0, 0, 0, 0, shanghaiLoc)},
			},
			want:    "INSERT INTO table_name (field1, field2, field3) VALUES (1, '\\\"hello\\\"', X'313233616263262829','2024-05-05 00:00:00');",
			wantErr: assert.NoError,
		},
		{
			name: "\\",
			fields: fields{
				Local: time.Local,
			},
			args: args{
				query: "UPDATE table_name SET field1 = ?, field2 = ? WHERE id = ?;",
				args:  []driver.Value{int64(1), string("\"hello\""), []byte("123")},
			},
			want:    "UPDATE table_name SET field1 = 1, field2 = '\\\"hello\\\"' WHERE id = X'313233';",
			wantErr: assert.NoError,
		},
		{
			name: "\\\\\\",
			fields: fields{
				Local: time.Local,
			},
			args: args{
				query: "DELETE FROM table_name WHERE id = ?;",
				args:  []driver.Value{string(`abc \\\&&&`)},
			},
			want:    "DELETE FROM table_name WHERE id = 'abc \\\\\\\\\\\\&&&';",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ParamsInterpolator{
				Local: tt.fields.Local,
			}
			got, err := p.Interpolate(tt.args.query, tt.args.args)
			if !tt.wantErr(t, err, fmt.Sprintf("Interpolate(%v, %v)", tt.args.query, tt.args.args)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Interpolate(%v, %v)", tt.args.query, tt.args.args)
		})
	}
}
