package gohive

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_columnRemoveTable(t *testing.T) {
	type args struct {
		n string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "",
			args: args{
				n: "t.name",
			},
			want: "name",
		},
		{
			name: "",
			args: args{
				n: "name",
			},
			want: "name",
		},
		{
			name: "",
			args: args{
				n: "",
			},
			want: "",
		},
		{
			name: "",
			args: args{
				n: ".",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, columnRemoveTable(tt.args.n), "columnRemoveTable(%v)", tt.args.n)
		})
	}
}
