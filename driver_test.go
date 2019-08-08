package gohive

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenConnection(t *testing.T) {
	db, err := sql.Open("hive", "127.0.0.1:10000")
	assert.Nil(t, err)
	defer db.Close()
}

func TestOpenConnectionAgainstAuth(t *testing.T) {
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn?auth=PLAIN")
	rows, err := db.Query("SELECT customerID, gender FROM train")
	assert.EqualError(t, err, "Bad SASL negotiation status: 4 ()")
	defer db.Close()
	if err == nil {
		defer rows.Close()
	}
}

func TestQuery(t *testing.T) {
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	rows, err := db.Query("SELECT customerID, gender FROM train")
	assert.Nil(t, err)
	defer db.Close()
	defer rows.Close()

	n := 0
	customerid := ""
	gender := ""
	for rows.Next() {
		err := rows.Scan(&customerid, &gender)
		assert.Nil(t, err)
		n++
	}
	assert.Nil(t, rows.Err())
	assert.Equal(t, 82, n) // The imported data size is 82.
}

func TestColumnName(t *testing.T) {
	a := assert.New(t)
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	rows, err := db.Query("SELECT customerID, gender FROM train;")
	assert.Nil(t, err)
	defer db.Close()
	defer rows.Close()

	cl, err := rows.Columns()
	a.NoError(err)
	a.Equal(cl, []string{"customerid", "gender"})
}

func TestColumnTypeName(t *testing.T) {
	a := assert.New(t)
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	rows, err := db.Query("SELECT customerID, gender FROM train")
	assert.Nil(t, err)
	defer db.Close()
	defer rows.Close()

	ct, err := rows.ColumnTypes()
	a.NoError(err)
	for _, c := range ct {
		assert.Equal(t, c.DatabaseTypeName(), "VARCHAR_TYPE")
	}
}

func TestColumnType(t *testing.T) {
	a := assert.New(t)
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	rows, err := db.Query("SELECT customerID, gender FROM train")

	defer db.Close()
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	a.NoError(err)
	for _, ct := range cts {
		assert.Equal(t, reflect.TypeOf("string"), ct.ScanType())
	}
}

func TestShowCreateTable(t *testing.T) {
	a := assert.New(t)
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	rows, err := db.Query("show create table train")

	defer db.Close()
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	a.NoError(err)
	for _, ct := range cts {
		assert.Equal(t, reflect.TypeOf("string"), ct.ScanType())
	}
}

func TestDescribeTable(t *testing.T) {
	a := assert.New(t)
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	rows, err := db.Query("describe train")

	defer db.Close()
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	a.NoError(err)
	for _, ct := range cts {
		assert.Equal(t, reflect.TypeOf("string"), ct.ScanType())
	}
}

func TestShowDatabases(t *testing.T) {
	a := assert.New(t)
	db, _ := sql.Open("hive", "127.0.0.1:10000")
	rows, err := db.Query("show databases")

	defer db.Close()
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	a.NoError(err)
	for _, ct := range cts {
		assert.Equal(t, reflect.TypeOf("string"), ct.ScanType())
	}
}

func TestPing(t *testing.T) {
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	err := db.Ping()
	assert.Nil(t, err)
}

func TestExec(t *testing.T) {
	a := assert.New(t)
	db, _ := sql.Open("hive", "127.0.0.1:10000/churn")
	_, err := db.Exec("insert into churn.test (gender) values ('Female')")
	defer db.Close()
	a.NoError(err)
}
