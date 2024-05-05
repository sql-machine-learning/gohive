package gohive

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	hiveserver2 "sqlflow.org/gohive/hiveserver2/gen-go/tcliservice"
)

// rowSet implements the interface database/sql/driver.Rows.
type rowSet struct {
	thrift    *hiveserver2.TCLIServiceClient
	operation *hiveserver2.TOperationHandle
	options   hiveOptions

	columns    []*hiveserver2.TColumnDesc
	columnStrs []string

	offset int
	rowSet *hiveserver2.TRowSet

	// resultSet is column-oriented storage format
	resultSet [][]interface{}
	status    *hiveStatus

	ctx context.Context
}

type hiveStatus struct {
	state *hiveserver2.TOperationState
}

func (r *rowSet) Next(dest []driver.Value) error {
	if r.status == nil || !r.status.isStopped() {
		err := r.wait()
		if err != nil {
			return nil
		}
	}
	if r.status == nil {
		return fmt.Errorf("could not get job status.")
	}
	if !r.status.isFinished() {
		return fmt.Errorf("job failed.")
	}
	// First execution or reach the end of the current result set.
	if r.resultSet == nil || r.offset >= len(r.resultSet[0]) {
		r.offset = 0
		if err := r.batchFetch(); err != nil {
			return err
		}
	}

	if len(r.resultSet) <= 0 {
		return fmt.Errorf("the length of resultSet is not greater than zero.")
	}
	// Fill in dest with one single row data.
	for colIndex, values := range r.resultSet {
		// Reach to the end of the last result set.
		if len(values) == 0 {
			return io.EOF
		}
		dest[colIndex] = values[r.offset]
	}
	r.offset++
	return nil
}

// Returns the names of the columns for the given operation,
// blocking if necessary until the information is available.
func (r *rowSet) Columns() []string {
	if r.columnStrs == nil {
		if r.status == nil || !r.status.isStopped() {
			err := r.wait()
			if err != nil {
				return nil
			}
		}
		if r.status == nil || !r.status.isFinished() {
			return nil
		}
		ret := make([]string, len(r.columns))
		for i, col := range r.columns {
			if r.options.ColumnsWithoutTableName {
				ret[i] = columnRemoveTable(col.ColumnName)
				continue
			}
			ret[i] = col.ColumnName
		}
		r.columnStrs = ret
	}
	return r.columnStrs
}

func columnRemoveTable(n string) string {
	index := strings.Index(n, ".")
	if index == -1 {
		return n
	}
	return n[index+1:]
}

func (r *rowSet) Close() (err error) {
	return nil
}

var (
	scanTypeVarchar = reflect.TypeOf("varchar")
	scanTypeBool    = reflect.TypeOf(true)
	scanTypeFloat32 = reflect.TypeOf(float32(0))
	scanTypeFloat64 = reflect.TypeOf(float64(0))
	scanTypeInt8    = reflect.TypeOf(int8(0))
	scanTypeInt16   = reflect.TypeOf(int16(0))
	scanTypeInt32   = reflect.TypeOf(int32(0))
	scanTypeInt64   = reflect.TypeOf(int64(0))
	scanTypeUnknown = reflect.TypeOf(new(interface{}))
)

func (r *rowSet) ColumnTypeScanType(i int) reflect.Type {
	ct := r.columns[i].TypeDesc.Types[0].PrimitiveEntry.Type
	switch ct {
	case hiveserver2.TTypeId_STRING_TYPE:
		return scanTypeVarchar
	case hiveserver2.TTypeId_VARCHAR_TYPE:
		return scanTypeVarchar
	case hiveserver2.TTypeId_BOOLEAN_TYPE:
		return scanTypeBool
	case hiveserver2.TTypeId_TINYINT_TYPE:
		return scanTypeInt8
	case hiveserver2.TTypeId_SMALLINT_TYPE:
		return scanTypeInt16
	case hiveserver2.TTypeId_INT_TYPE:
		return scanTypeInt32
	case hiveserver2.TTypeId_BIGINT_TYPE:
		return scanTypeInt64
	case hiveserver2.TTypeId_TIMESTAMP_TYPE:
		return scanTypeInt64
	case hiveserver2.TTypeId_FLOAT_TYPE:
		return scanTypeFloat32
	case hiveserver2.TTypeId_DOUBLE_TYPE:
		return scanTypeFloat64
	case hiveserver2.TTypeId_DECIMAL_TYPE:
		return scanTypeFloat32
	default:
		return scanTypeUnknown
	}
}

func (r *rowSet) ColumnTypeDatabaseTypeName(i int) string {
	return r.columns[i].TypeDesc.Types[0].PrimitiveEntry.Type.String()
}

// Issue a thrift call to check for the job's current status.
func (r *rowSet) poll() error {
	req := hiveserver2.NewTGetOperationStatusReq()
	req.OperationHandle = r.operation

	resp, err := r.thrift.GetOperationStatus(r.ctx, req)
	if err != nil {
		return fmt.Errorf("Error getting status: %+v, %v", resp, err)
	}
	if !isSuccessStatus(resp.Status) {
		return fmt.Errorf("GetStatus call failed: %s", resp.Status.String())
	}
	if resp.OperationState == nil {
		return errors.New("No error from GetStatus, but nil status!")
	}
	r.status = &hiveStatus{resp.OperationState}
	return nil
}

func (r *rowSet) wait() error {
	for {
		err := r.poll()
		if err != nil {
			return err
		}
		if r.status.isStopped() {
			if r.status.isFinished() {
				metadataReq := hiveserver2.NewTGetResultSetMetadataReq()
				metadataReq.OperationHandle = r.operation

				metadataResp, err := r.thrift.GetResultSetMetadata(r.ctx, metadataReq)
				if err != nil {
					return err
				}
				if !isSuccessStatus(metadataResp.Status) {
					return fmt.Errorf("GetResultSetMetadata failed: %s",
						metadataResp.Status.String())
				}
				r.columns = metadataResp.Schema.Columns
				return nil
			} else {
				return fmt.Errorf("Query failed execution: %s", r.status.state.String())
			}
		}
		time.Sleep(time.Duration(r.options.PollIntervalSeconds) * time.Second)
	}
}

func (r *rowSet) batchFetch() error {
	fetchReq := hiveserver2.NewTFetchResultsReq()
	fetchReq.OperationHandle = r.operation
	fetchReq.Orientation = hiveserver2.TFetchOrientation_FETCH_NEXT
	fetchReq.MaxRows = r.options.BatchSize

	resp, err := r.thrift.FetchResults(r.ctx, fetchReq)
	if err != nil {
		return err
	}
	if !isSuccessStatus(resp.Status) {
		return fmt.Errorf("FetchResults failed: %s\n", resp.Status.String())
	}
	r.rowSet = resp.GetResults()

	rs := r.rowSet.Columns
	colLen := len(rs)
	r.resultSet = make([][]interface{}, colLen)

	for i := 0; i < colLen; i++ {
		typeDesc := r.columns[i].TypeDesc
		v, length, err := convertColumn(rs[i], typeDesc)
		if err != nil {
			return fmt.Errorf("convertColumn failed: %w, col_idx: %d, value: %+v, type: %+v",
				err, i, rs[i], typeDesc)
		}
		c := make([]interface{}, length)
		for j := 0; j < length; j++ {
			c[j] = reflect.ValueOf(v).Index(j).Interface()
		}
		r.resultSet[i] = c
	}
	return nil
}

var (
	dateTypeSet = map[hiveserver2.TTypeId]bool{
		hiveserver2.TTypeId_DATE_TYPE:      true,
		hiveserver2.TTypeId_TIMESTAMP_TYPE: true,
		//hiveserver2.TTypeId_TIMESTAMPLOCALTZ_TYPE: true, // TODO: suport TIMESTAMPLOCALTZ
	}
)

func convertColumn(col *hiveserver2.TColumn, typeDesc *hiveserver2.TTypeDesc) (colValues interface{}, length int, err error) {
	types := typeDesc.GetTypes()
	var primitiveTypeID hiveserver2.TTypeId
	var typ *hiveserver2.TTypeEntry
	if len(types) > 0 {
		typ = types[0]
		if typ != nil && typ.GetPrimitiveEntry() != nil {
			primitiveTypeID = typ.GetPrimitiveEntry().GetType()
		}
	}
	switch {
	case col.IsSetStringVal():
		strValues, length := col.GetStringVal().GetValues(), len(col.GetStringVal().GetValues())
		if dateTypeSet[primitiveTypeID] {
			colValues, err = convertToDate(strValues, primitiveTypeID)
			if err != nil {
				return nil, 0, err
			}
			return colValues, length, nil
		}
		return strValues, length, nil
	case col.IsSetBoolVal():
		return col.GetBoolVal().GetValues(), len(col.GetBoolVal().GetValues()), nil
	case col.IsSetByteVal():
		return col.GetByteVal().GetValues(), len(col.GetByteVal().GetValues()), nil
	case col.IsSetI16Val():
		return col.GetI16Val().GetValues(), len(col.GetI16Val().GetValues()), nil
	case col.IsSetI32Val():
		return col.GetI32Val().GetValues(), len(col.GetI32Val().GetValues()), nil
	case col.IsSetI64Val():
		return col.GetI64Val().GetValues(), len(col.GetI64Val().GetValues()), nil
	case col.IsSetDoubleVal():
		return col.GetDoubleVal().GetValues(), len(col.GetDoubleVal().GetValues()), nil
	default:
		return nil, 0, nil
	}
}

func convertToDate(values []string, primitiveTypeID hiveserver2.TTypeId) (res []time.Time, err error) {
	for _, v := range values {
		switch primitiveTypeID {
		case hiveserver2.TTypeId_DATE_TYPE:
			t, err := time.ParseInLocation(DateLayout, v, time.Local)
			if err != nil {
				return nil, err
			}
			res = append(res, t)
		case hiveserver2.TTypeId_TIMESTAMP_TYPE:
			t, err := time.ParseInLocation(TimeStampLayout, v, time.Local)
			if err != nil {
				return nil, err
			}
			res = append(res, t)
		//case hiveserver2.TTypeId_TIMESTAMPLOCALTZ_TYPE: // TODO: support TIMESTAMPLOCALTZ
		default:
			return nil, fmt.Errorf("convertToDate failed, unsupported type %s", primitiveTypeID)
		}
	}
	return res, nil
}

func (s hiveStatus) isStopped() bool {
	if s.state == nil {
		return false
	}
	switch *s.state {
	case hiveserver2.TOperationState_FINISHED_STATE,
		hiveserver2.TOperationState_CANCELED_STATE,
		hiveserver2.TOperationState_CLOSED_STATE,
		hiveserver2.TOperationState_ERROR_STATE:
		return true
	}
	return false
}

func (s hiveStatus) isFinished() bool {
	return s.state != nil && *s.state == hiveserver2.TOperationState_FINISHED_STATE
}

func newRows(thrift *hiveserver2.TCLIServiceClient, operation *hiveserver2.TOperationHandle, options hiveOptions, ctx context.Context) driver.Rows {
	return &rowSet{thrift, operation, options, nil, nil,
		0, nil, nil, nil, ctx}
}
