package munch

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// SQL Formatting directives
const (
	mysql_TableWrap = "`"
	mysql_StringWrap = "\""
	psql_TableWrap = "\""
	psql_StringWrap = "'"
)

// SQL Operations - ENUM
const (
	sql_SELECT = iota
	sql_INSERT
	sql_UPDATE
	sql_DELETE
)

// Client Types - ENUM
const (
	MySQL = iota
	Postgres
)

// TODO: Configuration / Function to Initialise instance of SQL Builder
type queryBuilder struct {
	conn *sql.DB
	client int
}

func NewQueryBuilder(config SQLConfig) (*queryBuilder, error) {
	var (
		connString string
		driverName string
		err error
	)
	switch config.Client {
	case Postgres:
		connString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", config.Host, config.Port, config.User, config.Pass, config.DBName, config.SSLMode)
		driverName = "postgres"
		break
	case MySQL:
	default:
		connString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Pass, config.Host, config.Port, config.DBName)
		driverName = "mysql"
		break
	}

	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, err
	}

	return &queryBuilder{conn: db, client: config.Client}, nil
}

type SQLConfig struct {
	Client int
	Host string
	Port int
	User string
	Pass string
	DBName string
	SSLMode string
}

func (q *queryBuilder) Table(tableName string) *query {
	return &query{
		parent:    q,
		operation: sql_SELECT,
		table:     tableName,
	}
}

// TODO: Query Predicate
type query struct {
	parent *queryBuilder

	operation int
	table     string
	where     []filter
	columns   []string

	data map[string]interface{}
}

type filter struct {
	isOr       bool
	columnName string
	comparator string
	value      interface{}
}

func getColumns(in reflect.Type) []string {
	cols := make([]string, 0)
	for x := 0; x < in.NumField(); x++ {
		var columnName string
		sqlTag := in.Field(x).Tag.Get("sql")
		if len(sqlTag) > 0 {
			columnName = sqlTag
		} else {
			columnName = in.Field(x).Name
		}

		cols = append(cols, columnName)
	}

	return cols
}

func (q *query) Select(cols []string) {
	q.columns = cols
}

func (q *query) Where(in interface{}) {
	t := reflect.TypeOf(in)

	if t.Kind() == reflect.Slice {
		mySlice := reflect.ValueOf(in)
		for i := 0; i < mySlice.Len(); i++ {
			obj := mySlice.Index(i).Interface()

			q.Where(obj)
		}
	} else if t.Kind() == reflect.Struct {
		cols := getColumns(t)

		q.filterObj(in, cols)
	}
}

func (q *query) WhereIn(columnName string, values interface{}, isOr bool) {
	q.addFilter(columnName, "IN", values, isOr)
}

func (q *query) filterObj(obj interface{}, columns []string) {
	v := reflect.ValueOf(obj)

	for i := 0; i < len(columns); i++ {
		fieldVal := v.Field(i)
		//fieldStr := fieldVal.String()
		if fieldVal.IsValid() {
			q.addFilter(columns[i], "=", fieldVal.Interface(), false)
		}
	}
}

func (q *query) OrWhereRaw(columnName, comparator string, value interface{}) {
	q.addFilter(columnName, comparator, value, true)
}

func (q *query) WhereRaw(columnName, comparator string, value interface{}) {
	q.addFilter(columnName, comparator, value, false)
}

func (q *query) AndWhereRaw(columnName, comparator string, value interface{}) {
	q.addFilter(columnName, comparator, value, false)
}

func (q *query) addFilter(columnName, comparator string, value interface{}, isOr bool) {
	q.where = append(q.where, filter{
		isOr:       isOr,
		columnName: columnName,
		comparator: comparator,
		value:      value,
	})
}

func (q *query) appendData(in interface{}) {
	t := reflect.TypeOf(in)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		sTag := field.Tag.Get("sql")

		v := reflect.ValueOf(in)
		//s := reflect.Indirect(v).FieldByName(field.Name)
		s := v.Field(i).Interface()

		if len(sTag) > 0 {
			q.data[sTag] = s
		} else {
			q.data[field.Name] = s
		}
	}
}

func (q *query) Insert(in interface{}) {
	q.operation = sql_INSERT

	if q.data == nil {
		q.data = make(map[string]interface{})
	}

	q.appendData(in)
}

func (q *query) Update(in interface{}) {
	q.operation = sql_UPDATE

	if q.data == nil {
		q.data = make(map[string]interface{})
	}

	q.appendData(in)
}

func (q *query) Del() {
	q.Delete()
}

func (q *query) Delete() {
	q.operation = sql_DELETE
}

func formatValue(t reflect.Type, v reflect.Value, value_wrap string) string {
	valStr := ""

	if t.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
	}

	switch t.Kind() {
	case reflect.Int:
	case reflect.Int8:
	case reflect.Int16:
	case reflect.Int32:
	case reflect.Int64:
		valStr = strconv.FormatInt(v.Int(), 10)
	case reflect.Uint:
	case reflect.Uint8:
	case reflect.Uint16:
	case reflect.Uint32:
	case reflect.Uint64:
		valStr = strconv.FormatUint(v.Uint(), 10)
	case reflect.Bool:
		valStr = strings.ToUpper(strconv.FormatBool(v.Bool()))
	case reflect.Float64:
		valStr = strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Slice:
		valList := ""
		for i := 0; i < v.Len(); i++ {
			vIdx := v.Index(i)
			if i > 0 {
				valList += ", "
			}
			valList += formatValue(vIdx.Type(), vIdx, value_wrap)
		}
		if len(valList) > 0 {
			valStr = fmt.Sprintf("(%s)", valList)
		}
	default:
		s := v.String()
		if len(s) > 0 {
			valStr = fmt.Sprintf("%s%s%s", value_wrap, v.String(), value_wrap)
		}
	}

	return valStr
}

func (q *query) SQL() string {
	var (
		wrap_table string
		wrap_string string
	)

	switch q.parent.client {
	case Postgres:
		wrap_table = psql_TableWrap
		wrap_string = psql_StringWrap
		break
	case MySQL:
	default:
		wrap_table = mysql_TableWrap
		wrap_string = mysql_StringWrap
		break
	}

	var sqlStr string
	filterSql := ""
	dataSql := ""

	switch q.operation {
	case sql_INSERT:
		sqlStr = fmt.Sprintf("INSERT INTO %s%s%s", wrap_table, q.table, wrap_table)
		break
	case sql_UPDATE:
		sqlStr = fmt.Sprintf("UPDATE %s%s%s", wrap_table, q.table, wrap_table)
		break
	case sql_DELETE:
		sqlStr = fmt.Sprintf("DELETE FROM %s%s%s", wrap_table, q.table, wrap_table)
		break
	default:
		colString := "*"

		if len(q.columns) > 0 {
			colString = "`" + strings.Join(q.columns, "`, `") + "`"
		}

		sqlStr = fmt.Sprintf("SELECT %s FROM %s%s%s", colString, wrap_table, q.table, wrap_table)
		break
	}

	if q.operation != sql_INSERT && len(q.where) > 0 {
		first := true
		filterSql = ""
		for _, filter := range q.where {
			var (
				valStr string
				escape = ""
			)

			fType := reflect.TypeOf(filter.value)
			fValue := reflect.ValueOf(filter.value)

			valStr = formatValue(fType, fValue, wrap_string)

			if len(valStr) > 2 {
				predicate := "AND"
				if first {
					predicate = "WHERE"
					first = false
				} else if filter.isOr {
					predicate = "OR"
				}

				if fType.Kind() == reflect.Slice {
					filter.comparator = "IN"
				}

				filterSql += fmt.Sprintf(" %s %s%s%s %s %s%s%s", predicate, wrap_table, filter.columnName, wrap_table, filter.comparator, escape, valStr, escape)
			}
		}
	}

	if len(q.data) > 0 {
		first := true
		if q.operation == sql_INSERT {
			cols := ""
			values := ""

			for col, val := range q.data {
				valT := reflect.TypeOf(val)
				valV := reflect.ValueOf(val)

				valString := ""
				if val != nil {
					 valString = formatValue(valT, valV, wrap_string)
				}

				if len(valString) > 0 {
					if first {
						first = false
					} else {
						cols += ", "
						values += ", "
					}
					cols += fmt.Sprintf("%s%s%s", wrap_table, col, wrap_table)

					values += valString
				}
			}

			dataSql += fmt.Sprintf(" (%s) VALUES (%s)", cols, values)
		} else if q.operation == sql_UPDATE {
			colUpdates := ""
			first := true
			for col, val := range q.data {
				valT := reflect.TypeOf(val)
				valV := reflect.ValueOf(val)

				valString := formatValue(valT, valV, wrap_string)

				if len(valString) > 0 {

					if first {
						first = false
					} else {
						colUpdates += ", "
					}

					colUpdates += fmt.Sprintf("%s%s%s = %s", wrap_table, col, wrap_table, valString)
				}
			}
			dataSql += fmt.Sprintf(" SET %s", colUpdates)
		}
	}

	return sqlStr + dataSql + filterSql + ";"
}

/*func rowToStruct(row *sql.Rows, out interface{}) error {
	outType := reflect.TypeOf(out)

	colData, _ := row.Columns()
	colDefs := getColumns(outType)



	outValue := reflect.ValueOf(out)

	for i := 0; i < outType.NumField(); i++ {
		field := outType.Field(i)

		outValue.Field(i)
	}
}*/

func structToColumnNames(in interface{}) map[string]int {
	retVal := make(map[string]int)

	inT := reflect.TypeOf(in).Elem()

	for i := 0; i < inT.NumField(); i++ {
		field := inT.Field(i)
		sqlTag := field.Tag.Get("sql")
		if len(sqlTag) > 0 {
			retVal[sqlTag] = i
		} else {
			retVal[field.Name] = i
		}
	}
	return retVal
}

func getStructDef(out chan map[string]int, T reflect.Type) {
	structIdx := make(map[string]int)
	for i := 0; i < T.NumField(); i++ {
		nameOverride := T.Field(i).Tag.Get("sql")

		if len(nameOverride) < 1{
			nameOverride = T.Field(i).Name
		}

		structIdx[nameOverride] = i
	}

	out <- structIdx
}

func scanRow(row *sql.Rows, out interface{}) {
	columns, _ := row.Columns()
	count := len(columns)

	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	outType := reflect.TypeOf(out).Elem()

	// ASYNC - Use the struct definition to map SQL column names to Field indexes.
	structChan := make(chan map[string]int, 1)
	// .Elem() looks underneath the pointer to the struct type itself.
	go getStructDef(structChan, outType)

	// Populate the slice of pointers with references to actual values.
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Scan row content into the points.
	row.Scan(valuePtrs...)

	// Now look under the pointer to the value itself.
	val := reflect.ValueOf(out).Elem()

	// Retrieve the result from the async struct processor.
	structIdx := <- structChan

	// Now reflect the result retrieved from DB into the struct.
	for i, colName := range columns {
		field := val.Field(structIdx[colName])

		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&values[i]))
		} else {
			field.Set(reflect.ValueOf(values[i]))
		}

	}
}

func (q *query) Exec(out interface{}) error {

	// Run the SQL statement constructed this far.
	rows, err := q.parent.conn.Query(q.SQL())
	if err != nil {
		return err
	}

	// Don't bother trying to retrieve any results, if no result object was passed
	if out == nil || reflect.ValueOf(out).IsNil() {
		return nil
	}

	outType := reflect.TypeOf(out)

	if outType.Kind() == reflect.Ptr {
		outType = outType.Elem()
	}

	switch outType.Kind() {
	case reflect.Slice:
		// Get the type under the slice...

		// For each row...
		for rows.Next() {
			// Create new struct to store this row.
			obj := reflect.New(outType.Elem())

			scanRow(rows, &obj)

			out = append(out.([]interface{}), obj)
		}

	case reflect.Struct:
		row, err := q.parent.conn.Query(q.SQL())
		if err != nil {
			return err
		}

		if row.Next() {
			scanRow(row, out)
		} else {
			return nil
		}
	}

	return nil
}