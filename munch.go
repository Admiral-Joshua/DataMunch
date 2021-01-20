package munch

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// SQL Operations - ENUM
const (
	sql_SELECT = iota
	sql_INSERT
	sql_UPDATE
	sql_DELETE
)

// TODO: Configuration / Function to Initialise instance of SQL Builder
type QueryBuilder struct {
	conn *sql.DB
}

func (q *QueryBuilder) Table(tableName string) *query {
	return &query{
		parent:    q,
		operation: sql_SELECT,
		table:     tableName,
	}
}

// TODO: Query Predicate
type query struct {
	parent *QueryBuilder

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

func formatValue(t reflect.Type, v reflect.Value) string {
	valStr := ""

	switch t.Kind() {
	case reflect.Int:
		valStr = strconv.FormatInt(v.Int(), 10)
		break
	case reflect.Bool:
		valStr = strings.ToUpper(strconv.FormatBool(v.Bool()))
		break
	case reflect.Float64:
		valStr = strconv.FormatFloat(v.Float(), 'f', -1, 64)
		break
	case reflect.Slice:
		valList := ""
		for i := 0; i < v.Len(); i++ {
			vIdx := v.Index(i)
			if i > 0 {
				valList += ", "
			}
			valList += formatValue(vIdx.Type(), vIdx)
		}
		if len(valList) > 0 {
			valStr = fmt.Sprintf("(%s)", valList)
		}
		break
	default:
		s := v.String()
		if len(s) > 0 {
			valStr = fmt.Sprintf("'%s'", v.String())
		}
		break
	}

	return valStr
}

func (q *query) ToSQL() string {
	var sqlStr string
	filterSql := ""
	dataSql := ""

	switch q.operation {
	case sql_INSERT:
		sqlStr = fmt.Sprintf("INSERT INTO `%s`", q.table)
		break
	case sql_UPDATE:
		sqlStr = fmt.Sprintf("UPDATE `%s`", q.table)
		break
	case sql_DELETE:
		sqlStr = fmt.Sprintf("DELETE FROM `%s`", q.table)
		break
	default:
		colString := "*"

		if len(q.columns) > 0 {
			colString = "`" + strings.Join(q.columns, "`, `") + "`"
		}

		sqlStr = fmt.Sprintf("SELECT %s FROM `%s`", colString, q.table)
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

			valStr = formatValue(fType, fValue)

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

				filterSql += fmt.Sprintf(" %s `%s` %s %s%s%s", predicate, filter.columnName, filter.comparator, escape, valStr, escape)
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

				valString := formatValue(valT, valV)

				if len(valString) > 0 {
					if first {
						first = false
					} else {
						cols += ", "
						values += ", "
					}
					cols += "`" + col + "`"

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

				valString := formatValue(valT, valV)

				if len(valString) > 0 {

					if first {
						first = false
					} else {
						colUpdates += ", "
					}

					colUpdates += fmt.Sprintf("`%s` = %s", col, valString)
				}
			}
			dataSql += fmt.Sprintf(" SET %s", colUpdates)
		}
	}

	return sqlStr + dataSql + filterSql + ";"
}

/*func (q *query) Execute(in interface{}, out interface{}) error {



	if in != nil {

	}
	return nil
}*/
