package munch

import (
	"database/sql"
	"fmt"
	"reflect"
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

	data map[string]string
}

type filter struct {
	isOr       bool
	columnName string
	comparator string
	value      string
}

func (q *query) OrWhere(columnName, comparator, value string) {
	q.addFilter(columnName, comparator, value, true)
}

func (q *query) Where(columnName, comparator, value string) {
	q.addFilter(columnName, comparator, value, false)
}

func (q *query) AndWhere(columnName, comparator, value string) {
	q.addFilter(columnName, comparator, value, false)
}

func (q *query) addFilter(columnName, comparator, value string, isOr bool) {
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
		s := reflect.Indirect(v).FieldByName(field.Name).String()

		if len(s) > 0 {
			if len(sTag) > 0 {
				q.data[sTag] = s
			} else {
				q.data[field.Name] = s
			}
		}
	}
}

func (q *query) Insert(in interface{}) {
	q.operation = sql_INSERT

	if q.data == nil {
		q.data = make(map[string]string)
	}

	q.appendData(in)
}

func (q *query) Update(in interface{}) {
	q.operation = sql_UPDATE

	if q.data == nil {
		q.data = make(map[string]string)
	}

	q.appendData(in)
}

func (q *query) Del() {
	q.Delete()
}

func (q *query) Delete() {
	q.operation = sql_DELETE
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
		sqlStr = fmt.Sprintf("SELECT * FROM `%s`", q.table)
		break
	}

	if q.operation != sql_INSERT && len(q.where) > 0 {
		first := true
		filterSql = ""
		for _, filter := range q.where {
			predicate := "AND"
			if first {
				predicate = "WHERE"
				first = false
			} else if filter.isOr {
				predicate = "OR"
			}

			filterSql += fmt.Sprintf(" %s `%s` %s \"%s\"", predicate, filter.columnName, filter.comparator, filter.value)
		}
	}

	if len(q.data) > 0 {
		first := true
		if q.operation == sql_INSERT {
			cols := ""
			vals := ""

			for col, val := range q.data {
				if first {
					first = false
				} else {
					cols += ", "
					vals += ", "
				}
				cols += "`" + col + "`"
				vals += "\"" + val + "\""
			}

			dataSql += fmt.Sprintf(" (%s) VALUES (%s)", cols, vals)
		} else if q.operation == sql_UPDATE {
			colUpdates := ""
			first := true
			for col, val := range q.data {
				if first {
					first = false
				} else {
					colUpdates += ", "
				}
				colUpdates += fmt.Sprintf("`%s` = \"%s\"", col, val)
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
