package db

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var modelInfoCached = map[string]*modelInfo{}

type modelInfo struct {

}

type collection struct {
	distinct string
	field string
	table string
	join [][]string
	multi []string
	where [][]interface{}
	order []string
	group string
	having string
	limit string
}

type Query struct {
	row Row
	table string
	alias string
	value interface{}
	model *modelInfo
	options collection
	bind []interface{}
	expression map[string]string
}

func (this *Query) init() *Query {
	this.options = collection{
		join: [][]string{},
		multi: []string{},
		where: [][]interface{}{},
		order: []string{},
	}

	this.bind = []interface{}{}

	this.expression = map[string]string {
		"eq": "=",
		"neq": "<>",
		"gt": ">",
		"egt": ">=",
		"lt": "<",
		"elt": "<=",
		"notlike": "NOT LIKE",
		"not like": "NOT LIKE",
		"like": "LIKE",
		"in": "IN",
		"exp": "EXP",
		"notin": "NOT IN",
		"not in": "NOT IN",
		"between": "BETWEEN",
		"not between": "NOT BETWEEN",
		"notbetween": "NOT BETWEEN",
		"exists": "EXISTS",
		"notexists": "NOT EXISTS",
		"not exists": "NOT EXISTS",
		"null": "NULL",
		"notnull": "NOT NULL",
		"not null": "NOT NULL",
	}

	if this.value != nil {
		this.initOfModel()
	}

	return this
}

func (this *Query) initOfModel() {
	val := reflect.ValueOf(this.value)
	ind := reflect.Indirect(val)
	typ := ind.Type()

	if val.Kind() != reflect.Ptr {
		this.model = nil
		return
	}

	name := typ.PkgPath() + "." + typ.Name()

	if mi, ok := modelInfoCached[name]; ok {
		this.model = mi
	}
}

func (this *Query) Options(key string) interface{} {
	switch strings.ToLower(key) {
	case "field":
		return this.options.field
	case "join":
		return this.options.join
	case "multi":
		return this.options.multi
	case "where":
		return this.options.where
	case "order":
		return this.options.order
	case "distinct":
		return this.options.distinct
	case "group":
		return this.options.group
	case "having":
		return this.options.having
	case "limit":
		return this.options.limit
	}

	return nil
}

func (this *Query) Alias(alias string) *Query {
	this.alias = alias
	return this
}

func (this *Query) Bind(args ...interface{}) *Query {
	for i := 0; i < len(args); i++ {
		this.bind = append(this.bind, args[i])
	}

	return this
}

func (this *Query) Distinct(distinct string) *Query {
	this.options.distinct = distinct
	return this
}

func (this *Query) Field(field string) *Query {
	this.options.field = field
	return this
}

func (this *Query) Join(join string, condition string, types ...string) *Query {
	ty := "INNER"
	if len(types) > 0 && types[0] != "" {
		ty = types[0]
	}

	this.options.join = append(this.options.join, []string{join, condition, ty})
	return this
}

func (this *Query) contains(data string, array []string) bool {
	for _, value := range array {
		if value == data {
			return true
		}
	}

	return false
}

func (this *Query) Where(args ...interface{}) *Query {
	if len(args) == 0 {
		return this
	}

	if data, ok := args[0].(string); ok && regexp.MustCompile(`[,=\>\<\'\"\(\s]`).Match([]byte(data)){
		this.options.multi = append(this.options.multi, data)

		if len(args) > 1 {
			if data, ok := args[1].(map[string]interface{}); ok {
				this.Bind(data)
			}
		}
	} else if len(args) == 1 {
		switch args[0].(type) {
		case []interface{}:
			this.options.where = append(this.options.where, args[0].([]interface{}))
		case string:
			this.options.where = append(this.options.where, []interface{}{args[0].(string), "null", ""})
		}
	} else if data, ok := args[1].(string); ok && this.contains(data, []string{"null", "notnull", "not null"}) {
		this.options.where = append(this.options.where, []interface{}{args[0].(string), data})
	} else if len(args) == 2 {
		switch args[1].(type) {
		case string, int, int64, float32, float64:
			this.options.multi = append(this.options.multi, args[0].(string) + " = " + this.parseValue(args[1]))
		case []int, []string:
			this.options.where = append(this.options.where, []interface{}{args[0].(string), "in", args[1]})
		}

	} else {
		op, _ := args[1].(string)
		if op == "exp" {
			this.options.where = append(this.options.where, []interface{}{args[0].(string), "exp", args[2]})
		} else {
			this.options.where = append(this.options.where, []interface{}{args[0].(string), args[1], args[2]})
		}
	}

	return this
}

func (this *Query) Group(group string) *Query {
	this.options.group = group
	return this
}

func (this *Query) Having(having string) *Query {
	this.options.having = having
	return this
}

func (this *Query) Order(field string, orders ...string) *Query {
	if field == "" {
		return this
	}

	if strings.Contains(field, ",") {
		temp := strings.Split(field, ",")
		for _, value := range temp {
			this.options.order = append(this.options.order, value)
		}
	} else if len(orders) > 0 {
		this.options.order = append(this.options.order, field + " " + orders[0])
	} else if len(orders) == 0 {
		this.options.order = append(this.options.order, field)
	}

	return this
}

func (this *Query) Limit(args ...int) *Query {
	if len(args) > 1 {
		this.options.limit = strconv.Itoa(args[0]) + "," + strconv.Itoa(args[1])
	} else if len(args) == 1 {
		this.options.limit = "0, " + strconv.Itoa(args[0])
	}

	return this
}

func (this *Query) Find() Row {
	this.Limit(1)

	if this.table == ""{
		return nil
	}

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	return FetchFirst(tsql, this.bind...)
}

func (this *Query) First() interface{} {
	if this.model != nil {
		this.Find()
	}

	return this.value
}

func (this *Query) Select() []Row {
	if this.table == ""{
		return []Row{}
	}

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	return FetchRows(tsql, this.bind...)
}

func (this *Query) Search() []interface{}{
	return nil
}

func (this *Query) Insert(row Row) int64 {
	return Insert(this.table, row)
}

func (this *Query) InsertBatch(rows []Row) int64 {
	return InsertBatch(this.table, rows)
}

func (this *Query) Create() int64 {
	if this.table == "" {
		return 0
	}

	return this.Insert(this.row)
}

func (this *Query) Update(args ...Row) int64 {
	if this.table == ""{
		return 0
	}

	var row Row
	if len(args) == 0 {
		row = this.row
	} else {
		row = args[0]
	}

	options := this.parseExpress()
	where := this.parseWhere(options)

	if where != "" {
		where = where[7:]
	}

	return Update(this.table, row, where)
}

func (this *Query) Delete() int64 {
	if this.table == "" {
		return 0
	}

	options := this.parseExpress()
	where := this.parseWhere(options)

	if where != "" {
		where = where[7:]
	}

	return Delete(this.table, where)
}

func (this *Query) Count() int64 {
	if this.table == ""{
		return 0
	}

	result := this.aggregate("COUNT", "1")
	if result == nil {
		return 0
	}

	return result.(int64)
}

func (this *Query) Sum(field string) interface{} {
	return this.aggregate("SUM", field)
}

func (this *Query) Avg(field string) interface{} {
	return this.aggregate("AVG", field)
}

func (this *Query) Max(field string) interface{} {
	return this.aggregate("MAX", field)
}

func (this *Query) Min(field string) interface{} {
	return this.aggregate("MIN", field)
}

func (this *Query) aggregate(aggregate string, field string) interface{} {
	if this.table == ""{
		return 0
	}

	this.Field(aggregate + "(" + field + ")").Limit(1)

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	return ResultFirst(tsql, this.bind...)
}

func (this *Query) buildSelectSql(options collection) string {
	var tsql = strings.Builder{}

	tsql.WriteString("SELECT")
	tsql.WriteString(this.parseDistinct(options.distinct))
	tsql.WriteString(" ")
	tsql.WriteString(this.parseField(options.field))
	tsql.WriteString(" FROM ")
	tsql.WriteString(options.table)

	if this.alias != "" {
		tsql.WriteString(" " + this.alias + " ")
	}

	tsql.WriteString(this.parseJoin(options.join))
	tsql.WriteString(this.parseWhere(options))
	tsql.WriteString(this.parseGroup(options.group))
	tsql.WriteString(this.parseHaving(options.having))
	tsql.WriteString(this.parseOrder(options.order))
	tsql.WriteString(this.parseLimit(options.limit))

	return tsql.String()
}

func (this *Query) parseValue(value interface{}) string {
	switch value.(type) {
	case string:
		temp := value.(string)
		if strings.Contains(temp, ".") {
			return temp
		} else {
			return "'" + temp + "'"
		}
	case int:
		return strconv.Itoa(value.(int))
	case int64:
		return strconv.FormatInt(value.(int64), 10)
	case float32:
		return strconv.FormatFloat(float64(value.(float32)), 'f', 2, 32)
	case float64:
		return strconv.FormatFloat(value.(float64), 'f', 2, 64)
	}

	return ""
}

func (this *Query) parseExpress() collection {
	options := this.options

	if options.table == "" {
		options.table = this.table
	}

	if options.field == "" {
		options.field = "*"
	}

	this.options = collection{}

	return options
}

func (this *Query) parseDistinct(distinct string) string {
	if distinct != "" {
		return " DISTINCT "
	} else {
		return ""
	}
}

func (this *Query) parseField(field string) string {
	return field
}

func (this *Query) parseJoin(join [][]string) string {
	if len(join) == 0 {
		return ""
	}

	joinString := strings.Builder{}
	for i := 0; i < len(join); i++ {
		if len(join[i]) != 3 {
			continue
		}

		joinString.WriteString(" ")
		joinString.WriteString(join[i][2])
		joinString.WriteString(" JOIN ")
		joinString.WriteString(join[i][0])
		joinString.WriteString(" ON ")
		joinString.WriteString(join[i][1])
	}

	return joinString.String()
}

func (this *Query) parseWhere(options collection) string {
	for i := 0; i < len(options.where); i++ {
		options.multi = append(options.multi, this.parseWhereItem(options.where[i]))
	}

	if len(options.multi) == 0 {
		return ""
	}

	return " WHERE " + strings.Join(options.multi, " AND ")
}

func (this *Query) parseWhereItem(args []interface{}) string {
	field := args[0].(string)
	exp := args[1].(string)

	if _, ok := this.expression[exp]; ok {
		exp = this.expression[exp]
	}

	if this.contains(exp, []string{"=", "<>", ">", ">=", "<", "<="}) {
		return field + " " + exp + " " + this.parseValue(args[2])
	} else if exp == "LIKE" || exp == "NOT LIKE" {
		return field + " " + exp + " " + this.parseValue(args[2])
	} else if this.contains(exp, []string{"NOT NULL", "NULL"}) {
		return field + " IS " + exp
	} else if this.contains(exp, []string{"NOT IN", "IN"}) {
		values := []string{}

		switch args[2].(type) {
		case []int:
			condition := args[2].([]int)
			for i := 0; i < len(condition); i++ {
				values = append(values, strconv.Itoa(condition[i]))
			}

			return field + " " + exp + "(" + strings.Join(values, ", ") + ")"
		case []string:
			condition := args[2].([]string)
			for i := 0; i < len(condition); i++ {
				values = append(values, "'" + condition[i] + "'")
			}
		}

		return field + " " + exp + "(" + strings.Join(values, ", ") + ")"
	} else if this.contains(exp, []string{"NOT BETWEEN", "BETWEEN"}) {
		values := []string{}

		switch args[2].(type) {
		case string:
			return field + " " + exp + " " + args[2].(string)
		case []int:
			condition := args[2].([]int)
			for i := 0; i < len(condition); i++ {
				values = append(values, strconv.Itoa(condition[i]))
			}

			return field + " " + exp + "(" + strings.Join(values, ", ") + ")"
		case []string:
			condition := args[2].([]string)
			for i := 0; i < len(condition); i++ {
				values = append(values, "'" + condition[i] + "'")
			}
		}

		return field + " " + exp + "(" + strings.Join(values, ", ") + ")"
	} else if this.contains(exp, []string{"NOT EXISTS", "EXISTS"}) {
		return field + " " + exp + " " + args[2].(string)
	}

	return ""
}

func (this *Query) parseGroup(group string) string {
	if group == "" {
		return ""
	}

	return " GROUP BY " + group
}

func (this *Query) parseHaving(having string) string {
	if having == "" {
		return ""
	}

	return " HAVING " + having
}

func (this *Query) parseOrder(order []string) string {
	if len(order) == 0 {
		return ""
	}

	for i := 0; i < len(order); i++ {
		if order[i] == "[rand]" {
			order[i] = "RAND()"
		} else {
			order[i] = strings.TrimSpace(order[i])
		}
	}

	return " ORDER BY " + strings.Join(order, ", ")
}

func (this *Query) parseLimit(limit string) string {
	if limit != "" {
		return " LIMIT " + limit
	} else {
		return ""
	}
}