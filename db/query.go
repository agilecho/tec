package db

import (
	"database/sql"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

func toInt64(value interface{}) int64 {
	switch value.(type) {
	case int:
		return int64(value.(int))
	case int8:
		return int64(value.(int8))
	case int16:
		return int64(value.(int16))
	case int32:
		return int64(value.(int32))
	case int64:
		return value.(int64)
	case uint:
		return int64(value.(uint))
	case uint8:
		return int64(value.(uint8))
	case uint16:
		return int64(value.(uint16))
	case uint32:
		return int64(value.(uint32))
	case uint64:
		return int64(value.(uint64))
	case float32:
		return int64(value.(float32))
	case float64:
		return int64(value.(float64))
	}

	return 0
}

func extractTagInfo(modelValue reflect.Value) map[string]reflect.Value {
	tagList := make(map[string]reflect.Value)

	directValue := reflect.Indirect(modelValue)
	if directValue.Kind() != reflect.Struct {
		return tagList
	}

	for i := 0; i < directValue.NumField(); i++ {
		value := directValue.Field(i)
		field := directValue.Type().Field(i)

		name := field.Name
		if tag, ok := field.Tag.Lookup("db"); ok {
			name = tag
		}

		tagList[name] = value
	}

	return tagList
}

func extractStructFun(model interface{}) func(rows *sql.Rows) {
	return func(rows *sql.Rows) {
		if rows == nil {
			return
		}

		defer rows.Close()

		modelType := reflect.TypeOf(model)
		if modelType.Kind() != reflect.Ptr {
			return
		}

		modelType = modelType.Elem()

		modelValue := reflect.ValueOf(model)
		modelValueInd := reflect.Indirect(modelValue)

		for rows.Next() {
			value := reflect.New(modelType)
			if modelType.Kind() == reflect.Slice {
				value = reflect.New(modelType.Elem())
			}

			tagList := extractTagInfo(value)

			fields, err := rows.Columns()
			if err != nil {
				return
			}

			refs := make([]interface{}, len(fields))
			for i, field := range fields {
				if f, ok := tagList[field]; ok {
					refs[i] = f.Addr().Interface()
				} else {
					refs[i] = new(interface{})
				}
			}

			rows.Scan(refs...)

			if modelType.Kind() == reflect.Slice {
				modelValueInd = reflect.Append(modelValueInd, value.Elem())
			} else {
				modelValue.Elem().Set(value.Elem())
				return
			}
		}

		modelValue.Elem().Set(modelValueInd)
	}
}

func extractRow(modelType reflect.Type, modelValue reflect.Value) Row {
	row := Row{}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)

		name := field.Name
		if tag, ok := field.Tag.Lookup("db"); ok {
			name = tag
		}

		value := modelValue.Field(i).Interface()
		if name == "id" && toInt64(value) <= 0 {
			continue
		}

		row[name] = value
	}

	return row
}

func extractRows(model interface{}) []Row {
	rows := []Row{}

	modelType := reflect.TypeOf(model)
	if modelType.Kind() != reflect.Ptr {
		return rows
	}

	modelType = modelType.Elem()
	modelValue := reflect.ValueOf(model).Elem()

	if modelType.Kind() == reflect.Slice {
		modelType = modelType.Elem()

		for i := 0; i < modelValue.Len(); i++ {
			item := modelValue.Index(i)
			rows = append(rows, extractRow(modelType, item))
		}

		return rows
	}

	rows = append(rows, extractRow(modelType, modelValue))

	return rows
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
	db *Db
	row Row
	table string
	alias string
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

	return this
}

func (this *Query) aggregate(aggregate string, field string) interface{} {
	if this.table == "" {
		return 0
	}

	this.Field(aggregate + "(" + field + ")").Limit(1)

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	return this.db.ResultFirst(tsql, this.bind...)
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

func (this *Query) First() Row {
	this.Limit(1)

	if this.table == "" {
		return nil
	}

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	return this.db.FetchFirst(tsql, this.bind...)
}

func (this *Query) Rows() []Row {
	if this.table == "" {
		return []Row{}
	}

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	return this.db.FetchRows(tsql, this.bind...)
}

func (this *Query) Insert(row Row) int64 {
	return this.db.Insert(this.table, row)
}

func (this *Query) InsertBatch(rows []Row) int64 {
	return this.db.InsertBatch(this.table, rows)
}

func (this *Query) Update(row Row) int64 {
	if this.table == "" {
		return 0
	}

	options := this.parseExpress()
	where := this.parseWhere(options)

	if where != "" {
		where = where[7:]
	}

	return this.db.Update(this.table, row, where)
}

func (this *Query) UpdateBatch(rows []Row) int64 {
	return 0
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

	return this.db.Delete(this.table, where)
}

func (this *Query) Count() int64 {
	if this.table == "" {
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

func (this *Query) Find(model interface{}) {
	this.Limit(1)

	if this.table == "" {
		return
	}

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	this.bind = append(this.bind, extractStructFun(model))

	this.db.FetchFirst(tsql, this.bind...)
}

func (this *Query) Search(models interface{}) {
	if this.table == "" {
		return
	}

	options := this.parseExpress()
	tsql := this.buildSelectSql(options)

	this.bind = append(this.bind, extractStructFun(models))

	this.db.FetchRows(tsql, this.bind...)
}

func (this *Query) Save(model interface{}) int64 {
	if this.table == "" || model == nil {
		return 0
	}

	rows := extractRows(model)
	if len(rows) == 1 {
		if val, ok := rows[0]["id"]; ok {
			return this.db.Update(this.table, rows[0], "id = ?", val)
		}

		return this.db.Insert(this.table, rows[0])
	}

	tx := this.db.Trans()

	for _, row := range rows {
		if val, ok := row["id"]; ok {
			tx.Update(this.table, row, "id = ?", val)
		} else {
			tx.Insert(this.table, row)
		}
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		this.db.logger("db.query.save error:" + err.Error())
		return 0
	}

	return 1
}