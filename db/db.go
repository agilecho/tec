package db

import (
	"tec/db/mysql"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Host string
	Port string
	User string
	Passwd string
	Database string
	Charset string
	Prefix string
	Pool int
	Active int
	Timeout int
	Deploy bool
	Slave string
	Debug bool
	Logs string
}

func (this *Config) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "host":
		this.Host = value
	case "port":
		this.Port = value
	case "user":
		this.User = value
	case "passwd":
		this.Passwd = value
	case "database":
		this.Database = value
	case "charset":
		this.Charset = value
	case "prefix":
		this.Prefix = value
	case "pool":
		this.Pool, _ = strconv.Atoi(value)
	case "active":
		this.Active, _ = strconv.Atoi(value)
	case "timeout":
		this.Timeout, _ = strconv.Atoi(value)
	case "deploy":
		this.Deploy, _ = strconv.ParseBool(value)
	case "slave":
		this.Slave = value
	case "logs":
		this.Logs = value
	case "debug":
		this.Debug, _ = strconv.ParseBool(value)
	}
}

type argWrapper struct {
	Args []interface{}
	Callback func(rows *sql.Rows)
}

func (this *argWrapper) Parse() *argWrapper {
	tmp := []interface{}{}
	for i := 0; i < len(this.Args); i++ {
		switch this.Args[i].(type) {
		case func(rows *sql.Rows):
			this.Callback = this.Args[i].(func(rows *sql.Rows))
		default:
			tmp = append(tmp, this.Args[i])
		}
	}

	this.Args = tmp

	return this
}

type Row map[string]interface{}

func (this Row) Float64(key string) float64 {
	val, _ := strconv.ParseFloat((this)[key].(string), 64)
	return val
}

type TxWrapper struct {
	handler *sql.Tx
}

func (this *TxWrapper) Rollback() error {
	return this.handler.Rollback()
}

func (this *TxWrapper) Commit() error {
	return this.handler.Commit()
}

func (this *TxWrapper) Insert(table string, row Row) int64 {
	tsql, args := buildInsertTsql(table, row)
	result := this.Execute(tsql, args...)
	if result == nil {
		return -1
	}

	insertid, err := result.LastInsertId()
	if err != nil {
		logger("db.TxWrapper.Insert.LastInsertId error:" + err.Error())
		return -1
	}

	return insertid
}

func (this *TxWrapper) Update(table string, row Row, condition string, args ...interface{}) int64 {
	tsql, argsNew := buildUpdateTsql(table, row, condition, args...)
	result := this.Execute(tsql, argsNew...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		logger("db.TxWrapper.Update.RowsAffected error:" + err.Error())
		return -1
	}

	return affected
}

func (this *TxWrapper) Delete(table string, condition string, args ...interface{}) int64 {
	result := this.Execute(buildDeleteTsql(table, condition), args...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return -1
	}

	return affected
}

func (this *TxWrapper) Execute(tsql string, args ...interface{}) sql.Result {
	if CONFIG != nil && CONFIG.Debug {
		result, _ := json.Marshal(args)
		logger("db.TxWrapper.Execute tsql:" + tsql + " arg:" + string(result))
	}

	result, err := this.handler.Exec(tsql, args...)
	if err != nil {
		logger("db.TxWrapper.Execute.Exec error:" + err.Error())
		return nil
	}

	return result
}

type linkWrapper struct {
	handler *sql.DB
	lifetime int64
}

var CONFIG *Config
var linkMaster *linkWrapper
var linkSlaves []*linkWrapper
var rwMu = &sync.RWMutex{}

// mysql error log
func logger(message string) {
	if CONFIG == nil {
		return
	}

	now := time.Now()

	var text = strings.Builder{}

	text.WriteString(now.Format("2006-01-02 15:04:05"))
	text.WriteString("(")
	text.WriteString(strconv.FormatFloat(float64(now.UnixNano() / 1e6) * 0.001, 'f', 4, 64))
	text.WriteString(") ")
	text.WriteString(message)
	text.WriteString("\r\n")

	if CONFIG.Debug  {
		fmt.Print(text.String())
	}
	
	go func(data string, config *Config) {
		rwMu.Lock()
		defer rwMu.Unlock()

		err := os.MkdirAll(config.Logs + "/" + time.Now().Format("20060102"), os.ModePerm)
		if err != nil {
			return
		}

		file, _ := os.OpenFile(config.Logs + "/" + time.Now().Format("20060102") + "/" + strconv.Itoa(time.Now().Hour()) + ".txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer file.Close()

		_, err = file.WriteString(data)
	}(text.String(), CONFIG)
}

func Init(config *Config) {
	if config.Pool < 5 {
		config.Pool = 5
	}

	if config.Active < 1 {
		config.Active = 1
	}

	sql.Register("mysql", &mysql.MySQLDriver{})

	linkMaster = buildLink(config)
	if config.Deploy {
		slaves := strings.Split(config.Slave, ",")
		for _, slave := range slaves {
			conf := &Config {
				Host: slave,
				Port: config.Port,
				User: config.User,
				Passwd: config.Passwd,
				Database: config.Database,
				Charset: config.Charset,
				Pool: config.Pool,
				Active: config.Active,
				Timeout: config.Timeout,
			}

			linkSlaves = append(linkSlaves, buildLink(conf))
		}
	}

	CONFIG = config
}

func Ping() {
	if linkMaster != nil {
		if linkMaster.lifetime < time.Now().Unix() - 7200 {
			linkMaster.handler.Query("SELECT 1")
			linkMaster.lifetime = time.Now().Unix()
		}
	}

	for _, link := range linkSlaves {
		if link.lifetime < time.Now().Unix() - 7200 {
			link.handler.Query("SELECT 1")
			link.lifetime = time.Now().Unix()
		}
	}
}

func Close() {
	if linkMaster != nil {
		linkMaster.handler.Close()
	}

	for _, link := range linkSlaves {
		link.handler.Close()
	}
}

func buildLink(config *Config) *linkWrapper {
	var builder = strings.Builder{}

	builder.WriteString(config.User)
	builder.WriteString(":")
	builder.WriteString(config.Passwd)
	builder.WriteString("@tcp(")
	builder.WriteString(config.Host)
	builder.WriteString(":")
	builder.WriteString(config.Port)
	builder.WriteString(")/")
	builder.WriteString(config.Database)
	builder.WriteString("?charset=")
	builder.WriteString(config.Charset)

	link, _ := sql.Open("mysql", builder.String())

	if config.Pool < 5 {
		config.Pool = 5
	}

	if config.Active < 1 {
		config.Active = 1
	}

	if config.Timeout < 1 {
		config.Timeout = 3600
	}

	link.SetMaxOpenConns(config.Pool)
	link.SetMaxIdleConns(config.Active)

	link.SetConnMaxLifetime(time.Duration(config.Timeout) * time.Second)

	return &linkWrapper{handler:link, lifetime:time.Now().Unix()}
}

func buildLinkOfMaster() *linkWrapper {
	linkMaster.lifetime = time.Now().Unix()
	return linkMaster
}

func buildLinkOfSlave() *linkWrapper {
	if len(linkSlaves) == 0 {
		return buildLinkOfMaster()
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	link := linkSlaves[random.Intn(len(linkSlaves) - 1)]
	link.lifetime = time.Now().Unix()

	return link;
}

func buildInsertTsql(table string, row Row) (string, []interface{}) {
	var tsql = strings.Builder{}

	tsql.WriteString("INSERT INTO ")
	tsql.WriteString(table)
	tsql.WriteString("(")

	args := make([]interface{}, len(row))

	var num = 0
	for key, value := range row {
		tsql.WriteString("`")
		tsql.WriteString(key)
		tsql.WriteString("`")
		if num < len(row) - 1 {
			tsql.WriteString(",")
		}

		args[num] = value
		num++
	}

	tsql.WriteString(") VALUES (")

	for i := 1; i <= len(row); i++ {
		num++
		tsql.WriteString("?")
		if i < len(row) {
			tsql.WriteString(",")
		}
	}

	tsql.WriteString(")")

	return tsql.String(), args
}

func buildUpdateTsql(table string, row Row, condition string, args ...interface{}) (string, []interface{}) {
	var tsql = strings.Builder{}

	tsql.WriteString("UPDATE ")
	tsql.WriteString(table)
	tsql.WriteString(" SET ")

	argsNew := make([]interface{}, len(row))

	var num = 0
	for key, value := range row {
		tsql.WriteString("`")
		tsql.WriteString(key)
		tsql.WriteString("` = ?")
		if num < len(row) - 1 {
			tsql.WriteString(",")
		}

		argsNew[num] = value
		num++
	}

	if condition == "" {
		tsql.WriteString(" WHERE 1")
	} else {
		tsql.WriteString(" WHERE ")
		tsql.WriteString(condition)
	}

	for i := 0; i < len(args); i++ {
		argsNew = append(argsNew, args[i])
	}

	return tsql.String(), argsNew
}

func buildDeleteTsql(table string, condition string) string {
	var tsql = strings.Builder{}

	tsql.WriteString("DELETE FROM ")
	tsql.WriteString(table)

	if condition == "" {
		tsql.WriteString(" WHERE 1")
	} else {
		tsql.WriteString(" WHERE ")
		tsql.WriteString(condition)
	}

	return tsql.String()
}

func buildLimitTsql(tsql string) string {
	if !(strings.Contains(tsql, "LIMIT") || strings.Contains(tsql, "limit") || strings.Contains(tsql, "Limit")) {
		tsql += " LIMIT 0, 1"
	}

	return tsql
}

func buildItem(column *sql.ColumnType, col interface{}) interface{} {
	stype := column.ScanType().String()
	if col == nil {
		switch stype {
		case "sql.RawBytes":
			return ""
		case "float32", "float64", "sql.NullFloat64":
			return float64(0)
		default:
			return int64(0)
		}
	}

	ctype := reflect.TypeOf(col).String()
	switch ctype {
	case "int":
		return int64(col.(int))
	case "int8":
		return int64(col.(int8))
	case "int32":
		return int64(col.(int32))
	case "uint":
		return int64(col.(uint))
	case "uint8":
		return int64(col.(uint8))
	case "uint32":
		return int64(col.(uint32))
	case "uint64":
		return int64(col.(uint64))
	case "int64":
		return col
	case "float32":
		return float64(col.(float32))
	case "float64":
		return col
	}

	var result interface{}
	value := string(col.([]byte))
	switch stype {
	case "int8", "int32", "uint8", "uint32", "int64", "uint64", "sql.NullInt64":
		result, _ = strconv.ParseInt(value, 10, 64)
	case "float32", "float64", "sql.NullFloat64":
		result, _ = strconv.ParseFloat(value, 64)
	case "sql.RawBytes":
		result = value
	}

	return result
}

func buildRow(rows *sql.Rows, columns []*sql.ColumnType) Row {
	scans := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scans[i] = &values[i]
	}

	err := rows.Scan(scans...)
	if err != nil {
		logger("db.buildRow.Scan error:" + err.Error())
		return nil
	}

	result := Row{}
	for i, col := range values {
		key := columns[i].Name()
		result[key] = buildItem(columns[i], col)
	}

	return result
}

func query(tsql string, args ...interface{}) (*sql.Rows, error) {
	if CONFIG != nil && CONFIG.Debug {
		result, _ := json.Marshal(args)
		logger("db.query tsql:" + tsql + " arg:" + string(result))
	}

	db := buildLinkOfSlave()

	if len(args) == 0 {
		return db.handler.Query(tsql)
	} else {
		var stmt *sql.Stmt

		stmt, err := db.handler.Prepare(tsql)
		if err != nil {
			return nil, err
		}

		defer stmt.Close()

		return stmt.Query(args...)
	}
}

func FetchRows(tsql string, args ...interface{}) []Row {
	scope := (&argWrapper{Args:args}).Parse()

	rows, err := query(tsql, scope.Args...)
	if err != nil {
		logger("db.FetchRows.query error:" + err.Error())
		return []Row{}
	}

	defer rows.Close()
	if scope.Callback != nil {
		scope.Callback(rows)
		return []Row{}
	}

	result := make([]Row, 0, 2)

	columns, err := rows.ColumnTypes()
	if err != nil {
		logger("db.FetchRows.ColumnTypes error:" + err.Error())
		return []Row{}
	}

	for rows.Next() {
		result = append(result, buildRow(rows, columns))
	}

	return result
}

func FetchFirst(tsql string, args ...interface{}) Row {
	tsql = buildLimitTsql(tsql)
	scope := (&argWrapper{Args:args}).Parse()

	rows, err := query(tsql, scope.Args...)
	if err != nil {
		logger("db.FetchFirst error:" + err.Error())
		return nil
	}

	defer rows.Close()
	if scope.Callback != nil {
		scope.Callback(rows)
		return nil
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		logger("db.FetchFirst.ColumnTypes error:" + err.Error())
		return nil
	}

	if rows.Next() {
		return buildRow(rows, columns)
	}

	return nil
}

func ResultFirst(tsql string, args ...interface{}) interface{} {
	scope := (&argWrapper{Args:args}).Parse()

	rows, err := query(tsql, scope.Args...)
	if err != nil {
		logger("db.ResultFirst.query error:" + err.Error())
		return nil
	}

	defer rows.Close()
	if scope.Callback != nil {
		scope.Callback(rows)
		return nil
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		logger("db.ResultFirst.ColumnTypes error:" + err.Error())
		return nil
	}

	var result interface{}
	if rows.Next() {
		rows.Scan(&result)
		return buildItem(columns[0], result)
	}

	return nil
}

func Insert(table string, row Row) int64 {
	tsql, args := buildInsertTsql(table, row)

	result := Execute(tsql, args...)
	if result == nil {
		return -1
	}

	insertid, err := result.LastInsertId()
	if err != nil {
		logger("db.Insert.LastInsertId error:" + err.Error())
		return -1
	}

	return insertid
}

func Update(table string, row Row, condition string, args ...interface{}) int64 {
	tsql, argsNew := buildUpdateTsql(table, row, condition, args...)
	result := Execute(tsql, argsNew...)
	if result == nil {
		return -1
	}
	
	affected, err := result.RowsAffected()
	if err != nil {
		logger("db.Update.RowsAffected error:" + err.Error())
		return -1
	}

	return affected
}

func Delete(table string, condition string, args ...interface{}) int64 {
	result := Execute(buildDeleteTsql(table, condition), args...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		logger("db.Delete.RowsAffected error:" + err.Error())
		return -1
	}

	return affected
}

func Execute(tsql string, args ...interface{}) sql.Result {
	if CONFIG != nil && CONFIG.Debug {
		result, _ := json.Marshal(args)
		logger("db.Execute tsql:" + tsql + " arg:" + string(result))
	}

	var result sql.Result
	var err error

	link := buildLinkOfMaster()
	if len(args) == 0 {
		result, err = link.handler.Exec(tsql)
	} else {
		var stmt *sql.Stmt

		stmt, err = link.handler.Prepare(tsql)
		defer stmt.Close()
		if err != nil {
			logger("db.Execute Prepare error:" + err.Error())
			return nil
		}

		result, err = stmt.Exec(args...)
	}

	if err != nil {
		logger("db.Execute Exec error:" + err.Error())
		return nil
	}

	return result
}

func Trans() *TxWrapper {
	link := buildLinkOfMaster()
	tx, err := link.handler.Begin()

	if err != nil {
		logger("db.Trans Begin error:" + err.Error())
		return nil
	}

	wrapper := &TxWrapper{
		handler:tx,
	}

	return wrapper
}

func Table(table string) *Query {
	return Model(table)
}

func Model(value interface{}) *Query {
	query := &Query{}
	switch value.(type) {
	case struct{}:
		query.value = value
	case string:
		query.table = value.(string)
	}

	return query.init()
}