package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/agilecho/tec/db/mysql"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

type linkWrapper struct {
	handler *sql.DB
	lifetime int64
}

type Row map[string]interface{}

func (this Row) Float64(key string) float64 {
	val, _ := strconv.ParseFloat((this)[key].(string), 64)
	return val
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

func buildInsertBatchTsql(table string, rows []Row) (string, []interface{}) {
	var tsql = strings.Builder{}

	tsql.WriteString("INSERT INTO ")
	tsql.WriteString(table)
	tsql.WriteString("(")

	args := []interface{}{}
	columns := []string{}
	var num = 0
	for key, _ := range rows[0] {
		columns = append(columns, key)

		tsql.WriteString("`")
		tsql.WriteString(key)
		tsql.WriteString("`")

		if num < len(rows[0]) - 1 {
			tsql.WriteString(",")
		}

		num++
	}

	tsql.WriteString(") VALUES")

	for i, row := range rows {
		tsql.WriteString("(")

		for j, column := range columns {
			tsql.WriteString("?")

			if j < len(columns) - 1 {
				tsql.WriteString(",")
			}

			args = append(args, row[column])
		}

		tsql.WriteString(")")

		if i < len(rows) - 1 {
			tsql.WriteString(",")
		}
	}

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

type TxWrapper struct {
	db *Db
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
		this.db.logger("db.TxWrapper.Insert.LastInsertId error:" + err.Error())
		return -1
	}

	return insertid
}

func (this *TxWrapper) InsertBatch(table string, rows []Row) int64 {
	if len(rows) == 0 {
		return -1
	}

	tsql, args := buildInsertBatchTsql(table, rows)
	result := this.Execute(tsql, args...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		this.db.logger("db.TxWrapper.Insert.RowsAffected error:" + err.Error())
		return -1
	}

	return affected
}

func (this *TxWrapper) Update(table string, row Row, condition string, args ...interface{}) int64 {
	tsql, argsNew := buildUpdateTsql(table, row, condition, args...)
	result := this.Execute(tsql, argsNew...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		this.db.logger("db.TxWrapper.Update.RowsAffected error:" + err.Error())
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
	if this.db.config != nil && this.db.config.Debug {
		result, _ := json.Marshal(args)
		this.db.logger("db.TxWrapper.Execute tsql:" + tsql + " arg:" + string(result))
	}

	result, err := this.handler.Exec(tsql, args...)
	if err != nil {
		this.db.logger("db.TxWrapper.Execute.Exec error:" + err.Error())
		return nil
	}

	return result
}

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

type Db struct {
	config *Config
	linkMaster *linkWrapper
	linkSlaves []*linkWrapper
	mu sync.RWMutex
}

func (this *Db) microtime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano() / 1e6) * 0.001, 'f', 4, 64)
}

func (this *Db) logger(message string) {
	if this.config == nil {
		return
	}

	now := time.Now()

	var text = strings.Builder{}
	text.WriteString(fmt.Sprintf("%v%v%v%v%v%v", now.Format("2006-01-02 15:04:05"), "(", this.microtime(), ") ", message, "\r\n"))

	if this.config.Debug {
		fmt.Print(text.String())
	}

	go func(data string, that *Db) {
		that.mu.Lock()
		defer that.mu.Unlock()

		err := os.MkdirAll(that.config.Logs + "/" + time.Now().Format("200601"), os.ModePerm)
		if err != nil {
			return
		}

		file, _ := os.OpenFile(that.config.Logs + "/" + time.Now().Format("200601") + "/" + time.Now().Format("2006010215") + ".txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer file.Close()

		file.WriteString(data)
	}(text.String(), this)
}

func (this *Db) buildLink(config *Config) *linkWrapper {
	link, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=%v", config.User, config.Passwd, config.Host, config.Port, config.Database, config.Charset))
	if err != nil {
		this.logger("db.open error:" + err.Error())
		return nil
	}

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

func (this *Db) buildLinkOfMaster() *linkWrapper {
	if this.linkMaster == nil {
		return nil
	}

	this.linkMaster.lifetime = time.Now().Unix()
	return this.linkMaster
}

func (this *Db) buildLinkOfSlave() *linkWrapper {
	if len(this.linkSlaves) == 0 {
		return this.buildLinkOfMaster()
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	link := this.linkSlaves[random.Intn(len(this.linkSlaves) - 1)]
	link.lifetime = time.Now().Unix()

	return link
}

func (this *Db) buildRow(rows *sql.Rows, columns []*sql.ColumnType) Row {
	scans := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scans[i] = &values[i]
	}

	err := rows.Scan(scans...)
	if err != nil {
		this.logger("db.buildRow.Scan error:" + err.Error())
		return nil
	}

	result := Row{}
	for i, col := range values {
		key := columns[i].Name()
		result[key] = buildItem(columns[i], col)
	}

	return result
}

func (this *Db) query(tsql string, args ...interface{}) (*sql.Rows, error) {
	if this.config != nil && this.config.Debug {
		result, _ := json.Marshal(args)
		this.logger("db.query tsql:" + tsql + " arg:" + string(result))
	}

	link := this.buildLinkOfSlave()
	if link == nil {
		return nil, nil
	}

	if len(args) == 0 {
		return link.handler.Query(tsql)
	} else {
		var stmt *sql.Stmt

		stmt, err := link.handler.Prepare(tsql)
		if err != nil {
			return nil, err
		}

		defer stmt.Close()

		return stmt.Query(args...)
	}
}

func (this *Db) FetchRows(tsql string, args ...interface{}) []Row {
	scope := (&argWrapper{Args:args}).Parse()

	rows, err := this.query(tsql, scope.Args...)
	if err != nil {
		this.logger("db.FetchRows.query error:" + err.Error())
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
		this.logger("db.FetchRows.ColumnTypes error:" + err.Error())
		return []Row{}
	}

	for rows.Next() {
		result = append(result, this.buildRow(rows, columns))
	}

	return result
}

func (this *Db) FetchFirst(tsql string, args ...interface{}) Row {
	tsql = buildLimitTsql(tsql)
	scope := (&argWrapper{Args:args}).Parse()

	rows, err := this.query(tsql, scope.Args...)
	if err != nil {
		this.logger("db.FetchFirst error:" + err.Error())
		return nil
	}

	defer rows.Close()
	if scope.Callback != nil {
		scope.Callback(rows)
		return nil
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		this.logger("db.FetchFirst.ColumnTypes error:" + err.Error())
		return nil
	}

	if rows.Next() {
		return this.buildRow(rows, columns)
	}

	return nil
}

func (this *Db) ResultFirst(tsql string, args ...interface{}) interface{} {
	scope := (&argWrapper{Args:args}).Parse()

	rows, err := this.query(tsql, scope.Args...)
	if err != nil {
		this.logger("db.ResultFirst.query error:" + err.Error())
		return nil
	}

	defer rows.Close()
	if scope.Callback != nil {
		scope.Callback(rows)
		return nil
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		this.logger("db.ResultFirst.ColumnTypes error:" + err.Error())
		return nil
	}

	var result interface{}
	if rows.Next() {
		rows.Scan(&result)
		return buildItem(columns[0], result)
	}

	return nil
}

func (this *Db) Insert(table string, row Row) int64 {
	tsql, args := buildInsertTsql(table, row)

	result := this.Execute(tsql, args...)
	if result == nil {
		return -1
	}

	insertid, err := result.LastInsertId()
	if err != nil {
		this.logger("db.Insert.LastInsertId error:" + err.Error())
		return -1
	}

	return insertid
}

func (this *Db) InsertBatch(table string, rows []Row) int64 {
	if len(rows) == 0 {
		return -1
	}

	tsql, args := buildInsertBatchTsql(table, rows)

	result := this.Execute(tsql, args...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		this.logger("db.InsertBatch.RowsAffected error:" + err.Error())
		return -1
	}

	return affected
}

func (this *Db) Update(table string, row Row, condition string, args ...interface{}) int64 {
	tsql, argsNew := buildUpdateTsql(table, row, condition, args...)
	result := this.Execute(tsql, argsNew...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		this.logger("db.Update.RowsAffected error:" + err.Error())
		return -1
	}

	return affected
}

func (this *Db) Delete(table string, condition string, args ...interface{}) int64 {
	result := this.Execute(buildDeleteTsql(table, condition), args...)
	if result == nil {
		return -1
	}

	affected, err := result.RowsAffected()
	if err != nil {
		this.logger("db.Delete.RowsAffected error:" + err.Error())
		return -1
	}

	return affected
}

func (this *Db) Execute(tsql string, args ...interface{}) sql.Result {
	if this.config != nil && this.config.Debug {
		result, _ := json.Marshal(args)
		this.logger("db.Execute tsql:" + tsql + " arg:" + string(result))
	}

	var result sql.Result
	var err error

	link := this.buildLinkOfMaster()
	if link == nil {
		return nil
	}

	if len(args) == 0 {
		result, err = link.handler.Exec(tsql)
	} else {
		var stmt *sql.Stmt

		stmt, err = link.handler.Prepare(tsql)
		defer stmt.Close()
		if err != nil {
			this.logger("db.Execute Prepare error:" + err.Error())
			return nil
		}

		result, err = stmt.Exec(args...)
	}

	if err != nil {
		this. logger("db.Execute Exec error:" + err.Error())
		return nil
	}

	return result
}

func (this *Db) Trans() *TxWrapper {
	link := this.buildLinkOfMaster()
	tx, err := link.handler.Begin()

	if err != nil {
		this.logger("db.Trans Begin error:" + err.Error())
		return nil
	}

	return &TxWrapper{
		db: this,
		handler:tx,
	}
}

func (this *Db) Ping() {
	if this.linkMaster != nil {
		if this.linkMaster.lifetime < time.Now().Unix() - 7200 {
			this.linkMaster.handler.Query("SELECT 1")
			this.linkMaster.lifetime = time.Now().Unix()
		}
	}

	for _, link := range this.linkSlaves {
		if link.lifetime < time.Now().Unix() - 7200 {
			link.handler.Query("SELECT 1")
			link.lifetime = time.Now().Unix()
		}
	}
}

func (this *Db) Close() {
	if this.linkMaster != nil {
		this.linkMaster.handler.Close()
	}

	for _, link := range this.linkSlaves {
		link.handler.Close()
	}
}

func init() {
	sql.Register("mysql", &mysql.MySQLDriver{})
}

func New(config *Config) *Db {
	if config.Pool < 5 {
		config.Pool = 5
	}

	if config.Active < 1 {
		config.Active = 1
	}

	tmp := &Db{config: config, linkSlaves: []*linkWrapper{}}

	tmp.linkMaster = tmp.buildLink(config)

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

			tmp.linkSlaves = append(tmp.linkSlaves, tmp.buildLink(conf))
		}
	}

	return tmp
}

var handler *Db

func Init(config *Config) {
	handler = New(config)
}

func FetchRows(tsql string, args ...interface{}) []Row {
	return handler.FetchRows(tsql, args...)
}

func FetchFirst(tsql string, args ...interface{}) Row {
	return handler.FetchFirst(tsql, args...)
}

func ResultFirst(tsql string, args ...interface{}) interface{} {
	return handler.ResultFirst(tsql, args...)
}

func Insert(table string, row Row) int64 {
	return handler.Insert(table, row)
}

func InsertBatch(table string, rows []Row) int64 {
	return handler.InsertBatch(table, rows)
}

func Update(table string, row Row, condition string, args ...interface{}) int64 {
	return handler.Update(table, row, condition, args...)
}

func Delete(table string, condition string, args ...interface{}) int64 {
	return handler.Delete(table, condition, args...)
}

func Execute(tsql string, args ...interface{}) sql.Result {
	return handler.Execute(tsql, args...)
}

func Trans() *TxWrapper {
	return handler.Trans()
}

func Ping() {
	handler.Ping()
}

func Close() {
	handler.Close()
}

func Table(table string) *Query {
	query := &Query{db:handler}
	query.table = table
	return query.init()
}