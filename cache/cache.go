package cache

import (
	"tec/cache/redis"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Host string
	Port string
	Passwd string
	Prefix string
	Pool int
	Active int
	Timeout int
	Logs string
	Debug bool
}

func (this *Config) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "host":
		this.Host = value
	case "port":
		this.Port = value
	case "passwd":
		this.Passwd = value
	case "prefix":
		this.Prefix = value
	case "pool":
		this.Pool, _ = strconv.Atoi(value)
	case "active":
		this.Active, _ = strconv.Atoi(value)
	case "timeout":
		this.Timeout, _ = strconv.Atoi(value)
	case "logs":
		this.Logs = value
	case "debug":
		this.Debug, _ = strconv.ParseBool(value)
	}
}

var CONFIG *Config
var pool *redis.Pool
var rwMu = &sync.RWMutex{}

// redis error log
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

	if config.Timeout < 1 {
		config.Timeout = 3600
	}

	pool = &redis.Pool {
		MaxIdle: config.Pool,
		MaxActive: config.Active,
		IdleTimeout: 30 * time.Second,
		MaxConnLifetime: time.Duration(config.Timeout) * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial(config.Host, config.Port, config.Passwd)
		},
		Wait:true,
	}

	CONFIG = config
}

func Close() {
	if pool != nil {
		pool.Close()
	}
}

func Do(command string, args ...interface{}) interface{} {
	conn := pool.Get()
	if conn == nil {
		return nil
	}

	defer conn.Close()

	args[0] = CONFIG.Prefix + args[0].(string)

	result, err := conn.Do(command, args...)
	if err != nil {
		logger("cache.Do error:" + err.Error())
		return nil
	}

	return result
}

func Has(key string) int {
	return redis.Int(Do("EXISTS", key))
}

func Get(key string) string {
	return redis.String(Do("GET", key))
}

func Append(key string, value string) int {
	return redis.Int(Do("APPEND", key, value))
}

func Set(key string, value string) bool {
	return redis.String(Do("SET", key, value)) == "OK"
}

func SetEx(key string, value string, timeout int) bool {
	return redis.String(Do("SETEX", key, timeout, value)) == "OK"
}

func SetNx(key string, value string) int {
	return redis.Int(Do("SETNX", key, value))
}

func Del (key string) int {
	return redis.Int(Do("DEL", key))
}

func Keys(key string) []string {
	return redis.Strings(Do("KEYS", key))
}

func Expire(key string, timeout int) int {
	return redis.Int(Do("EXPIRE", key, timeout))
}

func StrLen(key string) int {
	return redis.Int(Do("STRLEN", key))
}

func ReName(key string, name string) bool {
	return redis.String(Do("RENAME", key, name)) == "OK"
}

func Incr(key string) int {
	return redis.Int(Do("INCR", key))
}

func IncrBy(key string, num int) int {
	return redis.Int(Do("INCRBY", key, num))
}

func Decr(key string) int {
	return redis.Int(Do("DECR", key))
}

func DecrBy(key string, num int) int {
	return redis.Int(Do("DECRBY", key, num))
}

func HIncrBy(hash string, key string, num int) int {
	return redis.Int(Do("HINCRBY", hash, key, num))
}

func LIndex(key string, index int) string {
	return redis.String(Do("LINDEX", key, index))
}

func LLen(key string) int {
	return redis.Int(Do("LLEN", key))
}

func LPop(key string) string {
	return redis.String(Do("LPOP", key))
}

func LTrim(key string, start int, stop int) bool {
	return redis.String(Do("LTRIM", key, start, stop)) == "OK"
}

func LRange(key string, start int, end int) []string {
	return redis.Strings(Do("LRANGE", key, start, end))
}

func LPush(key string, value string) int {
	return redis.Int(Do("LPUSH", key, value))
}

func LPushX(key string, value string) int {
	return redis.Int(Do("LPUSHX", key, value))
}

func RPush(key string, value string) int {
	return redis.Int(Do("RPUSH", key, value))
}

func RPushX(key string, value string) int {
	return redis.Int(Do("RPUSHX", key, value))
}

func RPop(key string) string {
	return redis.String(Do("RPOP", key))
}

func HLen(hash string) int {
	return redis.Int(Do("HLEN", hash))
}

func HDel(hash string, key string) int {
	return redis.Int(Do("HDEL", hash, key))
}

func HExists(hash string, key string) int {
	return redis.Int(Do("HEXISTS", hash, key))
}

func HGet(hash string, key string) string {
	return redis.String(Do("HGET", hash, key))
}

func HKeys(hash string) []string {
	return redis.Strings(Do("HKEYS", hash))
}

func HSet(hash string, key string, value string) int {
	return redis.Int(Do("HSET", hash, key, value))
}

func HSetNx(hash string, key string, value string) int {
	return redis.Int(Do("HSETNX", hash, key, value))
}

func SCard(set string) int {
	return redis.Int(Do("SCARD", set))
}

func SMembers(set string) []string {
	return redis.Strings(Do("SMEMBERS", set))
}

func SAdd(set string, value string) int {
	return redis.Int(Do("SADD", set, value))
}

func SRem(set string, value string) int {
	return redis.Int(Do("SREM", set, value))
}

func SPop(set string) string {
	return redis.String(Do("SPOP", set))
}

func Type(key string) string {
	return redis.String(Do("TYPE", key))
}

func TTL(key string) int {
	return redis.Int(Do("TTL", key))
}