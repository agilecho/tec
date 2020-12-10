package cache

import (
	"fmt"
	"github.com/agilecho/tec/cache/redis"
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

type Cache struct {
	config *Config
	pool *redis.Pool
	mut sync.RWMutex
}

func (this *Cache) microtime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano() / 1e6) * 0.001, 'f', 4, 64)
}

func (this *Cache) logger(message string) {
	if this.config == nil {
		return
	}

	now := time.Now()

	var text = strings.Builder{}
	text.WriteString(fmt.Sprintf("%v%v%v%v%v%v", now.Format("2006-01-02 15:04:05"), "(", this.microtime(), ") ", message, "\r\n"))

	if this.config.Debug  {
		fmt.Print(text.String())
	}

	go func(data string, that *Cache) {
		that.mut.Lock()
		defer that.mut.Unlock()

		err := os.MkdirAll(that.config.Logs + "/" + time.Now().Format("200601"), os.ModePerm)
		if err != nil {
			return
		}

		file, _ := os.OpenFile(that.config.Logs + "/" + time.Now().Format("200601") + "/" + time.Now().Format("2006010215") + ".txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer file.Close()

		file.WriteString(data)
	}(text.String(), this)
}

func (this *Cache) Close() {
	if this.pool != nil {
		this.pool.Close()
	}
}

func (this *Cache) Do(command string, args ...interface{}) (interface{}, error) {
	if this.pool == nil {
		return nil, nil
	}

	conn := this.pool.Get()
	if conn == nil {
		return nil, nil
	}

	defer conn.Close()

	if command == "MGET" {
		for i, val := range args {
			args[i] = this.config.Prefix + val.(string)
		}
	} else {
		args[0] = this.config.Prefix + args[0].(string)
	}

	result, err := conn.Do(command, args...)
	if err != nil {
		this.logger("cache.Do error:" + err.Error())
		return nil, err
	}

	return result, nil
}

func (this *Cache) Has(key string) int {
	return redis.Int(this.Do("EXISTS", key))
}

func (this *Cache) Get(key string) string {
	return redis.String(this.Do("GET", key))
}

func (this *Cache) MGet(keys ...interface{}) []interface{} {
	return redis.Values(this.Do("MGET", keys...))
}

func (this *Cache) Append(key string, value string) int {
	return redis.Int(this.Do("APPEND", key, value))
}

func (this *Cache) Set(key string, value string) bool {
	return redis.String(this.Do("SET", key, value)) == "OK"
}

func (this *Cache) SetEx(key string, value string, timeout int) bool {
	return redis.String(this.Do("SETEX", key, timeout, value)) == "OK"
}

func (this *Cache) SetNx(key string, value string) int {
	return redis.Int(this.Do("SETNX", key, value))
}

func (this *Cache) Del(key string) int {
	return redis.Int(this.Do("DEL", key))
}

func (this *Cache) Keys(key string) []string {
	keys := redis.Strings(this.Do("KEYS", key))
	if keys == nil || len(keys) == 0 {
		return keys
	}

	tmps := []string{}
	for _, key := range keys {
		tmps = append(tmps, strings.TrimPrefix(key, this.config.Prefix))
	}

	return tmps
}

func (this *Cache) Expire(key string, timeout int) int {
	return redis.Int(this.Do("EXPIRE", key, timeout))
}

func (this *Cache) StrLen(key string) int {
	return redis.Int(this.Do("STRLEN", key))
}

func (this *Cache) ReName(key string, name string) bool {
	return redis.String(this.Do("RENAME", key, name)) == "OK"
}

func (this *Cache) Incr(key string) int {
	return redis.Int(this.Do("INCR", key))
}

func (this *Cache) IncrBy(key string, num int) int {
	return redis.Int(this.Do("INCRBY", key, num))
}

func (this *Cache) Decr(key string) int {
	return redis.Int(this.Do("DECR", key))
}

func (this *Cache) DecrBy(key string, num int) int {
	return redis.Int(this.Do("DECRBY", key, num))
}

func (this *Cache) HIncrBy(hash string, key string, num int) int {
	return redis.Int(this.Do("HINCRBY", hash, key, num))
}

func (this *Cache) LIndex(key string, index int) string {
	return redis.String(this.Do("LINDEX", key, index))
}

func (this *Cache) LLen(key string) int {
	return redis.Int(this.Do("LLEN", key))
}

func (this *Cache) LPop(key string) string {
	return redis.String(this.Do("LPOP", key))
}

func (this *Cache) LTrim(key string, start int, stop int) bool {
	return redis.String(this.Do("LTRIM", key, start, stop)) == "OK"
}

func (this *Cache) LRange(key string, start int, end int) []string {
	return redis.Strings(this.Do("LRANGE", key, start, end))
}

func (this *Cache) LPush(key string, value string) int {
	return redis.Int(this.Do("LPUSH", key, value))
}

func (this *Cache) LPushX(key string, value string) int {
	return redis.Int(this.Do("LPUSHX", key, value))
}

func (this *Cache) RPush(key string, value string) int {
	return redis.Int(this.Do("RPUSH", key, value))
}

func (this *Cache) RPushX(key string, value string) int {
	return redis.Int(this.Do("RPUSHX", key, value))
}

func (this *Cache) RPop(key string) string {
	return redis.String(this.Do("RPOP", key))
}

func (this *Cache) HLen(hash string) int {
	return redis.Int(this.Do("HLEN", hash))
}

func (this *Cache) HDel(hash string, key string) int {
	return redis.Int(this.Do("HDEL", hash, key))
}

func (this *Cache) HExists(hash string, key string) int {
	return redis.Int(this.Do("HEXISTS", hash, key))
}

func (this *Cache) HGet(hash string, key string) string {
	return redis.String(this.Do("HGET", hash, key))
}

func (this *Cache) HKeys(hash string) []string {
	return redis.Strings(this.Do("HKEYS", hash))
}

func (this *Cache) HSet(hash string, key string, value string) int {
	return redis.Int(this.Do("HSET", hash, key, value))
}

func (this *Cache) HSetNx(hash string, key string, value string) int {
	return redis.Int(this.Do("HSETNX", hash, key, value))
}

func (this *Cache) SCard(set string) int {
	return redis.Int(this.Do("SCARD", set))
}

func (this *Cache) SMembers(set string) []string {
	return redis.Strings(this.Do("SMEMBERS", set))
}

func (this *Cache) SAdd(set string, value string) int {
	return redis.Int(this.Do("SADD", set, value))
}

func (this *Cache) SRem(set string, value string) int {
	return redis.Int(this.Do("SREM", set, value))
}

func (this *Cache) SPop(set string) string {
	return redis.String(this.Do("SPOP", set))
}

func (this *Cache) Type(key string) string {
	return redis.String(this.Do("TYPE", key))
}

func (this *Cache) TTL(key string) int {
	return redis.Int(this.Do("TTL", key))
}

func New(config *Config) *Cache {
	if config.Pool < 5 {
		config.Pool = 5
	}

	if config.Active < 1 {
		config.Active = 1
	}

	if config.Timeout < 1 {
		config.Timeout = 3600
	}

	return &Cache{
		config: config,
		pool: &redis.Pool {
			MaxIdle: config.Pool,
			MaxActive: config.Active,
			IdleTimeout: 30 * time.Second,
			MaxConnLifetime: time.Duration(config.Timeout) * time.Second,
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", fmt.Sprintf("%v:%v", config.Host, config.Port), redis.DialPassword(config.Passwd))
			},
			Wait:true,
		},
	}
}

var handler *Cache

func Init(config *Config) {
	handler = New(config)
}

func Close() {
	handler.Close()
}

func Has(key string) int {
	return handler.Has(key)
}

func Get(key string) string {
	return handler.Get(key)
}

func MGet(keys ...interface{}) []interface{} {
	return handler.MGet(keys...)
}

func Append(key string, value string) int {
	return handler.Append(key, value)
}

func Set(key string, value string) bool {
	return handler.Set(key, value)
}

func SetEx(key string, value string, timeout int) bool {
	return handler.SetEx(key, value, timeout)
}

func SetNx(key string, value string) int {
	return handler.SetNx(key, value)
}

func Del(key string) int {
	return handler.Del(key)
}

func Keys(key string) []string {
	return handler.Keys(key)
}

func Expire(key string, timeout int) int {
	return handler.Expire(key, timeout)
}

func StrLen(key string) int {
	return handler.StrLen(key)
}

func ReName(key string, name string) bool {
	return handler.ReName(key, name)
}

func Incr(key string) int {
	return handler.Incr(key)
}

func IncrBy(key string, num int) int {
	return handler.IncrBy(key, num)
}

func Decr(key string) int {
	return handler.Decr(key)
}

func DecrBy(key string, num int) int {
	return handler.DecrBy(key, num)
}

func HIncrBy(hash string, key string, num int) int {
	return handler.HIncrBy(hash, key, num)
}

func LIndex(key string, index int) string {
	return handler.LIndex(key, index)
}

func LLen(key string) int {
	return handler.LLen(key)
}

func LPop(key string) string {
	return handler.LPop(key)
}

func LTrim(key string, start int, stop int) bool {
	return handler.LTrim(key, start, stop)
}

func LRange(key string, start int, end int) []string {
	return handler.LRange(key, start, end)
}

func LPush(key string, value string) int {
	return handler.LPush(key, value)
}

func LPushX(key string, value string) int {
	return handler.LPushX(key, value)
}

func RPush(key string, value string) int {
	return handler.RPush(key, value)
}

func RPushX(key string, value string) int {
	return handler.RPushX(key, value)
}

func RPop(key string) string {
	return handler.RPop(key)
}

func HLen(hash string) int {
	return handler.HLen(hash)
}

func HDel(hash string, key string) int {
	return handler.HDel(hash, key)
}

func HExists(hash string, key string) int {
	return handler.HExists(hash, key)
}

func HGet(hash string, key string) string {
	return handler.HGet(hash, key)
}

func HKeys(hash string) []string {
	return handler.HKeys(hash)
}

func HSet(hash string, key string, value string) int {
	return handler.HSet(hash, key, value)
}

func HSetNx(hash string, key string, value string) int {
	return handler.HSetNx(hash, key, value)
}

func SCard(set string) int {
	return handler.SCard(set)
}

func SMembers(set string) []string {
	return handler.SMembers(set)
}

func SAdd(set string, value string) int {
	return handler.SAdd(set, value)
}

func SRem(set string, value string) int {
	return handler.SRem(set, value)
}

func SPop(set string) string {
	return handler.SPop(set)
}

func Type(key string) string {
	return handler.Type(key)
}

func TTL(key string) int {
	return handler.TTL(key)
}