package tec

import (
	"github.com/agilecho/tec/cache"
	"github.com/agilecho/tec/db"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Session interface {
	Get(key string) interface{}
	Set(key string, value interface{})
	Remove(key string)

	Identity() string
	Expired() bool

	Start()
	Flush()
	Close()
	Destroy()
}

var serial int64 = 1
var sessionMap map[string]Session
var sessionMapMutex = &sync.Mutex{}

type SessionOfFile struct {
	identity string
	data map[string]interface{}
	path string
}

func (this *SessionOfFile) Get(key string) interface{} {
	if key == "" {
		return this.data
	}

	return this.data[key]
}

func (this *SessionOfFile) Set(key string, value interface{}) {
	this.data[key] = value
	this.Flush()
}

func (this *SessionOfFile) Remove(key string) {
	delete(this.data, key)
	this.Flush()
}

func (this *SessionOfFile) Identity() string {
	return this.identity
}

func (this *SessionOfFile) Path() string {
	return this.path
}

func (this *SessionOfFile) Expired() bool {
	if this.data == nil {
		return true
	}

	if _, ok := this.data["timestamp"]; !ok {
		return true
	}

	if int64(this.data["timestamp"].(float64)) <= time.Now().Unix() - int64(CONFIG.Session.Expire) {
		return true
	}

	return false
}

func (this *SessionOfFile) Start() {
	group := strconv.FormatFloat(Ceil(float64(serial) / 100000), 'f', -1, 64)
	if !IsDir(CONFIG.Session.Path + "/session" + group) {
		err := os.MkdirAll(CONFIG.Session.Path + "/session" + group, os.ModePerm)
		if err != nil {
			Logger("create session path:" + CONFIG.Session.Path + "/session" + group + " error:" + err.Error(), "error")
			return
		}
	}

	this.path = CONFIG.Session.Path + "/session" + group + "/" + this.identity

	content := FileGetContents(this.path)
	if content == "" {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		return
	}

	this.data = JsonDecode(content)
	if _, ok := this.data["timestamp"]; !ok {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		return
	}

	if int64(this.data["timestamp"].(float64)) <= time.Now().Unix() - int64(CONFIG.Session.Expire) {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		return
	}
}

func (this *SessionOfFile) Flush() {
	if this.data == nil {
		return
	}

	this.data["timestamp"] = float64(time.Now().Unix())

	err := ioutil.WriteFile(this.path, []byte(JsonEncode(this.data)), 0666)
	if err != nil {
		Logger("flush session path:" + this.path + " error:" + err.Error(), "error")
	}
}

func (this *SessionOfFile) Close() {
	if int64(this.data["timestamp"].(float64)) + 60 <= time.Now().Unix() {
		this.Flush()
	}
}

func (this *SessionOfFile) Destroy() {
	if !FileExists(this.path) {
		return
	}

	err := Unlink(this.path)
	if err != nil {
		Logger("remove session file error:" + err.Error(), "error")
	}
}


type SessionOfRedis struct {
	identity string
	data map[string]interface{}
}

func (this *SessionOfRedis) Get(key string) interface{} {
	if key == "" {
		return this.data
	}

	return this.data[key]
}

func (this *SessionOfRedis) Set(key string, value interface{}) {
	this.data[key] = value
	this.Flush()
}

func (this *SessionOfRedis) Remove(key string) {
	delete(this.data, key)
	this.Flush()
}

func (this *SessionOfRedis) Identity() string {
	return this.identity
}

func (this *SessionOfRedis) Expired() bool {
	if this.data == nil {
		return true
	}

	if _, ok := this.data["timestamp"]; !ok {
		return true
	}

	if int64(this.data["timestamp"].(float64)) <= time.Now().Unix() - int64(CONFIG.Session.Expire)  {
		return true
	}

	return false
}

func (this *SessionOfRedis) Start() {
	if cache.HExists("SESSION", this.identity) == 0 {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		return
	}

	content := cache.HGet("SESSION", this.identity)
	if content == "" {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		cache.HDel("SESSION", this.identity)
		return
	}

	this.data = JsonDecode(content)

	if _, ok := this.data["timestamp"]; !ok {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		cache.HDel("SESSION", this.identity)
		return
	}

	if int64(this.data["timestamp"].(float64)) > time.Now().Unix() - int64(CONFIG.Session.Expire)  {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		cache.HDel("SESSION", this.identity)
		return
	}
}

func (this *SessionOfRedis) Flush() {
	this.data["timestamp"] = float64(time.Now().Unix())
	cache.HSet("SESSION", this.identity, JsonEncode(this.data))
}

func (this *SessionOfRedis) Close() {
	if int64(this.data["timestamp"].(float64)) + 60 <= time.Now().Unix() {
		this.Flush()
	}
}

func (this *SessionOfRedis) Destroy() {
	cache.HDel("SESSION", this.identity)
}

type SessionOfMySql struct {
	identity string
	data map[string]interface{}
}

func (this *SessionOfMySql) Get(key string) interface{} {
	if key == "" {
		return this.data
	}

	return this.data[key]
}

func (this *SessionOfMySql) Set(key string, value interface{}) {
	this.data[key] = value
	this.Flush()
}

func (this *SessionOfMySql) Remove(key string) {
	delete(this.data, key)
	this.Flush()
}

func (this *SessionOfMySql) Identity() string {
	return this.identity
}

func (this *SessionOfMySql) Expired() bool {
	if this.data == nil {
		return true
	}

	if _, ok := this.data["timestamp"]; !ok {
		return true
	}

	if int64(this.data["timestamp"].(float64)) <= time.Now().Unix() - int64(CONFIG.Session.Expire)  {
		return true
	}

	return false
}

func (this *SessionOfMySql) Start() {
	row := db.FetchFirst("SELECT * FROM session WHERE identity = ?", this.identity)
	if row == nil {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		return
	}

	this.data = JsonDecode(row["content"].(string))

	if _, ok := this.data["timestamp"]; !ok {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		return
	}

	if int64(this.data["timestamp"].(float64)) <= time.Now().Unix() - int64(CONFIG.Session.Expire)  {
		this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
		return
	}
}

func (this *SessionOfMySql) Flush() {
	this.data["timestamp"] = float64(time.Now().Unix())
	db.Update("session", db.Row{"content" : JsonEncode(this.data)}, "identity = ?", this.identity)
}

func (this *SessionOfMySql) Close() {
	if int64(this.data["timestamp"].(float64)) + 60 <= time.Now().Unix() {
		this.Flush()
	}
}

func (this *SessionOfMySql) Destroy() {
	db.Delete("session", "identity = ?", this.identity)
}


type SessionOfMemory struct {
	identity string
	data map[string]interface{}
}

func (this *SessionOfMemory) Get(key string) interface{} {
	if key == "" {
		return this.data
	}

	return this.data[key]
}

func (this *SessionOfMemory) Set(key string, value interface{}) {
	this.data[key] = value
	this.Flush()
}

func (this *SessionOfMemory) Remove(key string) {
	delete(this.data, key)
	this.Flush()
}

func (this *SessionOfMemory) Identity() string {
	return this.identity
}

func (this *SessionOfMemory) Expired() bool {
	if this.data == nil {
		return true
	}

	if _, ok := this.data["timestamp"]; !ok {
		return true
	}

	if int64(this.data["timestamp"].(float64)) <= time.Now().Unix() - int64(CONFIG.Session.Expire)  {
		return true
	}

	return false
}

func (this *SessionOfMemory) Start() {
	this.data = map[string]interface{}{"timestamp": float64(time.Now().Unix())}
}

func (this *SessionOfMemory) Flush() {
	this.data["timestamp"] = float64(time.Now().Unix())
}

func (this *SessionOfMemory) Close() {
	if int64(this.data["timestamp"].(float64)) + 60 <= time.Now().Unix() {
		this.Flush()
	}
}

func (this *SessionOfMemory) Destroy() {
	this.data = nil
}

func sessionStart() {
	sessionMap = map[string]Session{}
}

func sessionGC() {
	for _, session := range sessionMap {
		if InArray(CONFIG.Session.Type, []string{"redis", "mysql"}) && !session.Expired() {
			continue
		}

		session.Destroy()
	}

	sessionMap = nil

	if CONFIG.Session.Type == "file" {
		fs, _ := ioutil.ReadDir(CONFIG.Session.Path)
		for _, file := range fs {
			if file.IsDir() {
				files, _ := ioutil.ReadDir(CONFIG.Session.Path + "/" + file.Name())
				for _, fle := range files {
					session := &SessionOfFile{identity:fle.Name()}
					session.Start()

					if session.Expired() {
						session.Destroy()
					}
				}
			}
		}
	}
}

func sessionCreate(rep http.ResponseWriter, req *http.Request) Session {
	cookie, err := req.Cookie(CONFIG.Session.Name)

	var identity string
	if cookie == nil || err != nil || cookie.Value == "" {
		identity = GetUUID()

		http.SetCookie(rep, &http.Cookie{
			Name: CONFIG.Session.Name,
			Value: identity,
			MaxAge: CONFIG.Session.Expire,
			Path: CONFIG.Cookie.Path,
			Domain: CONFIG.Cookie.Domain,
			Secure: CONFIG.Cookie.Secure,
			HttpOnly: CONFIG.Cookie.HttpOnly,
		})

		atomic.AddInt64(&serial, 1)
	} else {
		identity = cookie.Value
	}

	var session Session

	if CONFIG.Session.Type == "file" {
		session = &SessionOfFile{identity:identity}
	} else if CONFIG.Session.Type == "redis" {
		session = &SessionOfRedis{identity:identity}
	} else if CONFIG.Session.Type == "mysql" {
		session = &SessionOfMySql{identity:identity}
	} else {
		session = &SessionOfMemory{identity:identity}
	}

	session.Start()

	sessionMapMutex.Lock()
	sessionMap[identity] = session
	sessionMapMutex.Unlock()

	return session
}

func SessionGet(identity string) Session {
	return sessionMap[identity]
}
