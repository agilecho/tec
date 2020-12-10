package mongo

import (
	"fmt"
	"github.com/agilecho/tec/mongo/mgo"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Condition map[string]interface{}
type Document map[string]interface{}

type Collection struct {
	mongo *Mongo
	handle *mgo.Collection
	Name string
}

func (this *Collection) Count(where Condition) int {
	count, _ := this.handle.Find(where).Count()
	return count
}

func (this *Collection) Find(where Condition, sort string, skip int, limit int) []Document {
	rows := []Document{}

	if limit <= 0 {
		var err error
		if sort == "" {
			err = this.handle.Find(where).All(&rows)
		} else {
			err = this.handle.Find(where).Sort(sort).All(&rows)
		}

		if err != nil {
			this.mongo.logger("mongo.Find error:" + err.Error())
			return rows
		}
	} else {
		var err error
		if sort == "" {
			err = this.handle.Find(where).Skip(skip).Limit(limit).All(&rows)
		} else {
			err = this.handle.Find(where).Sort(sort).Skip(skip).Limit(limit).All(&rows)
		}

		if err != nil {
			this.mongo.logger("mongo.Find error:" + err.Error())
			return rows
		}
	}

	return rows
}

func (this *Collection) One(where Condition) Document {
	documents := this.Find(where, "", 0, 1)

	if len(documents) == 0 {
		return nil
	} else {
		return documents[0]
	}
}

func (this *Collection) Insert(document Document) bool {
	err := this.handle.Insert(document)
	if err != nil {
		this.mongo.logger("mongo.Insert error:" + err.Error())
		return false
	}

	return true
}

func (this *Collection) Update(where Collection, document Document, args ...bool) bool {
	var err error

	if len(args) > 0 && args[0] == true {
		_, err = this.handle.UpdateAll(where, document)
	} else {
		err = this.handle.Update(where, document)
	}

	if err != nil {
		this.mongo.logger("mongo.Update error:" + err.Error())
		return false
	}

	return true
}

func (this *Collection) Remove(where Collection) bool {
	err := this.handle.Remove(where)
	if err != nil {
		this.mongo.logger("mongo.Remove error:" + err.Error())
		return false
	}

	return true
}

type Config struct {
	Host string
	Port string
	User string
	Passwd string
	Database string
	Prefix string
	Pool int
	Logs string
	Debug bool
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
	case "prefix":
		this.Prefix = value
	case "pool":
		this.Pool, _ = strconv.Atoi(value)
	case "logs":
		this.Logs = value
	case "debug":
		this.Debug, _ = strconv.ParseBool(value)
	}
}

type Mongo struct {
	config *Config
	session *mgo.Session
	mu sync.RWMutex
}

func (this *Mongo) microtime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano() / 1e6) * 0.001, 'f', 4, 64)
}

func (this *Mongo) logger(message string) {
	if this.config == nil {
		return
	}

	now := time.Now()

	var text = strings.Builder{}
	text.WriteString(fmt.Sprintf("%v%v%v%v%v%v", now.Format("2006-01-02 15:04:05"), "(", this.microtime(), ") ", message, "\r\n"))

	if this.config.Debug  {
		fmt.Print(text.String())
	}

	go func(data string, that *Mongo) {
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

func (this *Mongo) ListDBs() []string {
	client := this.session.Clone()

	names, err := client.DatabaseNames()
	if err != nil {
		this.logger("mongo.ListDBs error:" + err.Error())
	}

	return names
}

func (this *Mongo) ListCollections(db string) []*Collection {
	client := this.session.Clone()

	collections := []*Collection{}

	names, err := client.DB(db).CollectionNames()
	if err != nil {
		this.logger("mongo.ListCollections error:" + err.Error())
	}

	if len(names) == 0 {
		return collections
	}

	for i := 0; i < len(names); i++ {
		collections = append(collections, &Collection{
			mongo: this,
			handle: client.DB(db).C(names[i]),
			Name: names[i],
		})
	}

	return collections
}

func (this *Mongo) CreateCollection(db string, collection string) bool {
	client := this.session.Clone()

	err := client.DB(db).C(collection).Create(&mgo.CollectionInfo{})
	if err != nil {
		this.logger("mongo.CreateCollection error:" + err.Error())
		return false
	}

	return true
}

func (this *Mongo) SelectCollection(db string, collection string) *Collection {
	client := this.session.Clone()

	return &Collection {
		mongo: this,
		handle: client.DB(db).C(collection),
		Name:collection,
	}
}

func (this *Mongo) DropCollection(db string, collection string) bool {
	client := this.session.Clone()

	err := client.DB(db).C(collection).DropCollection()
	if err != nil {
		this.logger("mongo.DropCollection error:" + err.Error())
		return false
	}

	return true
}

func (this *Mongo) Close() {
	if this.session != nil {
		this.session.Close()
	}
}

func New(config *Config) *Mongo {
	if config.Pool < 5 {
		config.Pool = 5
	}

	tmp := &Mongo{config:config}

	session, _ := mgo.Dial(config.Host + ":" + config.Port)
	session.DB(config.Database).Login(config.User, config.Passwd)
	session.SetPoolLimit(config.Pool)

	tmp.session = session

	return tmp
}

var handler *Mongo

func Init(config *Config) {
	handler = New(config)
}

func ListDBs() []string {
	return handler.ListDBs()
}

func ListCollections(db string) []*Collection {
	return handler.ListCollections(db)
}

func CreateCollection(db string, collection string) bool {
	return handler.CreateCollection(db, collection)
}

func SelectCollection(db string, collection string) *Collection {
	return handler.SelectCollection(db, collection)
}

func DropCollection(db string, collection string) bool {
	return handler.DropCollection(db, collection)
}

func Close() {
	handler.Close()
}