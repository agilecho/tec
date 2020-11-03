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

type Condition map[string]interface{}
type Document map[string]interface{}

type Collection struct {
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
			logger("mongo.Find error:" + err.Error())
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
			logger("mongo.Find error:" + err.Error())
			return rows
		}
	}

	return rows
}

func (this *Collection) FindOne(where Condition) Document {
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
		logger("mongo.Insert error:" + err.Error())
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
		logger("mongo.Update error:" + err.Error())
		return false
	}

	return true
}

func (this *Collection) Remove(where Collection) bool {
	err := this.handle.Remove(where)
	if err != nil {
		logger("mongo.Remove error:" + err.Error())
		return false
	}

	return true
}

var CONFIG *Config
var session *mgo.Session
var rwMu = &sync.RWMutex{}

// mongo error log
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

	session, _ = mgo.Dial(config.Host + ":" + config.Port)
	session.DB(config.Database).Login(config.User, config.Passwd)
	session.SetPoolLimit(config.Pool)

	CONFIG = config
}

func Close() {
	if session != nil {
		session.Close()
	}
}

func ListDBs() []string {
	client := session.Clone()
	names, err := client.DatabaseNames()

	if err != nil {
		logger("mongo.ListDBs error:" + err.Error())
	}

	return names
}

func ListCollections(db string) []*Collection {
	client := session.Clone()

	collections := []*Collection{}

	names, err := client.DB(db).CollectionNames()

	if err != nil {
		logger("mongo.ListCollections error:" + err.Error())
	}

	if len(names) == 0 {
		return collections
	}

	for i := 0; i < len(names); i++ {
		collections = append(collections, &Collection{
			handle: client.DB(db).C(names[i]),
			Name: names[i],
		})
	}

	return collections
}

func CreateCollection(db string, collection string) bool {
	client := session.Clone()
	err := client.DB(db).C(collection).Create(&mgo.CollectionInfo{})
	if err != nil {
		logger("mongo.CreateCollection error:" + err.Error())
		return false
	}

	return true
}

func SelectCollection(db string, collection string) *Collection {
	client := session.Clone()

	return &Collection {
		handle: client.DB(db).C(collection),
		Name:collection,
	}
}

func DropCollection(db string, collection string) bool {
	client := session.Clone()
	err := client.DB(db).C(collection).DropCollection()
	if err != nil {
		logger("mongo.DropCollection error:" + err.Error())
		return false
	}

	return true
}