package mgo

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"tec/mongo/mgo/bson"
	"time"
)

type Mode int

const (
	Primary            Mode = 2
	PrimaryPreferred   Mode = 3
	Secondary          Mode = 4
	SecondaryPreferred Mode = 5
	Nearest            Mode = 6
	Eventual  Mode = 0
	Monotonic Mode = 1
	Strong    Mode = 2
)

type Session struct {
	m                sync.RWMutex
	cluster_         *mongoCluster
	slaveSocket      *mongoSocket
	masterSocket     *mongoSocket
	slaveOk          bool
	consistency      Mode
	queryConfig      query
	safeOp           *queryOp
	syncTimeout      time.Duration
	sockTimeout      time.Duration
	defaultdb        string
	sourcedb         string
	dialCred         *Credential
	creds            []Credential
	poolLimit        int
	bypassValidation bool
}

type Database struct {
	Session *Session
	Name    string
}

type Collection struct {
	Database *Database
	Name     string
	FullName string
}

type Query struct {
	m       sync.Mutex
	session *Session
	query
}

type query struct {
	op       queryOp
	prefetch float64
	limit    int32
}

type getLastError struct {
	CmdName  int         "getLastError,omitempty"
	W        interface{} "w,omitempty"
	WTimeout int         "wtimeout,omitempty"
	FSync    bool        "fsync,omitempty"
	J        bool        "j,omitempty"
}

type Iter struct {
	m              sync.Mutex
	gotReply       sync.Cond
	session        *Session
	server         *mongoServer
	docData        queue
	err            error
	op             getMoreOp
	prefetch       float64
	limit          int32
	docsToReceive  int
	docsBeforeMore int
	timeout        time.Duration
	timedout       bool
	findCmd        bool
}

var (
	ErrNotFound = errors.New("not found")
	ErrCursor   = errors.New("invalid cursor")
)

const (
	defaultPrefetch  = 0.25
	maxUpsertRetries = 5
)

func Dial(url string) (*Session, error) {
	session, err := DialWithTimeout(url, 10*time.Second)
	if err == nil {
		session.SetSyncTimeout(1 * time.Minute)
		session.SetSocketTimeout(1 * time.Minute)
	}
	return session, err
}

func DialWithTimeout(url string, timeout time.Duration) (*Session, error) {
	info, err := ParseURL(url)
	if err != nil {
		return nil, err
	}
	info.Timeout = timeout
	return DialWithInfo(info)
}

func ParseURL(url string) (*DialInfo, error) {
	uinfo, err := extractURL(url)
	if err != nil {
		return nil, err
	}
	direct := false
	mechanism := ""
	service := ""
	source := ""
	setName := ""
	poolLimit := 0
	for k, v := range uinfo.options {
		switch k {
		case "authSource":
			source = v
		case "authMechanism":
			mechanism = v
		case "gssapiServiceName":
			service = v
		case "replicaSet":
			setName = v
		case "maxPoolSize":
			poolLimit, err = strconv.Atoi(v)
			if err != nil {
				return nil, errors.New("bad value for maxPoolSize: " + v)
			}
		case "connect":
			if v == "direct" {
				direct = true
				break
			}
			if v == "replicaSet" {
				break
			}
			fallthrough
		default:
			return nil, errors.New("unsupported connection URL option: " + k + "=" + v)
		}
	}
	info := DialInfo{
		Addrs:          uinfo.addrs,
		Direct:         direct,
		Database:       uinfo.db,
		Username:       uinfo.user,
		Password:       uinfo.pass,
		Mechanism:      mechanism,
		Service:        service,
		Source:         source,
		PoolLimit:      poolLimit,
		ReplicaSetName: setName,
	}
	return &info, nil
}

type DialInfo struct {
	Addrs []string
	Direct bool
	Timeout time.Duration
	FailFast bool
	Database string
	ReplicaSetName string
	Source string
	Service string
	ServiceHost string
	Mechanism string
	Username string
	Password string
	PoolLimit int
	DialServer func(addr *ServerAddr) (net.Conn, error)
	Dial func(addr net.Addr) (net.Conn, error)
}

type ServerAddr struct {
	str string
	tcp *net.TCPAddr
}

func (addr *ServerAddr) String() string {
	return addr.str
}

func (addr *ServerAddr) TCPAddr() *net.TCPAddr {
	return addr.tcp
}

func DialWithInfo(info *DialInfo) (*Session, error) {
	addrs := make([]string, len(info.Addrs))
	for i, addr := range info.Addrs {
		p := strings.LastIndexAny(addr, "]:")
		if p == -1 || addr[p] != ':' {
			addr += ":27017"
		}
		addrs[i] = addr
	}
	cluster := newCluster(addrs, info.Direct, info.FailFast, dialer{info.Dial, info.DialServer}, info.ReplicaSetName)
	session := newSession(Eventual, cluster, info.Timeout)
	session.defaultdb = info.Database
	if session.defaultdb == "" {
		session.defaultdb = "test"
	}
	session.sourcedb = info.Source
	if session.sourcedb == "" {
		session.sourcedb = info.Database
		if session.sourcedb == "" {
			session.sourcedb = "admin"
		}
	}
	if info.Username != "" {
		source := session.sourcedb
		if info.Source == "" &&
			(info.Mechanism == "GSSAPI" || info.Mechanism == "PLAIN" || info.Mechanism == "MONGODB-X509") {
			source = "$external"
		}
		session.dialCred = &Credential{
			Username:    info.Username,
			Password:    info.Password,
			Mechanism:   info.Mechanism,
			Service:     info.Service,
			ServiceHost: info.ServiceHost,
			Source:      source,
		}
		session.creds = []Credential{*session.dialCred}
	}
	if info.PoolLimit > 0 {
		session.poolLimit = info.PoolLimit
	}
	cluster.Release()

	if err := session.Ping(); err != nil {
		session.Close()
		return nil, err
	}
	session.SetMode(Strong, true)
	return session, nil
}

func isOptSep(c rune) bool {
	return c == ';' || c == '&'
}

type urlInfo struct {
	addrs   []string
	user    string
	pass    string
	db      string
	options map[string]string
}

func extractURL(s string) (*urlInfo, error) {
	if strings.HasPrefix(s, "mongodb://") {
		s = s[10:]
	}
	info := &urlInfo{options: make(map[string]string)}
	if c := strings.Index(s, "?"); c != -1 {
		for _, pair := range strings.FieldsFunc(s[c+1:], isOptSep) {
			l := strings.SplitN(pair, "=", 2)
			if len(l) != 2 || l[0] == "" || l[1] == "" {
				return nil, errors.New("connection option must be key=value: " + pair)
			}
			info.options[l[0]] = l[1]
		}
		s = s[:c]
	}
	if c := strings.Index(s, "@"); c != -1 {
		pair := strings.SplitN(s[:c], ":", 2)
		if len(pair) > 2 || pair[0] == "" {
			return nil, errors.New("credentials must be provided as user:pass@host")
		}
		var err error
		info.user, err = url.QueryUnescape(pair[0])
		if err != nil {
			return nil, fmt.Errorf("cannot unescape username in URL: %q", pair[0])
		}
		if len(pair) > 1 {
			info.pass, err = url.QueryUnescape(pair[1])
			if err != nil {
				return nil, fmt.Errorf("cannot unescape password in URL")
			}
		}
		s = s[c+1:]
	}
	if c := strings.Index(s, "/"); c != -1 {
		info.db = s[c+1:]
		s = s[:c]
	}
	info.addrs = strings.Split(s, ",")
	return info, nil
}

func newSession(consistency Mode, cluster *mongoCluster, timeout time.Duration) (session *Session) {
	cluster.Acquire()
	session = &Session{
		cluster_:    cluster,
		syncTimeout: timeout,
		sockTimeout: timeout,
		poolLimit:   4096,
	}

	session.SetMode(consistency, true)
	session.SetSafe(&Safe{})
	session.queryConfig.prefetch = defaultPrefetch
	return session
}

func copySession(session *Session, keepCreds bool) (s *Session) {
	cluster := session.cluster()
	cluster.Acquire()
	if session.masterSocket != nil {
		session.masterSocket.Acquire()
	}
	if session.slaveSocket != nil {
		session.slaveSocket.Acquire()
	}
	var creds []Credential
	if keepCreds {
		creds = make([]Credential, len(session.creds))
		copy(creds, session.creds)
	} else if session.dialCred != nil {
		creds = []Credential{*session.dialCred}
	}
	scopy := *session
	scopy.m = sync.RWMutex{}
	scopy.creds = creds
	s = &scopy
	return s
}

func (s *Session) LiveServers() (addrs []string) {
	s.m.RLock()
	addrs = s.cluster().LiveServers()
	s.m.RUnlock()
	return addrs
}

func (s *Session) DB(name string) *Database {
	if name == "" {
		name = s.defaultdb
	}
	return &Database{s, name}
}

func (db *Database) C(name string) *Collection {
	return &Collection{db, name, db.Name + "." + name}
}

func (db *Database) With(s *Session) *Database {
	newdb := *db
	newdb.Session = s
	return &newdb
}

func (c *Collection) With(s *Session) *Collection {
	newdb := *c.Database
	newdb.Session = s
	newc := *c
	newc.Database = &newdb
	return &newc
}


func (db *Database) GridFS(prefix string) *GridFS {
	return newGridFS(db, prefix)
}

func (db *Database) Run(cmd interface{}, result interface{}) error {
	socket, err := db.Session.acquireSocket(true)
	if err != nil {
		return err
	}
	defer socket.Release()

	return db.run(socket, cmd, result)
}

type Credential struct {
	Username string
	Password string
	Source string
	Service string
	ServiceHost string
	Mechanism string
}

func (db *Database) Login(user, pass string) error {
	return db.Session.Login(&Credential{Username: user, Password: pass, Source: db.Name})
}

func (s *Session) Login(cred *Credential) error {
	socket, err := s.acquireSocket(true)
	if err != nil {
		return err
	}
	defer socket.Release()

	credCopy := *cred
	if cred.Source == "" {
		if cred.Mechanism == "GSSAPI" {
			credCopy.Source = "$external"
		} else {
			credCopy.Source = s.sourcedb
		}
	}
	err = socket.Login(credCopy)
	if err != nil {
		return err
	}

	s.m.Lock()
	s.creds = append(s.creds, credCopy)
	s.m.Unlock()
	return nil
}

func (s *Session) socketLogin(socket *mongoSocket) error {
	for _, cred := range s.creds {
		if err := socket.Login(cred); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) Logout() {
	session := db.Session
	dbname := db.Name
	session.m.Lock()
	found := false
	for i, cred := range session.creds {
		if cred.Source == dbname {
			copy(session.creds[i:], session.creds[i+1:])
			session.creds = session.creds[:len(session.creds)-1]
			found = true
			break
		}
	}
	if found {
		if session.masterSocket != nil {
			session.masterSocket.Logout(dbname)
		}
		if session.slaveSocket != nil {
			session.slaveSocket.Logout(dbname)
		}
	}
	session.m.Unlock()
}

func (s *Session) LogoutAll() {
	s.m.Lock()
	for _, cred := range s.creds {
		if s.masterSocket != nil {
			s.masterSocket.Logout(cred.Source)
		}
		if s.slaveSocket != nil {
			s.slaveSocket.Logout(cred.Source)
		}
	}
	s.creds = s.creds[0:0]
	s.m.Unlock()
}

type User struct {
	Username string `bson:"user"`
	Password string `bson:",omitempty"`
	PasswordHash string `bson:"pwd,omitempty"`
	CustomData interface{} `bson:"customData,omitempty"`
	Roles []Role `bson:"roles"`
	OtherDBRoles map[string][]Role `bson:"otherDBRoles,omitempty"`
	UserSource string `bson:"userSource,omitempty"`
}

type Role string

const (
	RoleRoot         Role = "root"
	RoleRead         Role = "read"
	RoleReadAny      Role = "readAnyDatabase"
	RoleReadWrite    Role = "readWrite"
	RoleReadWriteAny Role = "readWriteAnyDatabase"
	RoleDBAdmin      Role = "dbAdmin"
	RoleDBAdminAny   Role = "dbAdminAnyDatabase"
	RoleUserAdmin    Role = "userAdmin"
	RoleUserAdminAny Role = "userAdminAnyDatabase"
	RoleClusterAdmin Role = "clusterAdmin"
)

func (db *Database) UpsertUser(user *User) error {
	if user.Username == "" {
		return fmt.Errorf("user has no Username")
	}

	if (user.Password != "" || user.PasswordHash != "") && user.UserSource != "" {
		return fmt.Errorf("user has both Password/PasswordHash and UserSource set")
	}

	if len(user.OtherDBRoles) > 0 && db.Name != "admin" && db.Name != "$external" {
		return fmt.Errorf("user with OtherDBRoles is only supported in the admin or $external databases")
	}

	rundb := db
	if user.UserSource != "" {
		rundb = db.Session.DB(user.UserSource)
	}
	err := rundb.runUserCmd("updateUser", user)
	if isNotFound(err) || isAuthError(err) {
		return rundb.runUserCmd("createUser", user)
	}

	if !isNoCmd(err) {
		return err
	}

	var set, unset bson.D
	if user.Password != "" {
		psum := md5.New()
		psum.Write([]byte(user.Username + ":mongo:" + user.Password))
		set = append(set, bson.DocElem{"pwd", hex.EncodeToString(psum.Sum(nil))})
		unset = append(unset, bson.DocElem{"userSource", 1})
	} else if user.PasswordHash != "" {
		set = append(set, bson.DocElem{"pwd", user.PasswordHash})
		unset = append(unset, bson.DocElem{"userSource", 1})
	}
	if user.UserSource != "" {
		set = append(set, bson.DocElem{"userSource", user.UserSource})
		unset = append(unset, bson.DocElem{"pwd", 1})
	}
	if user.Roles != nil || user.OtherDBRoles != nil {
		set = append(set, bson.DocElem{"roles", user.Roles})
		if len(user.OtherDBRoles) > 0 {
			set = append(set, bson.DocElem{"otherDBRoles", user.OtherDBRoles})
		} else {
			unset = append(unset, bson.DocElem{"otherDBRoles", 1})
		}
	}
	users := db.C("system.users")
	err = users.Update(bson.D{{"user", user.Username}}, bson.D{{"$unset", unset}, {"$set", set}})
	if err == ErrNotFound {
		set = append(set, bson.DocElem{"user", user.Username})
		if user.Roles == nil && user.OtherDBRoles == nil {
			set = append(set, bson.DocElem{"roles", user.Roles})
		}
		err = users.Insert(set)
	}
	return err
}

func isNoCmd(err error) bool {
	e, ok := err.(*QueryError)
	return ok && (e.Code == 59 || e.Code == 13390 || strings.HasPrefix(e.Message, "no such cmd:"))
}

func isNotFound(err error) bool {
	e, ok := err.(*QueryError)
	return ok && e.Code == 11
}

func isAuthError(err error) bool {
	e, ok := err.(*QueryError)
	return ok && e.Code == 13
}

func (db *Database) runUserCmd(cmdName string, user *User) error {
	cmd := make(bson.D, 0, 16)
	cmd = append(cmd, bson.DocElem{cmdName, user.Username})
	if user.Password != "" {
		cmd = append(cmd, bson.DocElem{"pwd", user.Password})
	}
	var roles []interface{}
	for _, role := range user.Roles {
		roles = append(roles, role)
	}
	for db, dbroles := range user.OtherDBRoles {
		for _, role := range dbroles {
			roles = append(roles, bson.D{{"role", role}, {"db", db}})
		}
	}
	if roles != nil || user.Roles != nil || cmdName == "createUser" {
		cmd = append(cmd, bson.DocElem{"roles", roles})
	}
	err := db.Run(cmd, nil)
	if !isNoCmd(err) && user.UserSource != "" && (user.UserSource != "$external" || db.Name != "$external") {
		return fmt.Errorf("MongoDB 2.6+ does not support the UserSource setting")
	}
	return err
}

func (db *Database) AddUser(username, password string, readOnly bool) error {
	user := &User{Username: username, Password: password}
	if db.Name == "admin" {
		if readOnly {
			user.Roles = []Role{RoleReadAny}
		} else {
			user.Roles = []Role{RoleReadWriteAny}
		}
	} else {
		if readOnly {
			user.Roles = []Role{RoleRead}
		} else {
			user.Roles = []Role{RoleReadWrite}
		}
	}
	err := db.runUserCmd("updateUser", user)
	if isNotFound(err) {
		return db.runUserCmd("createUser", user)
	}
	if !isNoCmd(err) {
		return err
	}

	psum := md5.New()
	psum.Write([]byte(username + ":mongo:" + password))
	digest := hex.EncodeToString(psum.Sum(nil))
	c := db.C("system.users")
	_, err = c.Upsert(bson.M{"user": username}, bson.M{"$set": bson.M{"user": username, "pwd": digest, "readOnly": readOnly}})
	return err
}

func (db *Database) RemoveUser(user string) error {
	err := db.Run(bson.D{{"dropUser", user}}, nil)
	if isNoCmd(err) {
		users := db.C("system.users")
		return users.Remove(bson.M{"user": user})
	}
	if isNotFound(err) {
		return ErrNotFound
	}
	return err
}

type indexSpec struct {
	Name, NS         string
	Key              bson.D
	Unique           bool    ",omitempty"
	DropDups         bool    "dropDups,omitempty"
	Background       bool    ",omitempty"
	Sparse           bool    ",omitempty"
	Bits             int     ",omitempty"
	Min, Max         float64 ",omitempty"
	BucketSize       float64 "bucketSize,omitempty"
	ExpireAfter      int     "expireAfterSeconds,omitempty"
	Weights          bson.D  ",omitempty"
	DefaultLanguage  string  "default_language,omitempty"
	LanguageOverride string  "language_override,omitempty"
	TextIndexVersion int     "textIndexVersion,omitempty"

	Collation *Collation "collation,omitempty"
}

type Index struct {
	Key  []string
	Unique bool
	DropDups bool
	Background bool
	Sparse bool
	ExpireAfter time.Duration
	Name string
	Min, Max   int
	Minf, Maxf float64
	BucketSize float64
	Bits       int
	DefaultLanguage  string
	LanguageOverride string
	Weights map[string]int
	Collation *Collation
}

type Collation struct {
	Locale string `bson:"locale"`
	CaseLevel bool `bson:"caseLevel,omitempty"`
	CaseFirst string `bson:"caseFirst,omitempty"`
	Strength int `bson:"strength,omitempty"`
	NumericOrdering bool `bson:"numericOrdering,omitempty"`
	Alternate string `bson:"alternate,omitempty"`
	Backwards bool `bson:"backwards,omitempty"`
}

type indexKeyInfo struct {
	name    string
	key     bson.D
	weights bson.D
}

func parseIndexKey(key []string) (*indexKeyInfo, error) {
	var keyInfo indexKeyInfo
	isText := false
	var order interface{}
	for _, field := range key {
		raw := field
		if keyInfo.name != "" {
			keyInfo.name += "_"
		}
		var kind string
		if field != "" {
			if field[0] == '$' {
				if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
					kind = field[1:c]
					field = field[c+1:]
					keyInfo.name += field + "_" + kind
				} else {
					field = "\x00"
				}
			}
			switch field[0] {
			case 0:
				field = ""
			case '@':
				order = "2d"
				field = field[1:]
				keyInfo.name += field + "_2d"
			case '-':
				order = -1
				field = field[1:]
				keyInfo.name += field + "_-1"
			case '+':
				field = field[1:]
				fallthrough
			default:
				if kind == "" {
					order = 1
					keyInfo.name += field + "_1"
				} else {
					order = kind
				}
			}
		}
		if field == "" || kind != "" && order != kind {
			return nil, fmt.Errorf(`invalid index key: want "[$<kind>:][-]<field name>", got %q`, raw)
		}
		if kind == "text" {
			if !isText {
				keyInfo.key = append(keyInfo.key, bson.DocElem{"_fts", "text"}, bson.DocElem{"_ftsx", 1})
				isText = true
			}
			keyInfo.weights = append(keyInfo.weights, bson.DocElem{field, 1})
		} else {
			keyInfo.key = append(keyInfo.key, bson.DocElem{field, order})
		}
	}
	if keyInfo.name == "" {
		return nil, errors.New("invalid index key: no fields provided")
	}
	return &keyInfo, nil
}

func (c *Collection) EnsureIndexKey(key ...string) error {
	return c.EnsureIndex(Index{Key: key})
}

func (c *Collection) EnsureIndex(index Index) error {
	keyInfo, err := parseIndexKey(index.Key)
	if err != nil {
		return err
	}

	session := c.Database.Session
	cacheKey := c.FullName + "\x00" + keyInfo.name
	if session.cluster().HasCachedIndex(cacheKey) {
		return nil
	}

	spec := indexSpec{
		Name:             keyInfo.name,
		NS:               c.FullName,
		Key:              keyInfo.key,
		Unique:           index.Unique,
		DropDups:         index.DropDups,
		Background:       index.Background,
		Sparse:           index.Sparse,
		Bits:             index.Bits,
		Min:              index.Minf,
		Max:              index.Maxf,
		BucketSize:       index.BucketSize,
		ExpireAfter:      int(index.ExpireAfter / time.Second),
		Weights:          keyInfo.weights,
		DefaultLanguage:  index.DefaultLanguage,
		LanguageOverride: index.LanguageOverride,
		Collation:        index.Collation,
	}

	if spec.Min == 0 && spec.Max == 0 {
		spec.Min = float64(index.Min)
		spec.Max = float64(index.Max)
	}

	if index.Name != "" {
		spec.Name = index.Name
	}

NextField:
	for name, weight := range index.Weights {
		for i, elem := range spec.Weights {
			if elem.Name == name {
				spec.Weights[i].Value = weight
				continue NextField
			}
		}
		panic("weight provided for field that is not part of index key: " + name)
	}

	cloned := session.Clone()
	defer cloned.Close()
	cloned.SetMode(Strong, false)
	cloned.EnsureSafe(&Safe{})
	db := c.Database.With(cloned)

	err = db.Run(bson.D{{"createIndexes", c.Name}, {"indexes", []indexSpec{spec}}}, nil)
	if isNoCmd(err) {
		err = db.C("system.indexes").Insert(&spec)
	}
	if err == nil {
		session.cluster().CacheIndex(cacheKey, true)
	}
	return err
}

func (c *Collection) DropIndex(key ...string) error {
	keyInfo, err := parseIndexKey(key)
	if err != nil {
		return err
	}

	session := c.Database.Session
	cacheKey := c.FullName + "\x00" + keyInfo.name
	session.cluster().CacheIndex(cacheKey, false)

	session = session.Clone()
	defer session.Close()
	session.SetMode(Strong, false)

	db := c.Database.With(session)
	result := struct {
		ErrMsg string
		Ok     bool
	}{}
	err = db.Run(bson.D{{"dropIndexes", c.Name}, {"index", keyInfo.name}}, &result)
	if err != nil {
		return err
	}
	if !result.Ok {
		return errors.New(result.ErrMsg)
	}
	return nil
}

func (c *Collection) DropIndexName(name string) error {
	session := c.Database.Session

	session = session.Clone()
	defer session.Close()
	session.SetMode(Strong, false)

	c = c.With(session)

	indexes, err := c.Indexes()
	if err != nil {
		return err
	}

	var index Index
	for _, idx := range indexes {
		if idx.Name == name {
			index = idx
			break
		}
	}

	if index.Name != "" {
		keyInfo, err := parseIndexKey(index.Key)
		if err != nil {
			return err
		}

		cacheKey := c.FullName + "\x00" + keyInfo.name
		session.cluster().CacheIndex(cacheKey, false)
	}

	result := struct {
		ErrMsg string
		Ok     bool
	}{}
	err = c.Database.Run(bson.D{{"dropIndexes", c.Name}, {"index", name}}, &result)
	if err != nil {
		return err
	}
	if !result.Ok {
		return errors.New(result.ErrMsg)
	}
	return nil
}

func (session *Session) nonEventual() *Session {
	cloned := session.Clone()
	if cloned.consistency == Eventual {
		cloned.SetMode(Monotonic, false)
	}
	return cloned
}

func (c *Collection) Indexes() (indexes []Index, err error) {
	cloned := c.Database.Session.nonEventual()
	defer cloned.Close()

	batchSize := int(cloned.queryConfig.op.limit)

	var result struct {
		Indexes []bson.Raw
		Cursor  cursorData
	}
	var iter *Iter
	err = c.Database.With(cloned).Run(bson.D{{"listIndexes", c.Name}, {"cursor", bson.D{{"batchSize", batchSize}}}}, &result)
	if err == nil {
		firstBatch := result.Indexes
		if firstBatch == nil {
			firstBatch = result.Cursor.FirstBatch
		}
		ns := strings.SplitN(result.Cursor.NS, ".", 2)
		if len(ns) < 2 {
			iter = c.With(cloned).NewIter(nil, firstBatch, result.Cursor.Id, nil)
		} else {
			iter = cloned.DB(ns[0]).C(ns[1]).NewIter(nil, firstBatch, result.Cursor.Id, nil)
		}
	} else if isNoCmd(err) {
		iter = c.Database.C("system.indexes").Find(bson.M{"ns": c.FullName}).Iter()
	} else {
		return nil, err
	}

	var spec indexSpec
	for iter.Next(&spec) {
		indexes = append(indexes, indexFromSpec(spec))
	}
	if err = iter.Close(); err != nil {
		return nil, err
	}
	sort.Sort(indexSlice(indexes))
	return indexes, nil
}

func indexFromSpec(spec indexSpec) Index {
	index := Index{
		Name:             spec.Name,
		Key:              simpleIndexKey(spec.Key),
		Unique:           spec.Unique,
		DropDups:         spec.DropDups,
		Background:       spec.Background,
		Sparse:           spec.Sparse,
		Minf:             spec.Min,
		Maxf:             spec.Max,
		Bits:             spec.Bits,
		BucketSize:       spec.BucketSize,
		DefaultLanguage:  spec.DefaultLanguage,
		LanguageOverride: spec.LanguageOverride,
		ExpireAfter:      time.Duration(spec.ExpireAfter) * time.Second,
		Collation:        spec.Collation,
	}
	if float64(int(spec.Min)) == spec.Min && float64(int(spec.Max)) == spec.Max {
		index.Min = int(spec.Min)
		index.Max = int(spec.Max)
	}
	if spec.TextIndexVersion > 0 {
		index.Key = make([]string, len(spec.Weights))
		index.Weights = make(map[string]int)
		for i, elem := range spec.Weights {
			index.Key[i] = "$text:" + elem.Name
			if w, ok := elem.Value.(int); ok {
				index.Weights[elem.Name] = w
			}
		}
	}
	return index
}

type indexSlice []Index

func (idxs indexSlice) Len() int           { return len(idxs) }
func (idxs indexSlice) Less(i, j int) bool { return idxs[i].Name < idxs[j].Name }
func (idxs indexSlice) Swap(i, j int)      { idxs[i], idxs[j] = idxs[j], idxs[i] }

func simpleIndexKey(realKey bson.D) (key []string) {
	for i := range realKey {
		var vi int
		field := realKey[i].Name

		switch realKey[i].Value.(type) {
		case int64:
			vf, _ := realKey[i].Value.(int64)
			vi = int(vf)
		case float64:
			vf, _ := realKey[i].Value.(float64)
			vi = int(vf)
		case string:
			if vs, ok := realKey[i].Value.(string); ok {
				key = append(key, "$"+vs+":"+field)
				continue
			}
		case int:
			vi = realKey[i].Value.(int)
		}

		if vi == 1 {
			key = append(key, field)
			continue
		}
		if vi == -1 {
			key = append(key, "-"+field)
			continue
		}
		panic("Got unknown index key type for field " + field)
	}
	return
}

func (s *Session) ResetIndexCache() {
	s.cluster().ResetIndexCache()
}

func (s *Session) New() *Session {
	s.m.Lock()
	scopy := copySession(s, false)
	s.m.Unlock()
	scopy.Refresh()
	return scopy
}

func (s *Session) Copy() *Session {
	s.m.Lock()
	scopy := copySession(s, true)
	s.m.Unlock()
	scopy.Refresh()
	return scopy
}

func (s *Session) Clone() *Session {
	s.m.Lock()
	scopy := copySession(s, true)
	s.m.Unlock()
	return scopy
}

func (s *Session) Close() {
	s.m.Lock()
	if s.cluster_ != nil {
		s.unsetSocket()
		s.cluster_.Release()
		s.cluster_ = nil
	}
	s.m.Unlock()
}

func (s *Session) cluster() *mongoCluster {
	if s.cluster_ == nil {
		panic("Session already closed")
	}
	return s.cluster_
}

func (s *Session) Refresh() {
	s.m.Lock()
	s.slaveOk = s.consistency != Strong
	s.unsetSocket()
	s.m.Unlock()
}

func (s *Session) SetMode(consistency Mode, refresh bool) {
	s.m.Lock()
	s.consistency = consistency
	if refresh {
		s.slaveOk = s.consistency != Strong
		s.unsetSocket()
	} else if s.consistency == Strong {
		s.slaveOk = false
	} else if s.masterSocket == nil {
		s.slaveOk = true
	}
	s.m.Unlock()
}

func (s *Session) Mode() Mode {
	s.m.RLock()
	mode := s.consistency
	s.m.RUnlock()
	return mode
}

func (s *Session) SetSyncTimeout(d time.Duration) {
	s.m.Lock()
	s.syncTimeout = d
	s.m.Unlock()
}

func (s *Session) SetSocketTimeout(d time.Duration) {
	s.m.Lock()
	s.sockTimeout = d
	if s.masterSocket != nil {
		s.masterSocket.SetTimeout(d)
	}
	if s.slaveSocket != nil {
		s.slaveSocket.SetTimeout(d)
	}
	s.m.Unlock()
}

func (s *Session) SetCursorTimeout(d time.Duration) {
	s.m.Lock()
	if d == 0 {
		s.queryConfig.op.flags |= flagNoCursorTimeout
	} else {
		panic("SetCursorTimeout: only 0 (disable timeout) supported for now")
	}
	s.m.Unlock()
}

func (s *Session) SetPoolLimit(limit int) {
	s.m.Lock()
	s.poolLimit = limit
	s.m.Unlock()
}

func (s *Session) SetBypassValidation(bypass bool) {
	s.m.Lock()
	s.bypassValidation = bypass
	s.m.Unlock()
}

func (s *Session) SetBatch(n int) {
	if n == 1 {
		n = 2
	}
	s.m.Lock()
	s.queryConfig.op.limit = int32(n)
	s.m.Unlock()
}

func (s *Session) SetPrefetch(p float64) {
	s.m.Lock()
	s.queryConfig.prefetch = p
	s.m.Unlock()
}

type Safe struct {
	W        int
	WMode    string
	WTimeout int
	FSync    bool
	J        bool
}

func (s *Session) Safe() (safe *Safe) {
	s.m.Lock()
	defer s.m.Unlock()
	if s.safeOp != nil {
		cmd := s.safeOp.query.(*getLastError)
		safe = &Safe{WTimeout: cmd.WTimeout, FSync: cmd.FSync, J: cmd.J}
		switch w := cmd.W.(type) {
		case string:
			safe.WMode = w
		case int:
			safe.W = w
		}
	}
	return
}

func (s *Session) SetSafe(safe *Safe) {
	s.m.Lock()
	s.safeOp = nil
	s.ensureSafe(safe)
	s.m.Unlock()
}

func (s *Session) EnsureSafe(safe *Safe) {
	s.m.Lock()
	s.ensureSafe(safe)
	s.m.Unlock()
}

func (s *Session) ensureSafe(safe *Safe) {
	if safe == nil {
		return
	}

	var w interface{}
	if safe.WMode != "" {
		w = safe.WMode
	} else if safe.W > 0 {
		w = safe.W
	}

	var cmd getLastError
	if s.safeOp == nil {
		cmd = getLastError{1, w, safe.WTimeout, safe.FSync, safe.J}
	} else {
		cmd = *(s.safeOp.query.(*getLastError))
		if cmd.W == nil {
			cmd.W = w
		} else if safe.WMode != "" {
			cmd.W = safe.WMode
		} else if i, ok := cmd.W.(int); ok && safe.W > i {
			cmd.W = safe.W
		}
		if safe.WTimeout > 0 && safe.WTimeout < cmd.WTimeout {
			cmd.WTimeout = safe.WTimeout
		}
		if safe.FSync {
			cmd.FSync = true
			cmd.J = false
		} else if safe.J && !cmd.FSync {
			cmd.J = true
		}
	}
	s.safeOp = &queryOp{
		query:      &cmd,
		collection: "admin.$cmd",
		limit:      -1,
	}
}

func (s *Session) Run(cmd interface{}, result interface{}) error {
	return s.DB("admin").Run(cmd, result)
}

func (s *Session) SelectServers(tags ...bson.D) {
	s.m.Lock()
	s.queryConfig.op.serverTags = tags
	s.m.Unlock()
}

func (s *Session) Ping() error {
	return s.Run("ping", nil)
}

func (s *Session) Fsync(async bool) error {
	return s.Run(bson.D{{"fsync", 1}, {"async", async}}, nil)
}

func (s *Session) FsyncLock() error {
	return s.Run(bson.D{{"fsync", 1}, {"lock", true}}, nil)
}

func (s *Session) FsyncUnlock() error {
	err := s.Run(bson.D{{"fsyncUnlock", 1}}, nil)
	if isNoCmd(err) {
		err = s.DB("admin").C("$cmd.sys.unlock").Find(nil).One(nil)
	}
	return err
}

func (c *Collection) Find(query interface{}) *Query {
	session := c.Database.Session
	session.m.RLock()
	q := &Query{session: session, query: session.queryConfig}
	session.m.RUnlock()
	q.op.query = query
	q.op.collection = c.FullName
	return q
}

type repairCmd struct {
	RepairCursor string           `bson:"repairCursor"`
	Cursor       *repairCmdCursor ",omitempty"
}

type repairCmdCursor struct {
	BatchSize int `bson:"batchSize,omitempty"`
}

func (c *Collection) Repair() *Iter {
	session := c.Database.Session
	cloned := session.nonEventual()
	defer cloned.Close()

	batchSize := int(cloned.queryConfig.op.limit)

	var result struct{ Cursor cursorData }

	cmd := repairCmd{
		RepairCursor: c.Name,
		Cursor:       &repairCmdCursor{batchSize},
	}

	clonedc := c.With(cloned)
	err := clonedc.Database.Run(cmd, &result)
	return clonedc.NewIter(session, result.Cursor.FirstBatch, result.Cursor.Id, err)
}

func (c *Collection) FindId(id interface{}) *Query {
	return c.Find(bson.D{{"_id", id}})
}

type Pipe struct {
	session    *Session
	collection *Collection
	pipeline   interface{}
	allowDisk  bool
	batchSize  int
}

type pipeCmd struct {
	Aggregate string
	Pipeline  interface{}
	Cursor    *pipeCmdCursor ",omitempty"
	Explain   bool           ",omitempty"
	AllowDisk bool           "allowDiskUse,omitempty"
}

type pipeCmdCursor struct {
	BatchSize int `bson:"batchSize,omitempty"`
}

func (c *Collection) Pipe(pipeline interface{}) *Pipe {
	session := c.Database.Session
	session.m.RLock()
	batchSize := int(session.queryConfig.op.limit)
	session.m.RUnlock()
	return &Pipe{
		session:    session,
		collection: c,
		pipeline:   pipeline,
		batchSize:  batchSize,
	}
}

func (p *Pipe) Iter() *Iter {
	cloned := p.session.nonEventual()
	defer cloned.Close()
	c := p.collection.With(cloned)

	var result struct {
		Result []bson.Raw
		Cursor cursorData
	}

	cmd := pipeCmd{
		Aggregate: c.Name,
		Pipeline:  p.pipeline,
		AllowDisk: p.allowDisk,
		Cursor:    &pipeCmdCursor{p.batchSize},
	}
	err := c.Database.Run(cmd, &result)
	if e, ok := err.(*QueryError); ok && e.Message == `unrecognized field "cursor` {
		cmd.Cursor = nil
		cmd.AllowDisk = false
		err = c.Database.Run(cmd, &result)
	}
	firstBatch := result.Result
	if firstBatch == nil {
		firstBatch = result.Cursor.FirstBatch
	}
	return c.NewIter(p.session, firstBatch, result.Cursor.Id, err)
}

func (c *Collection) NewIter(session *Session, firstBatch []bson.Raw, cursorId int64, err error) *Iter {
	var server *mongoServer
	csession := c.Database.Session
	csession.m.RLock()
	socket := csession.masterSocket
	if socket == nil {
		socket = csession.slaveSocket
	}
	if socket != nil {
		server = socket.Server()
	}
	csession.m.RUnlock()

	if server == nil {
		if csession.Mode() == Eventual {
			panic("Collection.NewIter called in Eventual mode")
		}
		if err == nil {
			err = errors.New("server not available")
		}
	}

	if session == nil {
		session = csession
	}

	iter := &Iter{
		session: session,
		server:  server,
		timeout: -1,
		err:     err,
	}
	iter.gotReply.L = &iter.m
	for _, doc := range firstBatch {
		iter.docData.Push(doc.Data)
	}
	if cursorId != 0 {
		iter.op.cursorId = cursorId
		iter.op.collection = c.FullName
		iter.op.replyFunc = iter.replyFunc()
	}
	return iter
}

func (p *Pipe) All(result interface{}) error {
	return p.Iter().All(result)
}

func (p *Pipe) One(result interface{}) error {
	iter := p.Iter()
	if iter.Next(result) {
		return nil
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return ErrNotFound
}

func (p *Pipe) Explain(result interface{}) error {
	c := p.collection
	cmd := pipeCmd{
		Aggregate: c.Name,
		Pipeline:  p.pipeline,
		AllowDisk: p.allowDisk,
		Explain:   true,
	}
	return c.Database.Run(cmd, result)
}

func (p *Pipe) AllowDiskUse() *Pipe {
	p.allowDisk = true
	return p
}

func (p *Pipe) Batch(n int) *Pipe {
	p.batchSize = n
	return p
}

type LastError struct {
	Err             string
	Code, N, Waited int
	FSyncFiles      int `bson:"fsyncFiles"`
	WTimeout        bool
	UpdatedExisting bool        `bson:"updatedExisting"`
	UpsertedId      interface{} `bson:"upserted"`

	modified int
	ecases   []BulkErrorCase
}

func (err *LastError) Error() string {
	return err.Err
}

type queryError struct {
	Err           string "$err"
	ErrMsg        string
	Assertion     string
	Code          int
	AssertionCode int "assertionCode"
}

type QueryError struct {
	Code      int
	Message   string
	Assertion bool
}

func (err *QueryError) Error() string {
	return err.Message
}

func IsDup(err error) bool {
	switch e := err.(type) {
	case *LastError:
		return e.Code == 11000 || e.Code == 11001 || e.Code == 12582 || e.Code == 16460 && strings.Contains(e.Err, " E11000 ")
	case *QueryError:
		return e.Code == 11000 || e.Code == 11001 || e.Code == 12582
	case *BulkError:
		for _, ecase := range e.ecases {
			if !IsDup(ecase.Err) {
				return false
			}
		}
		return true
	}
	return false
}

func (c *Collection) Insert(docs ...interface{}) error {
	_, err := c.writeOp(&insertOp{c.FullName, docs, 0}, true)
	return err
}

func (c *Collection) Update(selector interface{}, update interface{}) error {
	if selector == nil {
		selector = bson.D{}
	}
	op := updateOp{
		Collection: c.FullName,
		Selector:   selector,
		Update:     update,
	}
	lerr, err := c.writeOp(&op, true)
	if err == nil && lerr != nil && !lerr.UpdatedExisting {
		return ErrNotFound
	}
	return err
}

func (c *Collection) UpdateId(id interface{}, update interface{}) error {
	return c.Update(bson.D{{"_id", id}}, update)
}

type ChangeInfo struct {
	Updated    int
	Removed    int
	Matched    int
	UpsertedId interface{}
}

func (c *Collection) UpdateAll(selector interface{}, update interface{}) (info *ChangeInfo, err error) {
	if selector == nil {
		selector = bson.D{}
	}
	op := updateOp{
		Collection: c.FullName,
		Selector:   selector,
		Update:     update,
		Flags:      2,
		Multi:      true,
	}
	lerr, err := c.writeOp(&op, true)
	if err == nil && lerr != nil {
		info = &ChangeInfo{Updated: lerr.modified, Matched: lerr.N}
	}
	return info, err
}

func (c *Collection) Upsert(selector interface{}, update interface{}) (info *ChangeInfo, err error) {
	if selector == nil {
		selector = bson.D{}
	}
	op := updateOp{
		Collection: c.FullName,
		Selector:   selector,
		Update:     update,
		Flags:      1,
		Upsert:     true,
	}
	var lerr *LastError
	for i := 0; i < maxUpsertRetries; i++ {
		lerr, err = c.writeOp(&op, true)
		if !IsDup(err) {
			break
		}
	}
	if err == nil && lerr != nil {
		info = &ChangeInfo{}
		if lerr.UpdatedExisting {
			info.Matched = lerr.N
			info.Updated = lerr.modified
		} else {
			info.UpsertedId = lerr.UpsertedId
		}
	}
	return info, err
}

func (c *Collection) UpsertId(id interface{}, update interface{}) (info *ChangeInfo, err error) {
	return c.Upsert(bson.D{{"_id", id}}, update)
}

func (c *Collection) Remove(selector interface{}) error {
	if selector == nil {
		selector = bson.D{}
	}
	lerr, err := c.writeOp(&deleteOp{c.FullName, selector, 1, 1}, true)
	if err == nil && lerr != nil && lerr.N == 0 {
		return ErrNotFound
	}
	return err
}

func (c *Collection) RemoveId(id interface{}) error {
	return c.Remove(bson.D{{"_id", id}})
}

func (c *Collection) RemoveAll(selector interface{}) (info *ChangeInfo, err error) {
	if selector == nil {
		selector = bson.D{}
	}
	lerr, err := c.writeOp(&deleteOp{c.FullName, selector, 0, 0}, true)
	if err == nil && lerr != nil {
		info = &ChangeInfo{Removed: lerr.N, Matched: lerr.N}
	}
	return info, err
}

func (db *Database) DropDatabase() error {
	return db.Run(bson.D{{"dropDatabase", 1}}, nil)
}

func (c *Collection) DropCollection() error {
	return c.Database.Run(bson.D{{"drop", c.Name}}, nil)
}

type CollectionInfo struct {
	DisableIdIndex bool
	ForceIdIndex bool
	Capped   bool
	MaxBytes int
	MaxDocs  int
	Validator interface{}
	ValidationLevel string
	ValidationAction string
	StorageEngine interface{}
}

func (c *Collection) Create(info *CollectionInfo) error {
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{"create", c.Name})
	if info.Capped {
		if info.MaxBytes < 1 {
			return fmt.Errorf("Collection.Create: with Capped, MaxBytes must also be set")
		}
		cmd = append(cmd, bson.DocElem{"capped", true})
		cmd = append(cmd, bson.DocElem{"size", info.MaxBytes})
		if info.MaxDocs > 0 {
			cmd = append(cmd, bson.DocElem{"max", info.MaxDocs})
		}
	}
	if info.DisableIdIndex {
		cmd = append(cmd, bson.DocElem{"autoIndexId", false})
	}
	if info.ForceIdIndex {
		cmd = append(cmd, bson.DocElem{"autoIndexId", true})
	}
	if info.Validator != nil {
		cmd = append(cmd, bson.DocElem{"validator", info.Validator})
	}
	if info.ValidationLevel != "" {
		cmd = append(cmd, bson.DocElem{"validationLevel", info.ValidationLevel})
	}
	if info.ValidationAction != "" {
		cmd = append(cmd, bson.DocElem{"validationAction", info.ValidationAction})
	}
	if info.StorageEngine != nil {
		cmd = append(cmd, bson.DocElem{"storageEngine", info.StorageEngine})
	}
	return c.Database.Run(cmd, nil)
}

func (q *Query) Batch(n int) *Query {
	if n == 1 {
		n = 2
	}
	q.m.Lock()
	q.op.limit = int32(n)
	q.m.Unlock()
	return q
}

func (q *Query) Prefetch(p float64) *Query {
	q.m.Lock()
	q.prefetch = p
	q.m.Unlock()
	return q
}

func (q *Query) Skip(n int) *Query {
	q.m.Lock()
	q.op.skip = int32(n)
	q.m.Unlock()
	return q
}

func (q *Query) Limit(n int) *Query {
	q.m.Lock()
	switch {
	case n == 1:
		q.limit = 1
		q.op.limit = -1
	case n == math.MinInt32:
		q.limit = math.MaxInt32
		q.op.limit = math.MinInt32 + 1
	case n < 0:
		q.limit = int32(-n)
		q.op.limit = int32(n)
	default:
		q.limit = int32(n)
		q.op.limit = int32(n)
	}
	q.m.Unlock()
	return q
}

func (q *Query) Select(selector interface{}) *Query {
	q.m.Lock()
	q.op.selector = selector
	q.m.Unlock()
	return q
}

func (q *Query) Sort(fields ...string) *Query {
	q.m.Lock()
	var order bson.D
	for _, field := range fields {
		n := 1
		var kind string
		if field != "" {
			if field[0] == '$' {
				if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
					kind = field[1:c]
					field = field[c+1:]
				}
			}
			switch field[0] {
			case '+':
				field = field[1:]
			case '-':
				n = -1
				field = field[1:]
			}
		}
		if field == "" {
			panic("Sort: empty field name")
		}
		if kind == "textScore" {
			order = append(order, bson.DocElem{field, bson.M{"$meta": kind}})
		} else {
			order = append(order, bson.DocElem{field, n})
		}
	}
	q.op.options.OrderBy = order
	q.op.hasOptions = true
	q.m.Unlock()
	return q
}

func (q *Query) Explain(result interface{}) error {
	q.m.Lock()
	clone := &Query{session: q.session, query: q.query}
	q.m.Unlock()
	clone.op.options.Explain = true
	clone.op.hasOptions = true
	if clone.op.limit > 0 {
		clone.op.limit = -q.op.limit
	}
	iter := clone.Iter()
	if iter.Next(result) {
		return nil
	}
	return iter.Close()
}

func (q *Query) Hint(indexKey ...string) *Query {
	q.m.Lock()
	keyInfo, err := parseIndexKey(indexKey)
	q.op.options.Hint = keyInfo.key
	q.op.hasOptions = true
	q.m.Unlock()
	if err != nil {
		panic(err)
	}
	return q
}

func (q *Query) SetMaxScan(n int) *Query {
	q.m.Lock()
	q.op.options.MaxScan = n
	q.op.hasOptions = true
	q.m.Unlock()
	return q
}

func (q *Query) SetMaxTime(d time.Duration) *Query {
	q.m.Lock()
	q.op.options.MaxTimeMS = int(d / time.Millisecond)
	q.op.hasOptions = true
	q.m.Unlock()
	return q
}

func (q *Query) Snapshot() *Query {
	q.m.Lock()
	q.op.options.Snapshot = true
	q.op.hasOptions = true
	q.m.Unlock()
	return q
}

func (q *Query) Comment(comment string) *Query {
	q.m.Lock()
	q.op.options.Comment = comment
	q.op.hasOptions = true
	q.m.Unlock()
	return q
}

func (q *Query) LogReplay() *Query {
	q.m.Lock()
	q.op.flags |= flagLogReplay
	q.m.Unlock()
	return q
}

func checkQueryError(fullname string, d []byte) error {
	l := len(d)
	if l < 16 {
		return nil
	}
	if d[5] == '$' && d[6] == 'e' && d[7] == 'r' && d[8] == 'r' && d[9] == '\x00' && d[4] == '\x02' {
		goto Error
	}
	if len(fullname) < 5 || fullname[len(fullname)-5:] != ".$cmd" {
		return nil
	}
	for i := 0; i+8 < l; i++ {
		if d[i] == '\x02' && d[i+1] == 'e' && d[i+2] == 'r' && d[i+3] == 'r' && d[i+4] == 'm' && d[i+5] == 's' && d[i+6] == 'g' && d[i+7] == '\x00' {
			goto Error
		}
	}
	return nil

Error:
	result := &queryError{}
	bson.Unmarshal(d, result)
	if result.Err == "" && result.ErrMsg == "" {
		return nil
	}
	if result.AssertionCode != 0 && result.Assertion != "" {
		return &QueryError{Code: result.AssertionCode, Message: result.Assertion, Assertion: true}
	}
	if result.Err != "" {
		return &QueryError{Code: result.Code, Message: result.Err}
	}
	return &QueryError{Code: result.Code, Message: result.ErrMsg}
}

func (q *Query) One(result interface{}) (err error) {
	q.m.Lock()
	session := q.session
	op := q.op
	q.m.Unlock()

	socket, err := session.acquireSocket(true)
	if err != nil {
		return err
	}
	defer socket.Release()

	op.limit = -1

	session.prepareQuery(&op)

	expectFindReply := prepareFindOp(socket, &op, 1)

	data, err := socket.SimpleQuery(&op)
	if err != nil {
		return err
	}
	if data == nil {
		return ErrNotFound
	}
	if expectFindReply {
		var findReply struct {
			Ok     bool
			Code   int
			Errmsg string
			Cursor cursorData
		}
		err = bson.Unmarshal(data, &findReply)
		if err != nil {
			return err
		}
		if !findReply.Ok && findReply.Errmsg != "" {
			return &QueryError{Code: findReply.Code, Message: findReply.Errmsg}
		}
		if len(findReply.Cursor.FirstBatch) == 0 {
			return ErrNotFound
		}
		data = findReply.Cursor.FirstBatch[0].Data
	}
	if result != nil {
		err = bson.Unmarshal(data, result)
		if err != nil {
			return err
		}
	}
	return checkQueryError(op.collection, data)
}

func prepareFindOp(socket *mongoSocket, op *queryOp, limit int32) bool {
	if socket.ServerInfo().MaxWireVersion < 4 || op.collection == "admin.$cmd" {
		return false
	}

	nameDot := strings.Index(op.collection, ".")
	if nameDot < 0 {
		panic("invalid query collection name: " + op.collection)
	}

	find := findCmd{
		Collection:  op.collection[nameDot+1:],
		Filter:      op.query,
		Projection:  op.selector,
		Sort:        op.options.OrderBy,
		Skip:        op.skip,
		Limit:       limit,
		MaxTimeMS:   op.options.MaxTimeMS,
		MaxScan:     op.options.MaxScan,
		Hint:        op.options.Hint,
		Comment:     op.options.Comment,
		Snapshot:    op.options.Snapshot,
		OplogReplay: op.flags&flagLogReplay != 0,
	}
	if op.limit < 0 {
		find.BatchSize = -op.limit
		find.SingleBatch = true
	} else {
		find.BatchSize = op.limit
	}

	explain := op.options.Explain

	op.collection = op.collection[:nameDot] + ".$cmd"
	op.query = &find
	op.skip = 0
	op.limit = -1
	op.options = queryWrapper{}
	op.hasOptions = false

	if explain {
		op.query = bson.D{{"explain", op.query}}
		return false
	}
	return true
}

type cursorData struct {
	FirstBatch []bson.Raw "firstBatch"
	NextBatch  []bson.Raw "nextBatch"
	NS         string
	Id         int64
}

type findCmd struct {
	Collection          string      `bson:"find"`
	Filter              interface{} `bson:"filter,omitempty"`
	Sort                interface{} `bson:"sort,omitempty"`
	Projection          interface{} `bson:"projection,omitempty"`
	Hint                interface{} `bson:"hint,omitempty"`
	Skip                interface{} `bson:"skip,omitempty"`
	Limit               int32       `bson:"limit,omitempty"`
	BatchSize           int32       `bson:"batchSize,omitempty"`
	SingleBatch         bool        `bson:"singleBatch,omitempty"`
	Comment             string      `bson:"comment,omitempty"`
	MaxScan             int         `bson:"maxScan,omitempty"`
	MaxTimeMS           int         `bson:"maxTimeMS,omitempty"`
	ReadConcern         interface{} `bson:"readConcern,omitempty"`
	Max                 interface{} `bson:"max,omitempty"`
	Min                 interface{} `bson:"min,omitempty"`
	ReturnKey           bool        `bson:"returnKey,omitempty"`
	ShowRecordId        bool        `bson:"showRecordId,omitempty"`
	Snapshot            bool        `bson:"snapshot,omitempty"`
	Tailable            bool        `bson:"tailable,omitempty"`
	AwaitData           bool        `bson:"awaitData,omitempty"`
	OplogReplay         bool        `bson:"oplogReplay,omitempty"`
	NoCursorTimeout     bool        `bson:"noCursorTimeout,omitempty"`
	AllowPartialResults bool        `bson:"allowPartialResults,omitempty"`
}

type getMoreCmd struct {
	CursorId   int64  `bson:"getMore"`
	Collection string `bson:"collection"`
	BatchSize  int32  `bson:"batchSize,omitempty"`
	MaxTimeMS  int64  `bson:"maxTimeMS,omitempty"`
}

func (db *Database) run(socket *mongoSocket, cmd, result interface{}) (err error) {
	if name, ok := cmd.(string); ok {
		cmd = bson.D{{name, 1}}
	}
	session := db.Session
	session.m.RLock()
	op := session.queryConfig.op
	session.m.RUnlock()
	op.query = cmd
	op.collection = db.Name + ".$cmd"

	session.prepareQuery(&op)
	op.limit = -1

	data, err := socket.SimpleQuery(&op)
	if err != nil {
		return err
	}
	if data == nil {
		return ErrNotFound
	}
	if result != nil {
		err = bson.Unmarshal(data, result)
		if err != nil {
			return err
		}
	}
	return checkQueryError(op.collection, data)
}

type DBRef struct {
	Collection string      `bson:"$ref"`
	Id         interface{} `bson:"$id"`
	Database   string      `bson:"$db,omitempty"`
}

func (db *Database) FindRef(ref *DBRef) *Query {
	var c *Collection
	if ref.Database == "" {
		c = db.C(ref.Collection)
	} else {
		c = db.Session.DB(ref.Database).C(ref.Collection)
	}
	return c.FindId(ref.Id)
}

func (s *Session) FindRef(ref *DBRef) *Query {
	if ref.Database == "" {
		panic(errors.New(fmt.Sprintf("Can't resolve database for %#v", ref)))
	}
	c := s.DB(ref.Database).C(ref.Collection)
	return c.FindId(ref.Id)
}

func (db *Database) CollectionNames() (names []string, err error) {
	cloned := db.Session.nonEventual()
	defer cloned.Close()

	batchSize := int(cloned.queryConfig.op.limit)
	var result struct {
		Collections []bson.Raw
		Cursor      cursorData
	}
	err = db.With(cloned).Run(bson.D{{"listCollections", 1}, {"cursor", bson.D{{"batchSize", batchSize}}}}, &result)
	if err == nil {
		firstBatch := result.Collections
		if firstBatch == nil {
			firstBatch = result.Cursor.FirstBatch
		}
		var iter *Iter
		ns := strings.SplitN(result.Cursor.NS, ".", 2)
		if len(ns) < 2 {
			iter = db.With(cloned).C("").NewIter(nil, firstBatch, result.Cursor.Id, nil)
		} else {
			iter = cloned.DB(ns[0]).C(ns[1]).NewIter(nil, firstBatch, result.Cursor.Id, nil)
		}
		var coll struct{ Name string }
		for iter.Next(&coll) {
			names = append(names, coll.Name)
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		sort.Strings(names)
		return names, err
	}
	if err != nil && !isNoCmd(err) {
		return nil, err
	}
	nameIndex := len(db.Name) + 1
	iter := db.C("system.namespaces").Find(nil).Iter()
	var coll struct{ Name string }
	for iter.Next(&coll) {
		if strings.Index(coll.Name, "$") < 0 || strings.Index(coll.Name, ".oplog.$") >= 0 {
			names = append(names, coll.Name[nameIndex:])
		}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

type dbNames struct {
	Databases []struct {
		Name  string
		Empty bool
	}
}

func (s *Session) DatabaseNames() (names []string, err error) {
	var result dbNames
	err = s.Run("listDatabases", &result)
	if err != nil {
		return nil, err
	}
	for _, db := range result.Databases {
		if !db.Empty {
			names = append(names, db.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (q *Query) Iter() *Iter {
	q.m.Lock()
	session := q.session
	op := q.op
	prefetch := q.prefetch
	limit := q.limit
	q.m.Unlock()

	iter := &Iter{
		session:  session,
		prefetch: prefetch,
		limit:    limit,
		timeout:  -1,
	}
	iter.gotReply.L = &iter.m
	iter.op.collection = op.collection
	iter.op.limit = op.limit
	iter.op.replyFunc = iter.replyFunc()
	iter.docsToReceive++

	socket, err := session.acquireSocket(true)
	if err != nil {
		iter.err = err
		return iter
	}
	defer socket.Release()

	session.prepareQuery(&op)
	op.replyFunc = iter.op.replyFunc

	if prepareFindOp(socket, &op, limit) {
		iter.findCmd = true
	}

	iter.server = socket.Server()
	err = socket.Query(&op)
	if err != nil {
		iter.m.Lock()
		iter.err = err
		iter.m.Unlock()
	}

	return iter
}

func (q *Query) Tail(timeout time.Duration) *Iter {
	q.m.Lock()
	session := q.session
	op := q.op
	prefetch := q.prefetch
	q.m.Unlock()

	iter := &Iter{session: session, prefetch: prefetch}
	iter.gotReply.L = &iter.m
	iter.timeout = timeout
	iter.op.collection = op.collection
	iter.op.limit = op.limit
	iter.op.replyFunc = iter.replyFunc()
	iter.docsToReceive++
	session.prepareQuery(&op)
	op.replyFunc = iter.op.replyFunc
	op.flags |= flagTailable | flagAwaitData

	socket, err := session.acquireSocket(true)
	if err != nil {
		iter.err = err
	} else {
		iter.server = socket.Server()
		err = socket.Query(&op)
		if err != nil {
			iter.m.Lock()
			iter.err = err
			iter.m.Unlock()
		}
		socket.Release()
	}
	return iter
}

func (s *Session) prepareQuery(op *queryOp) {
	s.m.RLock()
	op.mode = s.consistency
	if s.slaveOk {
		op.flags |= flagSlaveOk
	}
	s.m.RUnlock()
	return
}

func (iter *Iter) Err() error {
	iter.m.Lock()
	err := iter.err
	iter.m.Unlock()
	if err == ErrNotFound {
		return nil
	}
	return err
}

func (iter *Iter) Close() error {
	iter.m.Lock()
	cursorId := iter.op.cursorId
	iter.op.cursorId = 0
	err := iter.err
	iter.m.Unlock()
	if cursorId == 0 {
		if err == ErrNotFound {
			return nil
		}
		return err
	}
	socket, err := iter.acquireSocket()
	if err == nil {
		err = socket.Query(&killCursorsOp{[]int64{cursorId}})
		socket.Release()
	}

	iter.m.Lock()
	if err != nil && (iter.err == nil || iter.err == ErrNotFound) {
		iter.err = err
	} else if iter.err != ErrNotFound {
		err = iter.err
	}
	iter.m.Unlock()
	return err
}

func (iter *Iter) Done() bool {
	iter.m.Lock()
	defer iter.m.Unlock()

	for {
		if iter.docData.Len() > 0 {
			return false
		}
		if iter.docsToReceive > 1 {
			return true
		}
		if iter.docsToReceive > 0 {
			iter.gotReply.Wait()
			continue
		}
		return iter.op.cursorId == 0
	}
}

func (iter *Iter) Timeout() bool {
	iter.m.Lock()
	result := iter.timedout
	iter.m.Unlock()
	return result
}

func (iter *Iter) Next(result interface{}) bool {
	iter.m.Lock()
	iter.timedout = false
	timeout := time.Time{}
	for iter.err == nil && iter.docData.Len() == 0 && (iter.docsToReceive > 0 || iter.op.cursorId != 0) {
		if iter.docsToReceive == 0 {
			if iter.timeout >= 0 {
				if timeout.IsZero() {
					timeout = time.Now().Add(iter.timeout)
				}
				if time.Now().After(timeout) {
					iter.timedout = true
					iter.m.Unlock()
					return false
				}
			}
			iter.getMore()
			if iter.err != nil {
				break
			}
		}
		iter.gotReply.Wait()
	}

	if docData, ok := iter.docData.Pop().([]byte); ok {
		close := false
		if iter.limit > 0 {
			iter.limit--
			if iter.limit == 0 {
				if iter.docData.Len() > 0 {
					iter.m.Unlock()
					panic(fmt.Errorf("data remains after limit exhausted: %d", iter.docData.Len()))
				}
				iter.err = ErrNotFound
				close = true
			}
		}
		if iter.op.cursorId != 0 && iter.err == nil {
			iter.docsBeforeMore--
			if iter.docsBeforeMore == -1 {
				iter.getMore()
			}
		}
		iter.m.Unlock()

		if close {
			iter.Close()
		}
		err := bson.Unmarshal(docData, result)
		if err != nil {
			iter.m.Lock()
			if iter.err == nil {
				iter.err = err
			}
			iter.m.Unlock()
			return false
		}
		err = checkQueryError(iter.op.collection, docData)
		if err != nil {
			iter.m.Lock()
			if iter.err == nil {
				iter.err = err
			}
			iter.m.Unlock()
			return false
		}
		return true
	} else if iter.err != nil {
		iter.m.Unlock()
		return false
	} else if iter.op.cursorId == 0 {
		iter.err = ErrNotFound
		iter.m.Unlock()
		return false
	}

	panic("unreachable")
}

func (iter *Iter) All(result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	i := 0
	for {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			if !iter.Next(elemp.Interface()) {
				break
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			if !iter.Next(slicev.Index(i).Addr().Interface()) {
				break
			}
		}
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return iter.Close()
}

func (q *Query) All(result interface{}) error {
	return q.Iter().All(result)
}

func (q *Query) For(result interface{}, f func() error) error {
	return q.Iter().For(result, f)
}

func (iter *Iter) For(result interface{}, f func() error) (err error) {
	valid := false
	v := reflect.ValueOf(result)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		switch v.Kind() {
		case reflect.Map, reflect.Ptr, reflect.Interface, reflect.Slice:
			valid = v.IsNil()
		}
	}
	if !valid {
		panic("For needs a pointer to nil reference value.  See the documentation.")
	}
	zero := reflect.Zero(v.Type())
	for {
		v.Set(zero)
		if !iter.Next(result) {
			break
		}
		err = f()
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func (iter *Iter) acquireSocket() (*mongoSocket, error) {
	socket, err := iter.session.acquireSocket(true)
	if err != nil {
		return nil, err
	}
	if socket.Server() != iter.server {
		iter.session.m.Lock()
		sockTimeout := iter.session.sockTimeout
		iter.session.m.Unlock()
		socket.Release()
		socket, _, err = iter.server.AcquireSocket(0, sockTimeout)
		if err != nil {
			return nil, err
		}
		err := iter.session.socketLogin(socket)
		if err != nil {
			socket.Release()
			return nil, err
		}
	}
	return socket, nil
}

func (iter *Iter) getMore() {
	iter.docsToReceive++
	iter.m.Unlock()
	socket, err := iter.acquireSocket()
	iter.m.Lock()
	if err != nil {
		iter.err = err
		return
	}
	defer socket.Release()

	if iter.limit > 0 {
		limit := iter.limit - int32(iter.docsToReceive-1) - int32(iter.docData.Len())
		if limit < iter.op.limit {
			iter.op.limit = limit
		}
	}
	var op interface{}
	if iter.findCmd {
		op = iter.getMoreCmd()
	} else {
		op = &iter.op
	}
	if err := socket.Query(op); err != nil {
		iter.docsToReceive--
		iter.err = err
	}
}

func (iter *Iter) getMoreCmd() *queryOp {
	nameDot := strings.Index(iter.op.collection, ".")
	if nameDot < 0 {
		panic("invalid query collection name: " + iter.op.collection)
	}

	getMore := getMoreCmd{
		CursorId:   iter.op.cursorId,
		Collection: iter.op.collection[nameDot+1:],
		BatchSize:  iter.op.limit,
	}

	var op queryOp
	op.collection = iter.op.collection[:nameDot] + ".$cmd"
	op.query = &getMore
	op.limit = -1
	op.replyFunc = iter.op.replyFunc
	return &op
}

type countCmd struct {
	Count string
	Query interface{}
	Limit int32 ",omitempty"
	Skip  int32 ",omitempty"
}

func (q *Query) Count() (n int, err error) {
	q.m.Lock()
	session := q.session
	op := q.op
	limit := q.limit
	q.m.Unlock()

	c := strings.Index(op.collection, ".")
	if c < 0 {
		return 0, errors.New("Bad collection name: " + op.collection)
	}

	dbname := op.collection[:c]
	cname := op.collection[c+1:]
	query := op.query
	if query == nil {
		query = bson.D{}
	}
	result := struct{ N int }{}
	err = session.DB(dbname).Run(countCmd{cname, query, limit, op.skip}, &result)
	return result.N, err
}

func (c *Collection) Count() (n int, err error) {
	return c.Find(nil).Count()
}

type distinctCmd struct {
	Collection string "distinct"
	Key        string
	Query      interface{} ",omitempty"
}

func (q *Query) Distinct(key string, result interface{}) error {
	q.m.Lock()
	session := q.session
	op := q.op
	q.m.Unlock()

	c := strings.Index(op.collection, ".")
	if c < 0 {
		return errors.New("Bad collection name: " + op.collection)
	}

	dbname := op.collection[:c]
	cname := op.collection[c+1:]

	var doc struct{ Values bson.Raw }
	err := session.DB(dbname).Run(distinctCmd{cname, key, op.query}, &doc)
	if err != nil {
		return err
	}
	return doc.Values.Unmarshal(result)
}

type mapReduceCmd struct {
	Collection string "mapreduce"
	Map        string ",omitempty"
	Reduce     string ",omitempty"
	Finalize   string ",omitempty"
	Limit      int32  ",omitempty"
	Out        interface{}
	Query      interface{} ",omitempty"
	Sort       interface{} ",omitempty"
	Scope      interface{} ",omitempty"
	Verbose    bool        ",omitempty"
}

type mapReduceResult struct {
	Results    bson.Raw
	Result     bson.Raw
	TimeMillis int64 "timeMillis"
	Counts     struct{ Input, Emit, Output int }
	Ok         bool
	Err        string
	Timing     *MapReduceTime
}

type MapReduce struct {
	Map      string
	Reduce   string
	Finalize string
	Out      interface{}
	Scope    interface{}
	Verbose  bool
}

type MapReduceInfo struct {
	InputCount  int
	EmitCount   int
	OutputCount int
	Database    string
	Collection  string
	Time        int64
	VerboseTime *MapReduceTime
}

type MapReduceTime struct {
	Total    int64
	Map      int64 "mapTime"
	EmitLoop int64 "emitLoop"
}

func (q *Query) MapReduce(job *MapReduce, result interface{}) (info *MapReduceInfo, err error) {
	q.m.Lock()
	session := q.session
	op := q.op
	limit := q.limit
	q.m.Unlock()

	c := strings.Index(op.collection, ".")
	if c < 0 {
		return nil, errors.New("Bad collection name: " + op.collection)
	}

	dbname := op.collection[:c]
	cname := op.collection[c+1:]

	cmd := mapReduceCmd{
		Collection: cname,
		Map:        job.Map,
		Reduce:     job.Reduce,
		Finalize:   job.Finalize,
		Out:        fixMROut(job.Out),
		Scope:      job.Scope,
		Verbose:    job.Verbose,
		Query:      op.query,
		Sort:       op.options.OrderBy,
		Limit:      limit,
	}

	if cmd.Out == nil {
		cmd.Out = bson.D{{"inline", 1}}
	}

	var doc mapReduceResult
	err = session.DB(dbname).Run(&cmd, &doc)
	if err != nil {
		return nil, err
	}
	if doc.Err != "" {
		return nil, errors.New(doc.Err)
	}

	info = &MapReduceInfo{
		InputCount:  doc.Counts.Input,
		EmitCount:   doc.Counts.Emit,
		OutputCount: doc.Counts.Output,
		Time:        doc.TimeMillis * 1e6,
	}

	if doc.Result.Kind == 0x02 {
		err = doc.Result.Unmarshal(&info.Collection)
		info.Database = dbname
	} else if doc.Result.Kind == 0x03 {
		var v struct{ Collection, Db string }
		err = doc.Result.Unmarshal(&v)
		info.Collection = v.Collection
		info.Database = v.Db
	}

	if doc.Timing != nil {
		info.VerboseTime = doc.Timing
		info.VerboseTime.Total *= 1e6
		info.VerboseTime.Map *= 1e6
		info.VerboseTime.EmitLoop *= 1e6
	}

	if err != nil {
		return nil, err
	}
	if result != nil {
		return info, doc.Results.Unmarshal(result)
	}
	return info, nil
}

func fixMROut(out interface{}) interface{} {
	outv := reflect.ValueOf(out)
	if outv.Kind() != reflect.Map || outv.Type().Key() != reflect.TypeOf("") {
		return out
	}
	outs := make(bson.D, outv.Len())

	outTypeIndex := -1
	for i, k := range outv.MapKeys() {
		ks := k.String()
		outs[i].Name = ks
		outs[i].Value = outv.MapIndex(k).Interface()
		switch ks {
		case "normal", "replace", "merge", "reduce", "inline":
			outTypeIndex = i
		}
	}
	if outTypeIndex > 0 {
		outs[0], outs[outTypeIndex] = outs[outTypeIndex], outs[0]
	}
	return outs
}

type Change struct {
	Update    interface{}
	Upsert    bool
	Remove    bool
	ReturnNew bool
}

type findModifyCmd struct {
	Collection                  string      "findAndModify"
	Query, Update, Sort, Fields interface{} ",omitempty"
	Upsert, Remove, New         bool        ",omitempty"
}

type valueResult struct {
	Value     bson.Raw
	LastError LastError "lastErrorObject"
}

func (q *Query) Apply(change Change, result interface{}) (info *ChangeInfo, err error) {
	q.m.Lock()
	session := q.session
	op := q.op
	q.m.Unlock()

	c := strings.Index(op.collection, ".")
	if c < 0 {
		return nil, errors.New("bad collection name: " + op.collection)
	}

	dbname := op.collection[:c]
	cname := op.collection[c+1:]

	cmd := findModifyCmd{
		Collection: cname,
		Update:     change.Update,
		Upsert:     change.Upsert,
		Remove:     change.Remove,
		New:        change.ReturnNew,
		Query:      op.query,
		Sort:       op.options.OrderBy,
		Fields:     op.selector,
	}

	session = session.Clone()
	defer session.Close()
	session.SetMode(Strong, false)

	var doc valueResult
	for i := 0; i < maxUpsertRetries; i++ {
		err = session.DB(dbname).Run(&cmd, &doc)
		if err == nil {
			break
		}
		if change.Upsert && IsDup(err) && i+1 < maxUpsertRetries {
			continue
		}
		if qerr, ok := err.(*QueryError); ok && qerr.Message == "No matching object found" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if doc.LastError.N == 0 {
		return nil, ErrNotFound
	}
	if doc.Value.Kind != 0x0A && result != nil {
		err = doc.Value.Unmarshal(result)
		if err != nil {
			return nil, err
		}
	}
	info = &ChangeInfo{}
	lerr := &doc.LastError
	if lerr.UpdatedExisting {
		info.Updated = lerr.N
		info.Matched = lerr.N
	} else if change.Remove {
		info.Removed = lerr.N
		info.Matched = lerr.N
	} else if change.Upsert {
		info.UpsertedId = lerr.UpsertedId
	}
	return info, nil
}

type BuildInfo struct {
	Version        string
	VersionArray   []int  `bson:"versionArray"`
	GitVersion     string `bson:"gitVersion"`
	OpenSSLVersion string `bson:"OpenSSLVersion"`
	SysInfo        string `bson:"sysInfo"`
	Bits           int
	Debug          bool
	MaxObjectSize  int `bson:"maxBsonObjectSize"`
}

func (bi *BuildInfo) VersionAtLeast(version ...int) bool {
	for i, vi := range version {
		if i == len(bi.VersionArray) {
			return false
		}
		if bivi := bi.VersionArray[i]; bivi != vi {
			return bivi >= vi
		}
	}
	return true
}

func (s *Session) BuildInfo() (info BuildInfo, err error) {
	err = s.Run(bson.D{{"buildInfo", "1"}}, &info)
	if len(info.VersionArray) == 0 {
		for _, a := range strings.Split(info.Version, ".") {
			i, err := strconv.Atoi(a)
			if err != nil {
				break
			}
			info.VersionArray = append(info.VersionArray, i)
		}
	}
	for len(info.VersionArray) < 4 {
		info.VersionArray = append(info.VersionArray, 0)
	}
	if i := strings.IndexByte(info.GitVersion, ' '); i >= 0 {
		info.GitVersion = info.GitVersion[:i]
	}
	if info.SysInfo == "deprecated" {
		info.SysInfo = ""
	}
	return
}

func (s *Session) acquireSocket(slaveOk bool) (*mongoSocket, error) {
	s.m.RLock()
	if s.slaveSocket != nil && s.slaveOk && slaveOk && (s.masterSocket == nil || s.consistency != PrimaryPreferred && s.consistency != Monotonic) {
		socket := s.slaveSocket
		socket.Acquire()
		s.m.RUnlock()
		return socket, nil
	}
	if s.masterSocket != nil {
		socket := s.masterSocket
		socket.Acquire()
		s.m.RUnlock()
		return socket, nil
	}
	s.m.RUnlock()

	s.m.Lock()
	defer s.m.Unlock()

	if s.slaveSocket != nil && s.slaveOk && slaveOk && (s.masterSocket == nil || s.consistency != PrimaryPreferred && s.consistency != Monotonic) {
		s.slaveSocket.Acquire()
		return s.slaveSocket, nil
	}
	if s.masterSocket != nil {
		s.masterSocket.Acquire()
		return s.masterSocket, nil
	}

	sock, err := s.cluster().AcquireSocket(s.consistency, slaveOk && s.slaveOk, s.syncTimeout, s.sockTimeout, s.queryConfig.op.serverTags, s.poolLimit)
	if err != nil {
		return nil, err
	}

	if err = s.socketLogin(sock); err != nil {
		sock.Release()
		return nil, err
	}

	if s.consistency != Eventual || s.slaveSocket != nil {
		s.setSocket(sock)
	}

	if !slaveOk && s.consistency == Monotonic {
		s.slaveOk = false
	}

	return sock, nil
}

func (s *Session) setSocket(socket *mongoSocket) {
	info := socket.Acquire()
	if info.Master {
		if s.masterSocket != nil {
			panic("setSocket(master) with existing master socket reserved")
		}
		s.masterSocket = socket
	} else {
		if s.slaveSocket != nil {
			panic("setSocket(slave) with existing slave socket reserved")
		}
		s.slaveSocket = socket
	}
}

func (s *Session) unsetSocket() {
	if s.masterSocket != nil {
		s.masterSocket.Release()
	}
	if s.slaveSocket != nil {
		s.slaveSocket.Release()
	}
	s.masterSocket = nil
	s.slaveSocket = nil
}

func (iter *Iter) replyFunc() replyFunc {
	return func(err error, op *replyOp, docNum int, docData []byte) {
		iter.m.Lock()
		iter.docsToReceive--
		if err != nil {
			iter.err = err
		} else if docNum == -1 {
			if op != nil && op.cursorId != 0 {
				iter.op.cursorId = op.cursorId
			} else if op != nil && op.cursorId == 0 && op.flags&1 == 1 {
				iter.err = ErrCursor
			} else {
				iter.err = ErrNotFound
			}
		} else if iter.findCmd {
			var findReply struct {
				Ok     bool
				Code   int
				Errmsg string
				Cursor cursorData
			}
			if err := bson.Unmarshal(docData, &findReply); err != nil {
				iter.err = err
			} else if !findReply.Ok && findReply.Errmsg != "" {
				iter.err = &QueryError{Code: findReply.Code, Message: findReply.Errmsg}
			} else if len(findReply.Cursor.FirstBatch) == 0 && len(findReply.Cursor.NextBatch) == 0 {
				iter.err = ErrNotFound
			} else {
				batch := findReply.Cursor.FirstBatch
				if len(batch) == 0 {
					batch = findReply.Cursor.NextBatch
				}
				rdocs := len(batch)
				for _, raw := range batch {
					iter.docData.Push(raw.Data)
				}
				iter.docsToReceive = 0
				docsToProcess := iter.docData.Len()
				if iter.limit == 0 || int32(docsToProcess) < iter.limit {
					iter.docsBeforeMore = docsToProcess - int(iter.prefetch*float64(rdocs))
				} else {
					iter.docsBeforeMore = -1
				}
				iter.op.cursorId = findReply.Cursor.Id
			}
		} else {
			rdocs := int(op.replyDocs)
			if docNum == 0 {
				iter.docsToReceive += rdocs - 1
				docsToProcess := iter.docData.Len() + rdocs
				if iter.limit == 0 || int32(docsToProcess) < iter.limit {
					iter.docsBeforeMore = docsToProcess - int(iter.prefetch*float64(rdocs))
				} else {
					iter.docsBeforeMore = -1
				}
				iter.op.cursorId = op.cursorId
			}
			iter.docData.Push(docData)
		}
		iter.gotReply.Broadcast()
		iter.m.Unlock()
	}
}

type writeCmdResult struct {
	Ok        bool
	N         int
	NModified int `bson:"nModified"`
	Upserted  []struct {
		Index int
		Id    interface{} `_id`
	}
	ConcernError writeConcernError `bson:"writeConcernError"`
	Errors       []writeCmdError   `bson:"writeErrors"`
}

type writeConcernError struct {
	Code   int
	ErrMsg string
}

type writeCmdError struct {
	Index  int
	Code   int
	ErrMsg string
}

func (r *writeCmdResult) BulkErrorCases() []BulkErrorCase {
	ecases := make([]BulkErrorCase, len(r.Errors))
	for i, err := range r.Errors {
		ecases[i] = BulkErrorCase{err.Index, &QueryError{Code: err.Code, Message: err.ErrMsg}}
	}
	return ecases
}

func (c *Collection) writeOp(op interface{}, ordered bool) (lerr *LastError, err error) {
	s := c.Database.Session
	socket, err := s.acquireSocket(c.Database.Name == "local")
	if err != nil {
		return nil, err
	}
	defer socket.Release()

	s.m.RLock()
	safeOp := s.safeOp
	bypassValidation := s.bypassValidation
	s.m.RUnlock()

	if socket.ServerInfo().MaxWireVersion >= 2 {
		if op, ok := op.(*insertOp); ok && len(op.documents) > 1000 {
			var lerr LastError
			all := op.documents
			for i := 0; i < len(all); i += 1000 {
				l := i + 1000
				if l > len(all) {
					l = len(all)
				}
				op.documents = all[i:l]
				oplerr, err := c.writeOpCommand(socket, safeOp, op, ordered, bypassValidation)
				lerr.N += oplerr.N
				lerr.modified += oplerr.modified
				if err != nil {
					for ei := range oplerr.ecases {
						oplerr.ecases[ei].Index += i
					}
					lerr.ecases = append(lerr.ecases, oplerr.ecases...)
					if op.flags&1 == 0 {
						return &lerr, err
					}
				}
			}
			if len(lerr.ecases) != 0 {
				return &lerr, lerr.ecases[0].Err
			}
			return &lerr, nil
		}
		return c.writeOpCommand(socket, safeOp, op, ordered, bypassValidation)
	} else if updateOps, ok := op.(bulkUpdateOp); ok {
		var lerr LastError
		for i, updateOp := range updateOps {
			oplerr, err := c.writeOpQuery(socket, safeOp, updateOp, ordered)
			lerr.N += oplerr.N
			lerr.modified += oplerr.modified
			if err != nil {
				lerr.ecases = append(lerr.ecases, BulkErrorCase{i, err})
				if ordered {
					break
				}
			}
		}
		if len(lerr.ecases) != 0 {
			return &lerr, lerr.ecases[0].Err
		}
		return &lerr, nil
	} else if deleteOps, ok := op.(bulkDeleteOp); ok {
		var lerr LastError
		for i, deleteOp := range deleteOps {
			oplerr, err := c.writeOpQuery(socket, safeOp, deleteOp, ordered)
			lerr.N += oplerr.N
			lerr.modified += oplerr.modified
			if err != nil {
				lerr.ecases = append(lerr.ecases, BulkErrorCase{i, err})
				if ordered {
					break
				}
			}
		}
		if len(lerr.ecases) != 0 {
			return &lerr, lerr.ecases[0].Err
		}
		return &lerr, nil
	}
	return c.writeOpQuery(socket, safeOp, op, ordered)
}

func (c *Collection) writeOpQuery(socket *mongoSocket, safeOp *queryOp, op interface{}, ordered bool) (lerr *LastError, err error) {
	if safeOp == nil {
		return nil, socket.Query(op)
	}

	var mutex sync.Mutex
	var replyData []byte
	var replyErr error
	mutex.Lock()
	query := *safeOp
	query.collection = c.Database.Name + ".$cmd"
	query.replyFunc = func(err error, reply *replyOp, docNum int, docData []byte) {
		replyData = docData
		replyErr = err
		mutex.Unlock()
	}
	err = socket.Query(op, &query)
	if err != nil {
		return nil, err
	}
	mutex.Lock()
	if replyErr != nil {
		return nil, replyErr
	}
	if hasErrMsg(replyData) {
		err = checkQueryError(query.collection, replyData)
		if err != nil {
			return nil, err
		}
	}
	result := &LastError{}
	bson.Unmarshal(replyData, &result)
	if result.Err != "" {
		result.ecases = []BulkErrorCase{{Index: 0, Err: result}}
		if insert, ok := op.(*insertOp); ok && len(insert.documents) > 1 {
			result.ecases[0].Index = -1
		}
		return result, result
	}

	result.modified = result.N
	return result, nil
}

func (c *Collection) writeOpCommand(socket *mongoSocket, safeOp *queryOp, op interface{}, ordered, bypassValidation bool) (lerr *LastError, err error) {
	var writeConcern interface{}
	if safeOp == nil {
		writeConcern = bson.D{{"w", 0}}
	} else {
		writeConcern = safeOp.query.(*getLastError)
	}

	var cmd bson.D
	switch op := op.(type) {
	case *insertOp:
		cmd = bson.D{
			{"insert", c.Name},
			{"documents", op.documents},
			{"writeConcern", writeConcern},
			{"ordered", op.flags&1 == 0},
		}
	case *updateOp:
		cmd = bson.D{
			{"update", c.Name},
			{"updates", []interface{}{op}},
			{"writeConcern", writeConcern},
			{"ordered", ordered},
		}
	case bulkUpdateOp:
		cmd = bson.D{
			{"update", c.Name},
			{"updates", op},
			{"writeConcern", writeConcern},
			{"ordered", ordered},
		}
	case *deleteOp:
		cmd = bson.D{
			{"delete", c.Name},
			{"deletes", []interface{}{op}},
			{"writeConcern", writeConcern},
			{"ordered", ordered},
		}
	case bulkDeleteOp:
		cmd = bson.D{
			{"delete", c.Name},
			{"deletes", op},
			{"writeConcern", writeConcern},
			{"ordered", ordered},
		}
	}
	if bypassValidation {
		cmd = append(cmd, bson.DocElem{"bypassDocumentValidation", true})
	}

	var result writeCmdResult
	err = c.Database.run(socket, cmd, &result)
	ecases := result.BulkErrorCases()
	lerr = &LastError{
		UpdatedExisting: result.N > 0 && len(result.Upserted) == 0,
		N:               result.N,

		modified: result.NModified,
		ecases:   ecases,
	}
	if len(result.Upserted) > 0 {
		lerr.UpsertedId = result.Upserted[0].Id
	}
	if len(result.Errors) > 0 {
		e := result.Errors[0]
		lerr.Code = e.Code
		lerr.Err = e.ErrMsg
		err = lerr
	} else if result.ConcernError.Code != 0 {
		e := result.ConcernError
		lerr.Code = e.Code
		lerr.Err = e.ErrMsg
		err = lerr
	}

	if err == nil && safeOp == nil {
		return nil, nil
	}
	return lerr, err
}

func hasErrMsg(d []byte) bool {
	l := len(d)
	for i := 0; i+8 < l; i++ {
		if d[i] == '\x02' && d[i+1] == 'e' && d[i+2] == 'r' && d[i+3] == 'r' && d[i+4] == 'm' && d[i+5] == 's' && d[i+6] == 'g' && d[i+7] == '\x00' {
			return true
		}
	}
	return false
}
