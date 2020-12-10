package tec

import (
	"github.com/agilecho/tec/cache"
	"github.com/agilecho/tec/cron"
	"github.com/agilecho/tec/db"
	"github.com/agilecho/tec/mongo"
	"github.com/agilecho/tec/mq"
	"github.com/agilecho/tec/ws"
	"strconv"
	"strings"
)

type configOfApp struct {
	Name string
	Host string
	Port int
	Token string
	Static string
	Cpu int
	Memory int64
	Debug bool
}

func (this *configOfApp) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "name":
		this.Name = value
	case "host":
		this.Host = value
	case "port":
		this.Port, _ = strconv.Atoi(value)
	case "token":
		this.Token = value
	case "static":
		this.Static = value
	case "cpu":
		this.Cpu, _ = strconv.Atoi(value)
	case "memory":
		this.Memory, _ = strconv.ParseInt(value, 10, 64)
	case "debug":
		this.Debug, _ = strconv.ParseBool(value)
	}
}

type configOfCookie struct {
	Domain string
	Path string
	Expire int
	Secure bool
	HttpOnly bool
	Prefix string
}

func (this *configOfCookie) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "domain":
		this.Domain = value
	case "path":
		this.Path = value
	case "expire":
		this.Expire, _ = strconv.Atoi(value)
	case "secure":
		this.Secure, _ = strconv.ParseBool(value)
	case "httponly":
		this.HttpOnly, _ = strconv.ParseBool(value)
	case "prefix":
		this.Prefix = value
	}
}

type configOfSession struct {
	Type string
	Name string
	Path string
	Expire int
	Prefix string
}

func (this *configOfSession) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "type":
		this.Type = value
	case "name":
		this.Name = value
	case "path":
		this.Path = value
	case "expire":
		this.Expire, _ = strconv.Atoi(value)
	case "prefix":
		this.Prefix = value
	}
}

type configOfTemplate struct {
	Path string
	Extension string
	Cache bool
	Define string
}

func (this *configOfTemplate) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "path":
		this.Path = value
	case "extension":
		this.Extension = value
	case "define":
		this.Define = value
	case "cache":
		this.Cache, _ = strconv.ParseBool(value)
	}
}

type configOfGateway struct {
	Enable bool
	Url string
}

func (this *configOfGateway) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "enable":
		this.Enable, _ = strconv.ParseBool(value)
	case "url":
		this.Url = value
	}
}

type configOfExtend struct {
	data map[string]map[string]string
}

func (this *configOfExtend) Get(section string) map[string]string {
	return this.data[section]
}

type ConfigOfWxApp struct {
	AppId string
	AppSecret string
	MchId string
	MchKey string
	ApiclientCert string
	ApiclientKey string
	Notify string
}

func (this *ConfigOfWxApp) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "appid":
		this.AppId = value
	case "appsecret":
		this.AppSecret = value
	case "mchid":
		this.MchId = value
	case "mchkey":
		this.MchKey = value
	case "apiclient_cert":
		this.ApiclientCert = value
	case "apiclient_key":
		this.ApiclientKey = value
	case "notify":
		this.Notify = value
	}
}

type ConfigOfWeixin struct {
	AppId string
	AppSecret string
	Token string
	EncodingAesKey string
	MchId string
	MchKey string
	ApiclientCert string
	ApiclientKey string
	Notify string
}

func (this *ConfigOfWeixin) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "appid":
		this.AppId = value
	case "appsecret":
		this.AppSecret = value
	case "token":
		this.Token = value
	case "encodingaeskey":
		this.EncodingAesKey = value
	case "mchid":
		this.MchId = value
	case "mchkey":
		this.MchKey = value
	case "apiclient_cert":
		this.ApiclientCert = value
	case "apiclient_key":
		this.ApiclientKey = value
	case "notify":
		this.Notify = value
	}
}

type ConfigOfWxOpen struct {
	Host string
	AppId string
	AppSecret string
	Token string
	EncodingAesKey string
	Exprie int64
}

func (this *ConfigOfWxOpen) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "host":
		this.Host = value
	case "appid":
		this.AppId = value
	case "appsecret":
		this.AppSecret = value
	case "token":
		this.Token = value
	case "encodingaeskey":
		this.EncodingAesKey = value
	case "exprie":
		this.Exprie, _ =  strconv.ParseInt(value, 10, 64)
	}
}

type ConfigOfWxWork struct {
	AppId string
	AppSecret string
	data map[string]string
}

func (this *ConfigOfWxWork) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "appid":
		this.AppId = value
	case "appsecret":
		this.AppSecret = value
	default:
		this.data[key] = value
	}
}

func (this *ConfigOfWxWork) Get(key string) string {
	return this.data[key]
}

type ConfigOfTim struct {
	SdkAppId string
	AccountType string
	Iidentifier string
	PrivatePem string
	Prefix string
}

func (this *ConfigOfTim) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "sdkappid":
		this.SdkAppId = value
	case "accounttype":
		this.AccountType = value
	case "identifier":
		this.Iidentifier = value
	case "private_pem":
		this.PrivatePem = value
	case "prefix":
		this.Prefix = value
	}
}

type Config struct {
	App *configOfApp
	Cookie *configOfCookie
	Session *configOfSession
	Template *configOfTemplate
	Gateway *configOfGateway
	Extend *configOfExtend

	Redis *cache.Config
	MySQL *db.Config
	Mongo *mongo.Config
	MQ *mq.Config
	WS *ws.Config

	Cron *cron.Config

	WxApp *ConfigOfWxApp
	Weixin *ConfigOfWeixin
	WxOpen *ConfigOfWxOpen
	WxWork *ConfigOfWxWork

	Tim *ConfigOfTim
}

func (this *Config) Constant(value string) string {
	value = strings.Replace(value, "ROOT_PATH", ROOT_PATH, -1)
	value = strings.Replace(value, "APP_PATH", APP_PATH, -1)
	value = strings.Replace(value, "PUBLIC_PATH", PUBLIC_PATH, -1)
	value = strings.Replace(value, "LOG_PATH", LOG_PATH, -1)

	return value
}

func (this *Config) SetApp(node map[string]string) {
	if this.App == nil {
		this.App = &configOfApp{}
	}

	for key, value := range node {
		this.App.Set(key, this.Constant(value))
	}
}

func (this *Config) SetCookie(node map[string]string) {
	if this.Cookie == nil {
		this.Cookie = &configOfCookie{}
	}

	for key, value := range node {
		this.Cookie.Set(key, this.Constant(value))
	}
}

func (this *Config) SetSession(node map[string]string) {
	if this.Session == nil {
		this.Session = &configOfSession{}
	}

	for key, value := range node {
		this.Session.Set(key, this.Constant(value))
	}
}

func (this *Config) SetTemplate(node map[string]string) {
	if this.Template == nil {
		this.Template = &configOfTemplate{}
	}

	for key, value := range node {
		this.Template.Set(key, this.Constant(value))
	}
}

func (this *Config) SetGateway(node map[string]string) {
	if this.Gateway == nil {
		this.Gateway = &configOfGateway{}
	}

	for key, value := range node {
		this.Gateway.Set(key, this.Constant(value))
	}
}

func (this *Config) SetExtend(section string, node map[string]string) {
	if this.Extend == nil {
		this.Extend = &configOfExtend{}
		this.Extend.data = map[string]map[string]string{}
	}

	if this.Extend.data[section] == nil {
		this.Extend.data[section] = map[string]string{}
	}

	for key, value := range node {
		this.Extend.data[section][key] = this.Constant(value)
	}
}

func (this *Config) SetRedis(node map[string]string) {
	if this.Redis == nil {
		this.Redis = &cache.Config{}
	}

	for key, value := range node {
		this.Redis.Set(key, this.Constant(value))
	}
}

func (this *Config) SetMySQL(node map[string]string) {
	if this.MySQL == nil {
		this.MySQL = &db.Config{}
	}

	for key, value := range node {
		this.MySQL.Set(key, this.Constant(value))
	}
}

func (this *Config) SetMongo(node map[string]string) {
	if this.Mongo == nil {
		this.Mongo = &mongo.Config{}
	}

	for key, value := range node {
		this.Mongo.Set(key, this.Constant(value))
	}
}

func (this *Config) SetMQ(node map[string]string) {
	if this.MQ == nil {
		this.MQ = &mq.Config{}
	}

	for key, value := range node {
		this.MQ.Set(key, this.Constant(value))
	}
}

func (this *Config) SetWS(node map[string]string) {
	if this.WS == nil {
		this.WS = &ws.Config{}
	}

	for key, value := range node {
		this.WS.Set(key, this.Constant(value))
	}
}

func (this *Config) SetCron(node map[string]string) {
	if this.Cron == nil {
		this.Cron = &cron.Config{Schedules: map[string]string{}}
	}

	for key, value := range node {
		this.Cron.Set(key, this.Constant(value))
	}
}

func (this *Config) SetWxApp(node map[string]string) {
	if this.WxApp == nil {
		this.WxApp = &ConfigOfWxApp{}
	}

	for key, value := range node {
		this.WxApp.Set(key, this.Constant(value))
	}
}

func (this *Config) SetWeixin(node map[string]string) {
	if this.Weixin == nil {
		this.Weixin = &ConfigOfWeixin{}
	}

	for key, value := range node {
		this.Weixin.Set(key, this.Constant(value))
	}
}

func (this *Config) SetWxOpen(node map[string]string) {
	if this.WxOpen == nil {
		this.WxOpen = &ConfigOfWxOpen{}
	}

	for key, value := range node {
		this.WxOpen.Set(key, this.Constant(value))
	}
}

func (this *Config) SetWxWork(node map[string]string) {
	if this.WxWork == nil {
		this.WxWork = &ConfigOfWxWork{}
		this.WxWork.data = map[string]string{}
	}

	for key, value := range node {
		this.WxWork.Set(key, this.Constant(value))
	}
}

func (this *Config) SetTim(node map[string]string) {
	if this.Tim == nil {
		this.Tim = &ConfigOfTim{}
	}

	for key, value := range node {
		this.Tim.Set(key, this.Constant(value))
	}
}

func (this *Config) Load(path string) {
	data := Ini(path)
	if data == nil {
		return
	}

	for section, node := range data {
		switch strings.ToLower(section) {
		case "app":
			this.SetApp(node)
		case "cookie":
			this.SetCookie(node)
		case "session":
			this.SetSession(node)
		case "template":
			this.SetTemplate(node)
		case "gateway":
			this.SetGateway(node)
		case "redis":
			this.SetRedis(node)
		case "mysql":
			this.SetMySQL(node)
		case "mongo":
			this.SetMongo(node)
		case "mq":
			this.SetMQ(node)
		case "ws":
			this.SetWS(node)
		case "cron":
			this.SetCron(node)
		case "wxapp":
			this.SetWxApp(node)
		case "weixin":
			this.SetWeixin(node)
		case "wxopen":
			this.SetWxOpen(node)
		case "wxwork":
			this.SetWxWork(node)
		case "tim":
			this.SetTim(node)
		default:
			this.SetExtend(section, node)
		}
	}
}