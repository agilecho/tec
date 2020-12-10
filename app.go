package tec

import (
	"context"
	"fmt"
	"github.com/agilecho/tec/cache"
	"github.com/agilecho/tec/cron"
	"github.com/agilecho/tec/db"
	"github.com/agilecho/tec/mongo"
	"github.com/agilecho/tec/mq"
	"github.com/agilecho/tec/ws"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Handler func(ctx *Context)
type StartFunc func(app *App)
type BeforeFilterFunc func(ctx *Context) bool
type AfterFilterFunc func(ctx *Context, method string, data interface{})

type Result struct {
	Code int `json:"code"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

type Current struct {
	Id int64 `json:"id"`
	Name string `json:"name"`
	Avatar string `json:"avatar"`
	Token string `json:"token"`
	Sign string `json:"sign"`
}

type App struct {
	Config *Config
	Router *Router
	Debug bool

	startFunc *StartFunc
	emptyFunc *Handler

	beforeFilter []BeforeFilterFunc
	afterFilter []AfterFilterFunc

	events map[string]interface{}

	pool *sync.Pool
	ws ws.Server

	srv http.Server
}

func (this *App) Before(filter BeforeFilterFunc) {
	this.beforeFilter = append(this.beforeFilter, filter)
}

func (this *App) Use(filter BeforeFilterFunc) {
	this.Before(filter)
}

func (this *App) After(filter AfterFilterFunc) {
	this.afterFilter = append(this.afterFilter, filter)
}

func (this *App) Start(fun StartFunc) {
	this.startFunc = &fun
}

func (this *App) Empty(fun Handler) {
	this.emptyFunc = &fun
}

func (this *App) Bind(event string, callback interface{}) {
	this.events[event] = callback
}

func (this *App) init() {
	if fun, ok := this.events["config"]; ok {
		fun.(func(config *Config))(this.Config)
	} else {
		this.Config.Load(ROOT_PATH + "/config/" + HOST_NAME + ".ini")
	}

	CONFIG = this.Config

	if this.Config.Redis != nil {
		cache.Init(this.Config.Redis)
	}

	if this.Config.MySQL != nil {
		db.Init(this.Config.MySQL)

		go func() {
			pring := time.NewTicker(3600 * time.Second)
			for {
				select {
				case <- pring.C:
					db.Ping()
				}
			}
		}()
	}

	if this.Config.Mongo != nil {
		mongo.Init(this.Config.Mongo)
	}

	if this.Config.MQ != nil {
		mq.Init(this.Config.MQ)
	}

	if this.Config.Cron != nil {
		this.Config.Cron.Log = LOG_PATH + "/cron.txt"
		cron.Init(this.Config.Cron)
	}

	if this.Config.Session != nil {
		sessionStart()
	}

	if this.startFunc != nil {
		(*this.startFunc)(this)
	}

	if this.emptyFunc == nil {
		this.Empty(func(ctx *Context) {
			ctx.Json(Result{Code: 404, Msg: "can not find handler path:" + ctx.Path + " method:" + ctx.Method})
		})
	}

	this.pool = &sync.Pool{
		New: func() interface{} {
			return &Context{}
		},
	}
}

func (this *App) Handler(rep http.ResponseWriter, req *http.Request) {
	if this.Config.App == nil || !this.Config.App.Debug {
		defer Exception("App Handler ")
	}

	if req.RequestURI == "/favicon.ico" {
		return
	}

	context := &Context{afterFilter: this.afterFilter}

	context.Request = req
	context.Response = rep

	if this.Config.Session != nil {
		context.Session = sessionCreate(rep, req)
	}

	context.Uri = req.RequestURI
	context.Url = req.URL.Path

	context.Init()

	for i := 0; i < len(this.beforeFilter); i++ {
		if !this.beforeFilter[i](context) {
			context.Close()
			return
		}
	}

	path := "/" + context.Module + "/" + context.Controller + "/" + context.Action

	handler := this.Router.find(path, req.Method)
	if handler == nil {
		(*this.emptyFunc)(context)
	} else {
		handler(context)
	}

	context.Close()
}

func (this *App) Run() {
	this.init()

	if this.Config.Gateway != nil && this.Config.Gateway.Enable {
		this.gatewayPing(true)
	}

	http.HandleFunc("/", this.Handler)

	if this.Config.App.Static != "" {
		http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(this.Config.App.Static))))
	}

	fmt.Println("app: " + this.Config.App.Host + ":" + strconv.Itoa(this.Config.App.Port))

	this.srv = http.Server{Addr:this.Config.App.Host + ":" + strconv.Itoa(this.Config.App.Port)}

	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)

	signWG := sync.WaitGroup{}
	signWG.Add(1)

	go func() {
		sign := <-channel
		this.Close(sign)
		signWG.Done()
	}()

	err := this.srv.ListenAndServe()

	signWG.Wait()

	if err != nil && err != http.ErrServerClosed {
		Logger("app.Run error:" + err.Error(), "error", "false")

		if this.Config.App.Debug {
			panic(err)
		}
	}
}

func (this *App) RunWS() {
	this.init()

	this.ws = ws.Server{Config:this.Config.WS}
	this.ws.Events = map[string]interface{}{}
	this.ws.Requests = map[int64]*ws.Request{}

	for key, value := range this.events {
		this.ws.Events[key] = value
	}

	http.Handle(this.Config.WS.Path, &this.ws)
	http.HandleFunc(this.Config.WS.Path + "/push", this.ws.HandlePUSH)

	fmt.Println("app: " + this.Config.WS.Host + ":" + strconv.Itoa(this.Config.WS.Port))

	this.srv = http.Server{Addr:this.Config.WS.Host + ":" + strconv.Itoa(this.Config.WS.Port)}

	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)

	signWG := sync.WaitGroup{}
	signWG.Add(1)

	go func() {
		sign := <-channel
		this.Close(sign)
		signWG.Done()
	}()

	err := this.srv.ListenAndServe()

	signWG.Wait()

	if err != nil && err != http.ErrServerClosed {
		Logger("app.RunWS error:" + err.Error(), "error", "false")

		if this.Config.App != nil && this.Config.App.Debug {
			panic(err)
		}
	}
}

func (this *App) gatewayPing(enable bool) {
	if this.Config.Gateway == nil || !this.Config.Gateway.Enable || this.Config.Gateway.Url == "" {
		return
	}

	if enable {
		gatewayRequest(this.Config, 1)

		go func(config *Config) {
			pring := time.NewTicker(2 * time.Second)
			for {
				select {
				case <- pring.C:
					gatewayRequest(config, 1)
				}
			}
		}(this.Config)
	} else {
		gatewayRequest(this.Config, 0)
	}
}

func (this *App) Close(sign os.Signal) {
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
	defer cancel()

	if this.Config.Redis != nil {
		cache.Close()
	}

	if this.Config.MySQL != nil {
		db.Close()
	}

	if this.Config.Mongo != nil {
		mongo.Close()
	}

	if this.Config.MQ != nil {
		mq.Close()
	}

	if this.Config.Cron != nil {
		cron.Stop()
	}

	if this.Config.Session != nil {
		sessionGC()
	}

	if this.Config.Gateway != nil && this.Config.Gateway.Enable {
		this.gatewayPing(false)
	}

	err := this.srv.Shutdown(ctx)
	if err != nil {
		Logger("app.server shutdown error:" + err.Error(), "error", "false")
	}

	fmt.Println("app close of " + sign.String())
}

func gatewayRequest(config *Config, state int) {
	url := strings.Builder{}
	url.WriteString(config.Gateway.Url)
	url.WriteString("/")
	url.WriteString(config.App.Host)
	url.WriteString("/")
	url.WriteString(strconv.Itoa(config.App.Port))
	url.WriteString("/")
	url.WriteString(config.App.Name)
	url.WriteString("/")
	url.WriteString(strconv.Itoa(state))

	client := &http.Client{}
	client.Timeout = 50 * 1000 * time.Microsecond

	request, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		Logger("app.gatewayRequest error:" + err.Error(), "error", "false")
		return
	}

	_, err = client.Do(request)
	if err != nil {
		Logger("app.gatewayRequest error:" + err.Error(), "error", "false")
	}
}

func New() *App {
	app := App{}

	app.Config = &Config{}
	app.Config.App = &configOfApp{}

	app.Router = &Router{}
	app.Router.data = map[string]map[string]Handler{}

	app.beforeFilter = []BeforeFilterFunc{}
	app.afterFilter = []AfterFilterFunc{}
	app.events = map[string]interface{}{}

	return &app
}

func Cli(callback func()) {
	if callback == nil {
		if CONFIG.Redis != nil {
			cache.Close()
		}

		if CONFIG.MySQL != nil {
			db.Close()
		}

		if CONFIG.Mongo != nil {
			mongo.Close()
		}

		if CONFIG.MQ != nil {
			mq.Close()
		}

		if CONFIG.Cron != nil {
			cron.Stop()
		}

		return
	}

	config := &Config{}
	config.Load(ROOT_PATH + "/config/" + GetHostName() + ".ini")

	if config.App == nil {
		config.App = &configOfApp{}
	}

	if config.Redis != nil {
		cache.Init(config.Redis)
	}

	if config.MySQL != nil {
		db.Init(config.MySQL)
	}

	if config.Mongo != nil {
		mongo.Init(config.Mongo)
	}

	if config.MQ != nil {
		mq.Init(config.MQ)
	}

	if config.Cron != nil {
		config.Cron.Log = LOG_PATH + "/cron.txt"
		cron.Init(config.Cron)
	}

	CONFIG = config

	fmt.Println("ROOT_PATH:" + ROOT_PATH + " HOST_NAME:" + HOST_NAME)
	fmt.Println("cli run")

	callback()
}

func Notify() {
	if CONFIG.MySQL != nil {
		go func() {
			pring := time.NewTicker(3600 * time.Second)
			for {
				select {
				case <- pring.C:
					db.Ping()
				}
			}
		}()
	}

	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)

	signWG := sync.WaitGroup{}
	signWG.Add(1)

	go func() {
		sign := <-channel
		fmt.Println("cli close of " + sign.String())
		Cli(nil)
		signWG.Done()
	}()

	signWG.Wait()
	os.Exit(0)
}