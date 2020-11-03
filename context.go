package tec

import (
	"encoding/xml"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Context struct {
	Request *http.Request
	Response http.ResponseWriter

	IsTLS bool
	IsRobot bool
	IsWeiXin bool
	IsAjax bool
	IsWebsocket bool

	Scheme string
	Host string
	Accept string
	Method string
	RealIP string
	Referer string
	Protocol string

	Uri string
	Url string

	Path string
	Module string
	Controller string
	Action string
	Current *Current

	Header map[string]string
	Param map[string]string
	Query map[string]string
	Form map[string]string

	Files map[string][]*multipart.FileHeader
	Cookies map[string]*http.Cookie

	Session Session
	Setting map[string]interface{}

	afterFilter []AfterFilterFunc
}

func (this *Context) Reset() {
	this.Request = nil
	this.Response = nil

	this.IsTLS = false
	this.IsRobot = false
	this.IsWeiXin = false
	this.IsAjax = false
	this.IsWebsocket = false

	this.Scheme = ""
	this.Host = ""
	this.Accept = ""
	this.Method = ""
	this.RealIP = ""
	this.Referer = ""
	this.Protocol = ""

	this.Uri = ""
	this.Url = ""

	this.Path = ""
	this.Module = ""
	this.Controller = ""
	this.Action = ""
	this.Current = nil

	this.Header = nil
	this.Param = nil
	this.Query = nil
	this.Form = nil

	this.Files = nil
	this.Cookies = nil

	this.Session = nil
	this.Setting = nil
	
	this.afterFilter = []AfterFilterFunc{}
}

func (this *Context) Init() {
	if CONFIG.Gateway != nil && CONFIG.Gateway.Enable {
		this.Uri = this.Uri[len(CONFIG.App.Name) + 1:]
	}

	this.Module = "home"
	this.Controller = "index"
	this.Action = "index"

	u, err := url.Parse(this.Uri)
	if err != nil {
		Logger("context.Init error:" + err.Error(), "error")
		return
	}

	this.Path = u.Path
	paths := strings.Split(u.Path, "/")

	if len(paths) > 1 && len(paths[1]) > 0 && paths[1] != "/" {
		this.Module = paths[1]
	}

	if len(paths) > 2 && len(paths[2]) > 0 && paths[2] != "/"{
		this.Controller = paths[2]
	}

	if len(paths) > 3 && len(paths[3]) > 0 && paths[3] != "/" {
		this.Action = paths[3]
	}

	this.IsTLS = this.Request.TLS != nil

	headers := map[string]string{}
	for key, value := range this.Request.Header {
		headers[key] = strings.Join(value, ",")
	}

	this.Header = headers

	userAgent := headers["User-Agent"]
	if !strings.Contains(userAgent, "http://") && regexp.MustCompile(`Bot|Crawl|Spider|slurp|sohu-search|lycos|robozilla`).Match([]byte(userAgent)) {
		this.IsRobot = true
	}

	if strings.Contains(userAgent, "MicroMessenger") {
		this.IsWeiXin = true
	}

	if header, ok := headers["Upgrade"]; ok && header == "XMLHttpRequest" {
		this.IsAjax = true
	}

	if header, ok := headers["Upgrade"]; ok && header == "websocket"{
		this.IsWebsocket = true
	}

	this.Method = this.Request.Method

	if ip, ok := headers["X-Forwarded-For"]; ok && ip != "" {
		this.RealIP = strings.Split(ip, ",")[0]
	} else if ip, ok := headers["X-Real-IP"]; ok && ip != "" {
		this.RealIP = ip
	} else if ip, ok := headers["Client-IP"]; ok && ip != "" {
		this.RealIP = ip
	} else if ra, _, _ := net.SplitHostPort(this.Request.RemoteAddr); ra != "" {
		this.RealIP = ra
	}

	this.Referer = headers["Referer"]

	if this.IsTLS {
		this.Scheme = "https"
	} else if scheme, ok := headers["X-Forwarded-Proto"]; ok && scheme != "" {
		this.Scheme = scheme
	} else if scheme, ok := headers["X-Forwarded-Protocol"]; ok && scheme != "" {
		this.Scheme = scheme
	} else if ssl, ok := headers["X-Forwarded-Ssl"]; ok && ssl == "on" {
		this.Scheme = "https"
	} else if scheme, ok := headers["X-Url-Scheme"]; ok && scheme != "" {
		this.Scheme = scheme
	} else {
		this.Scheme = "http"
	}

	if this.Request.Host != "" {
		if hostPart, _, err := net.SplitHostPort(this.Request.Host); err == nil {
			this.Host = hostPart
		} else {
			this.Host  = this.Request.Host
		}
	} else {
		this.Host = "localhost"
	}

	this.Accept = headers["Accept"]
	this.Protocol = this.Request.Proto

	this.Query = map[string]string{}
	for key, value := range this.Request.URL.Query() {
		this.Query[key] = strings.Join(value, ",")
	}

	this.Form = map[string]string{}
	formJson := map[string]string{}

	if strings.Contains(headers["Content-Type"], "multipart/form-data") {
		this.Request.ParseMultipartForm(CONFIG.App.Memory)

		for key, value := range this.Request.MultipartForm.Value {
			this.Form[key] = strings.Join(value, ",")
		}

		this.Files = this.Request.MultipartForm.File
	} else if strings.Contains(headers["Content-Type"], "application/json") {
		body, _ := ioutil.ReadAll(this.Request.Body)

		params := JsonDecode(string(body))
		if params != nil {
			for key, value := range params {
				switch value.(type) {
				case bool:
					formJson[key] = strconv.FormatBool(value.(bool))
				case string:
					formJson[key] = value.(string)
				case float64:
					formJson[key] = strconv.FormatFloat(value.(float64), 'f', -1, 64)
				}
			}
		}
	} else {
		this.Request.ParseForm()

		for key, value := range this.Request.PostForm {
			this.Form[key] = strings.Join(value, ",")
		}
	}

	this.Param = map[string]string{}
	for key, value := range this.Request.Form {
		this.Param[key] = strings.Join(value, ",")
	}

	for key, value := range formJson {
		this.Param[key] = value
	}

	this.Cookies = map[string]*http.Cookie{}
	for i := 0; i < len(this.Request.Cookies()); i++ {
		cookie := this.Request.Cookies()[i]
		this.Cookies[cookie.Name] = cookie
	}

	this.Setting = map[string]interface{}{}
}

func (this *Context) Dispatch() []string {
	return []string {this.Module, this.Controller, this.Action}
}

func (this *Context) Cookie(name, value string, expire int) {
	if expire == 0 {
		expire = CONFIG.Cookie.Expire
	}

	http.SetCookie(this.Response, &http.Cookie{
		Name: name,
		Value: url.QueryEscape(value),
		MaxAge: expire,
		Path: CONFIG.Cookie.Path,
		Domain: CONFIG.Cookie.Domain,
		Secure: CONFIG.Cookie.Secure,
		HttpOnly: CONFIG.Cookie.HttpOnly,
	})
}

func (this *Context) Save(name string, target string) bool {
	if target[0:1] != "/" {
		target = "/" + target
	}

	path := PUBLIC_PATH + target

	if !IsDir(filepath.Dir(path)) {
		err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if err != nil {
			Logger("context.Save MkdirAll error:" + err.Error(), "error")
			return false
		}
	}

	file, err := this.Files[name][0].Open()
	if err != nil {
		Logger("context.Save Open error:" + err.Error(), "error")
		return false
	}

	defer file.Close()

	out, err := os.Create(path)
	if err != nil {
		Logger("context.Save os.Create error:" + err.Error(), "error")
		return false
	}

	defer out.Close()

	_, err = io.Copy(out, file)
	if err != nil {
		Logger("context.Save io.Copy error:" + err.Error(), "error")
		return false
	}

	return true
}

func (this *Context) Download(file string, name string) {
	if _, err := os.Stat(file); err != nil {
		http.ServeFile(this.Response, this.Request, file)
		return
	}

	filename := url.PathEscape(name)
	if name == filename {
		filename = "filename=" + filename
	} else {
		filename = "filename=" + name + "; filename*=utf-8''" + filename
	}

	this.invokeAfter("Download", file)

	this.Response.Header().Set("Content-Disposition", "attachment; " + filename)
	this.Response.Header().Set("Content-Description", "File Transfer")
	this.Response.Header().Set("Content-Type", "application/octet-stream")
	this.Response.Header().Set("Content-Transfer-Encoding", "binary")
	this.Response.Header().Set("Expires", "0")
	this.Response.Header().Set("Cache-Control", "must-revalidate")
	this.Response.Header().Set("Pragma", "public")

	http.ServeFile(this.Response, this.Request, file)
}

func (this *Context) Text(args ...string) {
	this.invokeAfter("Text", args[0])

	chartset := "UTF-8"
	if len(args) > 1 {
		chartset = args[1]
	}

	this.Response.Header().Set("Content-Type", "text/html")
	this.Response.Header().Set("Charset", chartset)

	_, err := this.Response.Write([]byte(args[0]))
	if err != nil {
		Logger("context.Text error:" + err.Error(), "error")
	}
}

func (this *Context) Json(data interface{}) {
	this.invokeAfter("Json", data)

	this.Response.Header().Set("Content-Type", "application/json")
	this.Response.Header().Set("Charset", "UTF-8")

	_, err := this.Response.Write([]byte(JsonEncode(data)))
	if err != nil {
		Logger("context.Json error:" + err.Error(), "error")
	}
}

func (this *Context) Result(args ...interface{}) {
	result := Result{}
	for _, arg := range args {
		switch arg.(type) {
		case int:
			result.Code = arg.(int)
		case string:
			result.Msg = arg.(string)
		default:
			result.Data = arg
		}
	}

	this.Json(result)
}

func (this *Context) Html(data string) {
	var html = strings.Builder{}

	html.WriteString("<!DOCTYPE html><html><head>")
	html.WriteString("<meta charset=\"UTF-8\"></head>")
	html.WriteString("<body style=\"padding-top:2rem; text-align:center;\">")
	html.WriteString(data)
	html.WriteString("</body></html>")

	this.Text(html.String())
}

func (this *Context) Render(file string, data map[string]interface{}) {
	if CONFIG.Template == nil {
		CONFIG.Template = &configOfTemplate{
			Path: ROOT_PATH + "/app",
			Extension: ".html",
		}
	}

	if file == "" || file[0] != '/' {
		module := this.Module
		controller := this.Controller

		if file == "" {
			file = this.Action
		}

		if strings.Contains(file, "@") {
			module = file[0:strings.Index(file, "@")]
			file = file[strings.Index(file, "@") + 1:]
		}

		paths := strings.Split(file, "/")
		if len(paths) > 1 {
			controller = paths[0]
			file = paths[1]
		}

		file = "/" + module + "/" + strings.Replace(controller, ".", "/", -1) + "/" + file
	}

	if data == nil {
		data = map[string]interface{}{}
	}

	data["tec"] = map[string]interface{}{
		"config": CONFIG,
		"current": this.Current,
		"param": this.Param,
		"dispatch": map[string]string{
			"module": this.Module,
			"controller": this.Controller,
			"action": this.Action,
		},
	}

	data["setting"] = this.Setting

	if this.Session == nil {
		data["session"] = map[string]interface{}{}
	} else {
		data["session"] = this.Session.Get("")
	}

	this.invokeAfter("Render", []interface{}{file, data})

	this.Response.Header().Set("Content-Type", "text/html")
	this.Response.Header().Set("Charset", "UTF-8")

	files := []string{CONFIG.Template.Path + file + CONFIG.Template.Extension}

	if CONFIG.Template.Define != "" {
		defines := strings.Split(CONFIG.Template.Define, ",")
		if len(defines) > 0 {
			for _, value := range defines {
				files = append(files, CONFIG.Template.Path + value + CONFIG.Template.Extension)
			}
		}
	}

	tpl := template.New(BaseName(files[0]))
	tpl = tpl.Funcs(template.FuncMap{
		"Println": fmt.Println,
		"Sprintf": fmt.Sprintf,

		"IdEnCode": IdEnCode,
		"IdDeCode": IdDeCode,
		"UcFirst": UcFirst,
		"StripWords": StripWords,
		"CutString": CutString,
		"SubTimer": SubTimer,
		"TimeSpan": TimeSpan,
		"StripTags": StripTags,
		
		"Add": Add,
		"Subtract": Subtract,
		"Multiply": Multiply,
		"Divide": Divide,
		"Round": Round,
		"Floor": Floor,
		"Ceil": Ceil,
		"Max": Max,
		"Min": Min,
		"Rand": Rand,
		"Random": Random,

		"FormatInt": FormatInt,
		"FormatInt64": FormatInt64,
		"FormatFloat64": FormatFloat64,
		"FormatBytes": FormatBytes,
		"FormatDiscount": FormatDiscount,
		"FormatMobilePrivacy": FormatMobilePrivacy,
		"FormatPrice": FormatPrice,
		"FormatTime": FormatTime,

		"Loop": Loop,
		"Pager": Pager,
	})

	tpl, err := tpl.ParseFiles(files...)
	if err != nil {
		Logger("context.Render ParseFiles error:" + err.Error(), "error")
	}

	err = tpl.Execute(this.Response, data)
	if err != nil {
		Logger("context.Render Execute error:" + err.Error(), "error")
	}
}

func (this *Context) Message(file string, args ...string)  {
	data := map[string]interface{}{}
	if len(args) > 0 {
		data["message"] = args[0]
	}

	if len(args) > 1 {
		data["forward"] = args[1]
	}

	this.Render(file, data)
}

func (this *Context) XML(data interface{}) {
	this.invokeAfter("XML", data)

	this.Response.Header().Set("Content-Type", "application/xml")
	this.Response.Header().Set("Charset", "UTF-8")

	content, _ := xml.Marshal(data)

	_, err := this.Response.Write(content)
	if err != nil {
		Logger("context.XML error:" + err.Error(), "error")
	}
}

func (this *Context) Flush() {
	if f, ok := this.Response.(http.Flusher); ok {
		f.Flush()
	}
}

func (this *Context) Redirect(url string) {
	http.Redirect(this.Response, this.Request, url, http.StatusFound)
}

func (this *Context) Close() {
	if this.Session != nil {
		this.Session.Close()
	}
}

func (this *Context) invokeAfter(method string, data interface{}) {
	for i := 0; i < len(this.afterFilter); i++ {
		this.afterFilter[i](this, method, data)
	}
}