package tec

import (
	"strings"
)

type Router struct {
	data map[string]map[string]Handler
}

func (this *Router) handler(path string, handler map[string]Handler){
	if path == "/" {
		path = ""
	}

	paths := strings.Split(path, "/")

	if len(paths) == 4 {
		this.data[path] = handler
	} else if len(paths) == 3 {
		this.data[path + "/index"] = handler
	} else if len(paths) == 2 {
		this.data[path + "/index/index"] = handler
	} else if len(paths) == 1 {
		this.data["/home/index/index"] = handler
	}
}

func (this *Router) find(path string, method string) Handler {
	handler := this.data[path]
	if handler == nil {
		return nil
	}

	return handler[method]
}

func (this *Router) Add(path string, handler Handler) {
	this.REQUEST(path, handler)
}

func (this *Router) REQUEST(path string, handler Handler) {
	this.handler(path,  map[string]Handler{"GET": handler, "POST": handler})
}

func (this *Router) GET(path string, handler Handler) {
	this.handler(path,  map[string]Handler{"GET": handler})
}

func (this *Router) POST(path string, handler Handler) {
	this.handler(path,  map[string]Handler{"POST": handler})
}

func (this *Router) PUT(path string, handler Handler) {
	this.handler(path,  map[string]Handler{"PUT": handler})
}

func (this *Router) DELETE(path string, handler Handler) {
	this.handler(path,  map[string]Handler{"DELETE": handler})
}