package ws

import (
	"bufio"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var serial int64 = 0
var mutex sync.Mutex

type writeHook struct {
	p []byte
}

func (wh *writeHook) Write(p []byte) (int, error) {
	wh.p = p

	return len(p), nil
}

type Config struct {
	Host string
	Port int
	Path string
	Token string
	Origin string
	Version int
}

func (this *Config) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "host":
		this.Host = value
	case "port":
		this.Port, _ = strconv.Atoi(value)
	case "path":
		this.Path = value
	case "token":
		this.Token = value
	case "origin":
		this.Origin = value
	case "version":
		this.Version, _ = strconv.Atoi(value)
	}
}

type EventFunc func(req *Request, message string)

type Server struct {
	Config *Config
	Events map[string]interface{}
	Requests map[int64]*Request

	HandshakeTimeout time.Duration
	ReadBufferSize int
	WriteBufferSize int
	WriteBufferPool BufferPool
}

func (this *Server) ServeHTTP(rep http.ResponseWriter, req *http.Request) {
	request := this.HandleWS(rep, req)
	if request == nil {
		return
	}

	defer func() {
		mutex.Lock()
		delete(this.Requests, request.Fid)
		mutex.Unlock()

		request.Close()

		if closeFunc, ok := this.Events["close"]; ok {
			switch closeFunc.(type) {
			case func(req *Request):
				go closeFunc.(func(req *Request))(request)
			}
		}
	}()

	if connectFunc, ok := this.Events["connect"]; ok {
		switch connectFunc.(type) {
		case func(req *Request):
			go connectFunc.(func(req *Request))(request)
		}
	}

	for {
		message, err := request.ReadMessage()
		if err != nil {
			break
		}

		if messageFunc, ok := this.Events["message"]; ok {
			switch messageFunc.(type) {
			case func(req *Request, message string):
				go messageFunc.(func(req *Request, message string))(request, string(message))
			}
		}
	}
}

func (this *Server) HandleWS(rep http.ResponseWriter, req *http.Request) *Request {
	conn, brw, _ := rep.(http.Hijacker).Hijack()

	if brw.Reader.Buffered() > 0 {
		conn.Close()
		return nil
	}

	var br *bufio.Reader
	if this.ReadBufferSize == 0 && bufioReaderSize(conn, brw.Reader) > 256 {
		br = brw.Reader
	}

	buf := bufioWriterBuffer(conn, brw.Writer)

	var writeBuf []byte
	if this.WriteBufferPool == nil && this.WriteBufferSize == 0 && len(buf) >= maxFrameHeaderSize + 256 {
		writeBuf = buf
	}

	if br == nil {
		if this.ReadBufferSize == 0 {
			this.ReadBufferSize = defaultReadBufferSize
		} else if this.ReadBufferSize < maxControlFramePayloadSize {
			this.ReadBufferSize = maxControlFramePayloadSize
		}

		br = bufio.NewReaderSize(conn, this.ReadBufferSize)
	}

	if this.WriteBufferSize <= 0 {
		this.WriteBufferSize = defaultWriteBufferSize
	}

	this.WriteBufferSize += maxFrameHeaderSize

	if writeBuf == nil && this.WriteBufferPool == nil {
		writeBuf = make([]byte, this.WriteBufferSize)
	}

	atomic.AddInt64(&serial, 1)

	mu := make(chan struct{}, 1)
	mu <- struct{}{}

	request := &Request{
		br: br,
		conn: conn,
		handle: req,
		mu: mu,

		readFinal: true,
		writeBuf: writeBuf,
		writePool: this.WriteBufferPool,
		writeBufSize: this.WriteBufferSize,

		enableWriteCompression: true,

		Fid: serial,
		RoomId: 0,
		Server: this,
	}

	mutex.Lock()
	this.Requests[request.Fid] = request
	mutex.Unlock()

	p := buf
	if len(request.writeBuf) > len(p) {
		p = request.writeBuf
	}

	p = p[:0]

	p = append(p, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "...)
	p = append(p, computeAcceptKey(req.Header.Get("Sec-Websocket-Key"))...)
	p = append(p, "\r\n"...)
	p = append(p, "\r\n"...)

	conn.SetDeadline(time.Time{})

	if this.HandshakeTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(this.HandshakeTimeout))
	}

	if _, err := conn.Write(p); err != nil {
		conn.Close()
		return nil
	}

	if this.HandshakeTimeout > 0 {
		conn.SetWriteDeadline(time.Time{})
	}

	return request
}

func (this *Server) HandlePUSH(rep http.ResponseWriter, req *http.Request) {
	request := &Request{
		handle: req,
		Server: this,
	}

	if pushFunc, ok := this.Events["push"]; ok {
		switch pushFunc.(type) {
		case func(req *Request):
			go pushFunc.(func(req *Request))(request)
		}
	}
}

func (this *Server) Push(fid int64, message string) {
	if request, ok := this.Requests[fid]; ok && request != nil {
		request.WriteMessage([]byte(message))
	}
}

func (this *Server) Room(roomId int64, message string) {
	for _, request := range this.Requests {
		if request != nil && request.RoomId == roomId {
			request.WriteMessage([]byte(message))
		}
	}
}

func (this *Server) Broadcast(message string) {
	for _, request := range this.Requests {
		if request != nil {
			request.WriteMessage([]byte(message))
		}
	}
}
