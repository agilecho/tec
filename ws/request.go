package ws

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	finalBit = 1 << 7
	rsv1Bit  = 1 << 6
	rsv2Bit  = 1 << 5
	rsv3Bit  = 1 << 4
	maskBit = 1 << 7

	maxFrameHeaderSize = 2 + 8 + 4
	maxControlFramePayloadSize = 125

	writeWait = time.Second

	defaultReadBufferSize = 4096
	defaultWriteBufferSize = 4096

	continuationFrame = 0
	noFrame = -1
)

const (
	CloseNormalClosure = 1000
	CloseGoingAway = 1001
	CloseProtocolError = 1002
	CloseUnsupportedData = 1003
	CloseNoStatusReceived = 1005
	CloseAbnormalClosure = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation = 1008
	CloseMessageTooBig = 1009
	CloseMandatoryExtension = 1010
	CloseInternalServerErr = 1011
	CloseTLSHandshake = 1015
)

const (
	TextMessage = 1
	CloseMessage = 8
	PingMessage = 9
	PongMessage = 10
)

var (
	ErrCloseSent = errors.New("websocket: close sent")
	ErrReadLimit = errors.New("websocket: read limit exceeded")

	errWriteTimeout = &netError{msg: "websocket: write timeout", timeout: true, temporary: true}
	errUnexpectedEOF = &CloseError{Code: CloseAbnormalClosure, Text: io.ErrUnexpectedEOF.Error()}
	errBadWriteOpCode = errors.New("websocket: bad write message type")
	errWriteClosed = errors.New("websocket: write closed")
	errInvalidControlFrame = errors.New("websocket: invalid control frame")
)

type netError struct {
	msg string
	temporary bool
	timeout   bool
}

func (this *netError) Error() string {
	return this.msg
}

func (this *netError) Temporary() bool {
	return this.temporary
}

func (this *netError) Timeout() bool {
	return this.timeout
}

type CloseError struct {
	Code int
	Text string
}

func (this *CloseError) Error() string {
	s := []byte("websocket: close ")
	s = strconv.AppendInt(s, int64(this.Code), 10)
	switch this.Code {
	case CloseNormalClosure:
		s = append(s, " (normal)"...)
	case CloseGoingAway:
		s = append(s, " (going away)"...)
	case CloseProtocolError:
		s = append(s, " (protocol error)"...)
	case CloseUnsupportedData:
		s = append(s, " (unsupported data)"...)
	case CloseNoStatusReceived:
		s = append(s, " (no status)"...)
	case CloseAbnormalClosure:
		s = append(s, " (abnormal closure)"...)
	case CloseInvalidFramePayloadData:
		s = append(s, " (invalid payload data)"...)
	case ClosePolicyViolation:
		s = append(s, " (policy violation)"...)
	case CloseMessageTooBig:
		s = append(s, " (message too big)"...)
	case CloseMandatoryExtension:
		s = append(s, " (mandatory extension missing)"...)
	case CloseInternalServerErr:
		s = append(s, " (internal server error)"...)
	case CloseTLSHandshake:
		s = append(s, " (TLS handshake error)"...)
	}

	if this.Text != "" {
		s = append(s, ": "...)
		s = append(s, this.Text...)
	}

	return string(s)
}

type BufferPool interface {
	Get() interface{}
	Put(interface{})
}

type writePoolData struct {
	buf []byte
}

type Request struct {
	conn net.Conn
	handle *http.Request
	mu chan struct{}

	writeBuf []byte
	writePool BufferPool
	writeBufSize int
	writeDeadline time.Time
	writer io.WriteCloser

	writeErrMu sync.Mutex
	writeErr error

	enableWriteCompression bool
	newCompressionWriter func(io.WriteCloser) io.WriteCloser

	reader io.ReadCloser
	readErr error
	br *bufio.Reader
	readRemaining int64
	readFinal bool
	readLength int64
	readLimit int64
	readMaskPos int
	readMaskKey [4]byte
	readErrCount int

	frameReader *frameReader

	readDecompress bool
	newDecompressionReader func(io.Reader) io.ReadCloser

	Fid int64
	RoomId int64
	Server *Server
}

func (this *Request) writeFatal(err error) error {
	err = hideTempErr(err)
	this.writeErrMu.Lock()

	if this.writeErr == nil {
		this.writeErr = err
	}

	this.writeErrMu.Unlock()
	return err
}

func (this *Request) read(n int) ([]byte, error) {
	p, err := this.br.Peek(n)
	if err == io.EOF {
		err = errUnexpectedEOF
	}

	this.br.Discard(len(p))

	return p, err
}

func (this *Request) write(frameType int, deadline time.Time, buf0, buf1 []byte) error {
	<-this.mu

	defer func() { this.mu <- struct{}{} }()

	this.writeErrMu.Lock()
	err := this.writeErr
	this.writeErrMu.Unlock()

	if err != nil {
		return err
	}

	this.conn.SetWriteDeadline(deadline)
	if len(buf1) == 0 {
		_, err = this.conn.Write(buf0)
	} else {
		err = this.writeBufs(buf0, buf1)
	}

	if err != nil {
		return this.writeFatal(err)
	}

	if frameType == CloseMessage {
		this.writeFatal(ErrCloseSent)
	}

	return nil
}

func (this *Request) WriteControl(messageType int, data []byte, deadline time.Time) error {
	if !isControl(messageType) {
		return errBadWriteOpCode
	}
	if len(data) > maxControlFramePayloadSize {
		return errInvalidControlFrame
	}

	b0 := byte(messageType) | finalBit
	b1 := byte(len(data))

	buf := make([]byte, 0, maxFrameHeaderSize+maxControlFramePayloadSize)
	buf = append(buf, b0, b1)
	buf = append(buf, data...)

	d := 1000 * time.Hour
	if !deadline.IsZero() {
		d = deadline.Sub(time.Now())
		if d < 0 {
			return errWriteTimeout
		}
	}

	timer := time.NewTimer(d)
	select {
	case <-this.mu:
		timer.Stop()
	case <-timer.C:
		return errWriteTimeout
	}

	defer func() { this.mu <- struct{}{} }()

	this.writeErrMu.Lock()
	err := this.writeErr
	this.writeErrMu.Unlock()
	if err != nil {
		return err
	}

	this.conn.SetWriteDeadline(deadline)
	_, err = this.conn.Write(buf)
	if err != nil {
		return this.writeFatal(err)
	}
	if messageType == CloseMessage {
		this.writeFatal(ErrCloseSent)
	}
	return err
}

func (this *Request) beginMessage(mw *frameWriter, messageType int) error {
	if this.writer != nil {
		this.writer.Close()
		this.writer = nil
	}

	if !isControl(messageType) && !isData(messageType) {
		return errBadWriteOpCode
	}

	this.writeErrMu.Lock()
	err := this.writeErr
	this.writeErrMu.Unlock()
	if err != nil {
		return err
	}

	mw.request = this
	mw.frameType = messageType
	mw.pos = maxFrameHeaderSize

	if this.writeBuf == nil {
		wpd, ok := this.writePool.Get().(writePoolData)
		if ok {
			this.writeBuf = wpd.buf
		} else {
			this.writeBuf = make([]byte, this.writeBufSize)
		}
	}
	return nil
}

func (this *Request) NextWriter(messageType int) (io.WriteCloser, error) {
	var mw frameWriter
	if err := this.beginMessage(&mw, messageType); err != nil {
		return nil, err
	}

	this.writer = &mw

	if this.newCompressionWriter != nil && this.enableWriteCompression && isData(messageType) {
		w := this.newCompressionWriter(this.writer)
		this.writer = w
	}

	return this.writer, nil
}

func (this *Request) WriteMessage(data []byte) error {
	if this.newCompressionWriter == nil || !this.enableWriteCompression {
		var mw frameWriter
		if err := this.beginMessage(&mw, TextMessage); err != nil {
			return err
		}
		n := copy(this.writeBuf[mw.pos:], data)
		mw.pos += n
		data = data[n:]
		return mw.flushFrame(true, data)
	}

	w, err := this.NextWriter(TextMessage)
	if err != nil {
		return err
	}

	if _, err = w.Write(data); err != nil {
		return err
	}

	return w.Close()
}

func (this *Request) advanceFrame() (int, error) {
	if this.readRemaining > 0 {
		if _, err := io.CopyN(ioutil.Discard, this.br, this.readRemaining); err != nil {
			return noFrame, err
		}
	}

	p, err := this.read(2)
	if err != nil {
		return noFrame, err
	}

	final := p[0]&finalBit != 0
	frameType := int(p[0] & 0xf)
	mask := p[1] & maskBit != 0
	this.readRemaining = int64(p[1] & 0x7f)

	this.readDecompress = false
	if this.newDecompressionReader != nil && (p[0]&rsv1Bit) != 0 {
		this.readDecompress = true
		p[0] &^= rsv1Bit
	}

	if rsv := p[0] & (rsv1Bit | rsv2Bit | rsv3Bit); rsv != 0 {
		return noFrame, this.handleProtocolError("unexpected reserved bits 0x" + strconv.FormatInt(int64(rsv), 16))
	}

	switch frameType {
	case CloseMessage, PingMessage, PongMessage:
		if this.readRemaining > maxControlFramePayloadSize {
			return noFrame, this.handleProtocolError("control frame length > 125")
		}
		if !final {
			return noFrame, this.handleProtocolError("control frame not final")
		}
	case TextMessage:
		if !this.readFinal {
			return noFrame, this.handleProtocolError("message start before final message frame")
		}
		this.readFinal = final
	case continuationFrame:
		if this.readFinal {
			return noFrame, this.handleProtocolError("continuation after final message frame")
		}
		this.readFinal = final
	default:
		return noFrame, this.handleProtocolError("unknown opcode " + strconv.Itoa(frameType))
	}

	switch this.readRemaining {
	case 126:
		p, err := this.read(2)
		if err != nil {
			return noFrame, err
		}

		this.readRemaining = int64(binary.BigEndian.Uint16(p))
	case 127:
		p, err := this.read(8)
		if err != nil {
			return noFrame, err
		}
		this.readRemaining = int64(binary.BigEndian.Uint64(p))
	}

	if !mask {
		return noFrame, this.handleProtocolError("incorrect mask flag")
	}

	if mask {
		this.readMaskPos = 0
		p, err := this.read(len(this.readMaskKey))
		if err != nil {
			return noFrame, err
		}
		copy(this.readMaskKey[:], p)
	}

	if frameType == continuationFrame || frameType == TextMessage {

		this.readLength += this.readRemaining
		if this.readLimit > 0 && this.readLength > this.readLimit {
			this.WriteControl(CloseMessage, FormatCloseMessage(CloseMessageTooBig, ""), time.Now().Add(writeWait))
			return noFrame, ErrReadLimit
		}

		return frameType, nil
	}

	var payload []byte
	if this.readRemaining > 0 {
		payload, err = this.read(int(this.readRemaining))
		this.readRemaining = 0
		if err != nil {
			return noFrame, err
		}

		maskBytes(this.readMaskKey, 0, payload)
	}

	return frameType, nil
}

func (this *Request) handleProtocolError(message string) error {
	this.WriteControl(CloseMessage, FormatCloseMessage(CloseProtocolError, message), time.Now().Add(writeWait))
	return errors.New("websocket: " + message)
}

func (this *Request) NextReader() (r io.Reader, err error) {
	if this.reader != nil {
		this.reader.Close()
		this.reader = nil
	}

	this.frameReader = nil
	this.readLength = 0

	for this.readErr == nil {
		frameType, err := this.advanceFrame()
		if err != nil {
			this.readErr = hideTempErr(err)
			break
		}

		if frameType == TextMessage {
			this.frameReader = &frameReader{this}
			this.reader = this.frameReader
			if this.readDecompress {
				this.reader = this.newDecompressionReader(this.reader)
			}

			return this.reader, nil
		}
	}

	this.readErrCount++
	if this.readErrCount >= 1000 {
		panic("repeated read on failed websocket connection")
	}

	return nil, this.readErr
}

func (this *Request) ReadMessage() (p []byte, err error) {
	var r io.Reader
	r, err = this.NextReader()
	if err != nil {
		return nil, err
	}
	p, err = ioutil.ReadAll(r)
	return p, err
}

func (this *Request) SetReadDeadline(t time.Time) error {
	return this.conn.SetReadDeadline(t)
}

func (this *Request) writeBufs(bufs ...[]byte) error {
	b := net.Buffers(bufs)
	_, err := b.WriteTo(this.conn)
	return err
}

func (this *Request) Header(key string) string {
	return this.handle.Header.Get(key)
}

func (this *Request) Query(key string) string {
	temp := this.handle.URL.Query()[key]
	return strings.Join(temp, ",")
}

func (this *Request) Close() error {
	return this.conn.Close()
}