package ws

import (
	"encoding/binary"
	"errors"
	"io"
)

type frameWriter struct {
	request *Request
	pos int
	frameType int
	err error
}

func (this *frameWriter) endMessage(err error) error {
	if this.err != nil {
		return err
	}

	request := this.request

	this.err = err

	request.writer = nil
	if request.writePool != nil {
		request.writePool.Put(writePoolData{buf: request.writeBuf})
		request.writeBuf = nil
	}

	return err
}

func (this *frameWriter) flushFrame(final bool, extra []byte) error {
	request := this.request
	length := this.pos - maxFrameHeaderSize + len(extra)

	if isControl(this.frameType) &&
		(!final || length > maxControlFramePayloadSize) {
		return this.endMessage(errInvalidControlFrame)
	}

	b0 := byte(this.frameType)
	if final {
		b0 |= finalBit
	}

	b1 := byte(0)
	framePos := 0
	framePos = 4

	switch {
	case length >= 65536:
		request.writeBuf[framePos] = b0
		request.writeBuf[framePos+1] = b1 | 127
		binary.BigEndian.PutUint64(request.writeBuf[framePos+2:], uint64(length))
	case length > 125:
		framePos += 6
		request.writeBuf[framePos] = b0
		request.writeBuf[framePos+1] = b1 | 126
		binary.BigEndian.PutUint16(request.writeBuf[framePos+2:], uint16(length))
	default:
		framePos += 8
		request.writeBuf[framePos] = b0
		request.writeBuf[framePos+1] = b1 | byte(length)
	}

	err := request.write(this.frameType, request.writeDeadline, request.writeBuf[framePos:this.pos], extra)
	if err != nil {
		return this.endMessage(err)
	}

	if final {
		this.endMessage(errWriteClosed)
		return nil
	}

	this.pos = maxFrameHeaderSize
	this.frameType = continuationFrame

	return nil
}

func (this *frameWriter) ncopy(max int) (int, error) {
	n := len(this.request.writeBuf) - this.pos
	if n <= 0 {
		if err := this.flushFrame(false, nil); err != nil {
			return 0, err
		}

		n = len(this.request.writeBuf) - this.pos
	}

	if n > max {
		n = max
	}

	return n, nil
}

func (this *frameWriter) Write(p []byte) (int, error) {
	if this.err != nil {
		return 0, this.err
	}

	if len(p) > 2*len(this.request.writeBuf) {
		err := this.flushFrame(false, p)
		if err != nil {
			return 0, err
		}
		return len(p), nil
	}

	nn := len(p)
	for len(p) > 0 {
		n, err := this.ncopy(len(p))
		if err != nil {
			return 0, err
		}
		copy(this.request.writeBuf[this.pos:], p[:n])
		this.pos += n
		p = p[n:]
	}

	return nn, nil
}

func (this *frameWriter) WriteString(p string) (int, error) {
	if this.err != nil {
		return 0, this.err
	}

	nn := len(p)
	for len(p) > 0 {
		n, err := this.ncopy(len(p))
		if err != nil {
			return 0, err
		}
		copy(this.request.writeBuf[this.pos:], p[:n])
		this.pos += n
		p = p[n:]
	}

	return nn, nil
}

func (this *frameWriter) ReadFrom(r io.Reader) (nn int64, err error) {
	if this.err != nil {
		return 0, this.err
	}

	for {
		if this.pos == len(this.request.writeBuf) {
			err = this.flushFrame(false, nil)
			if err != nil {
				break
			}
		}

		var n int
		n, err = r.Read(this.request.writeBuf[this.pos:])
		this.pos += n
		nn += int64(n)

		if err != nil {
			if err == io.EOF {
				err = nil
			}

			break
		}
	}

	return nn, err
}

func (this *frameWriter) Close() error {
	if this.err != nil {
		return this.err
	}

	return this.flushFrame(true, nil)
}


type frameReader struct {
	request *Request
}

func (this *frameReader) Read(b []byte) (int, error) {
	request := this.request
	if request.frameReader != this {
		return 0, io.EOF
	}

	for request.readErr == nil {

		if request.readRemaining > 0 {
			if int64(len(b)) > request.readRemaining {
				b = b[:request.readRemaining]
			}

			n, err := request.br.Read(b)
			request.readErr = hideTempErr(err)
			request.readMaskPos = maskBytes(request.readMaskKey, request.readMaskPos, b[:n])

			request.readRemaining -= int64(n)
			if request.readRemaining > 0 && request.readErr == io.EOF {
				request.readErr = errUnexpectedEOF
			}

			return n, request.readErr
		}

		if request.readFinal {
			request.frameReader = nil
			return 0, io.EOF
		}

		frameType, err := request.advanceFrame()

		switch {
		case err != nil:
			request.readErr = hideTempErr(err)
		case frameType == TextMessage:
			request.readErr = errors.New("websocket: internal error, unexpected text or binary in Reader")
		}
	}

	err := request.readErr
	if err == io.EOF && request.frameReader == this {
		err = errUnexpectedEOF
	}

	return 0, err
}

func (r *frameReader) Close() error {
	return nil
}
