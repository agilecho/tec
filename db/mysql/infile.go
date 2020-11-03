package mysql

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

var (
	fileRegister       map[string]bool
	fileRegisterLock   sync.RWMutex
	readerRegister     map[string]func() io.Reader
	readerRegisterLock sync.RWMutex
)

func deferredClose(err *error, closer io.Closer) {
	closeErr := closer.Close()
	if *err == nil {
		*err = closeErr
	}
}

func (mc *mysqlConn) handleInFileRequest(name string) (err error) {
	var rdr io.Reader
	var data []byte
	packetSize := 16 * 1024
	if mc.maxWriteSize < packetSize {
		packetSize = mc.maxWriteSize
	}

	if idx := strings.Index(name, "Reader::"); idx == 0 || (idx > 0 && name[idx-1] == '/') {
		name = name[idx+8:]

		readerRegisterLock.RLock()
		handler, inMap := readerRegister[name]
		readerRegisterLock.RUnlock()

		if inMap {
			rdr = handler()
			if rdr != nil {
				if cl, ok := rdr.(io.Closer); ok {
					defer deferredClose(&err, cl)
				}
			} else {
				err = fmt.Errorf("Reader '%s' is <nil>", name)
			}
		} else {
			err = fmt.Errorf("Reader '%s' is not registered", name)
		}
	} else {
		name = strings.Trim(name, `"`)
		fileRegisterLock.RLock()
		fr := fileRegister[name]
		fileRegisterLock.RUnlock()
		if mc.cfg.AllowAllFiles || fr {
			var file *os.File
			var fi os.FileInfo

			if file, err = os.Open(name); err == nil {
				defer deferredClose(&err, file)

				// get file size
				if fi, err = file.Stat(); err == nil {
					rdr = file
					if fileSize := int(fi.Size()); fileSize < packetSize {
						packetSize = fileSize
					}
				}
			}
		} else {
			err = fmt.Errorf("local file '%s' is not registered", name)
		}
	}

	if err == nil && packetSize > 0 {
		data := make([]byte, 4+packetSize)
		var n int
		for err == nil {
			n, err = rdr.Read(data[4:])
			if n > 0 {
				if ioErr := mc.writePacket(data[:4+n]); ioErr != nil {
					return ioErr
				}
			}
		}
		if err == io.EOF {
			err = nil
		}
	}

	if data == nil {
		data = make([]byte, 4)
	}
	if ioErr := mc.writePacket(data[:4]); ioErr != nil {
		return ioErr
	}

	if err == nil {
		return mc.readResultOK()
	}

	mc.readPacket()
	return err
}
