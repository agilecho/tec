package mysql

import (
	"errors"
	"fmt"
	"log"
	"os"
)

var (
	ErrInvalidConn       = errors.New("invalid connection")
	ErrMalformPkt        = errors.New("malformed packet")
	ErrUnknownPlugin     = errors.New("this authentication plugin is not supported")
	ErrOldProtocol       = errors.New("MySQL server does not support required protocol 41+")
	ErrPktSync           = errors.New("commands out of sync. You can't run this command now")
	ErrPktSyncMul        = errors.New("commands out of sync. Did you run multiple statements at once?")
	ErrPktTooLarge       = errors.New("packet for query is too large. Try adjusting the 'max_allowed_packet' variable on the server")
	ErrBusyBuffer        = errors.New("busy buffer")
	errBadConnNoWrite = errors.New("bad connection")
)

var errLog = Logger(log.New(os.Stderr, "[mysql] ", log.Ldate|log.Ltime|log.Lshortfile))

type Logger interface {
	Print(v ...interface{})
}

type MySQLError struct {
	Number  uint16
	Message string
}

func (me *MySQLError) Error() string {
	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}
