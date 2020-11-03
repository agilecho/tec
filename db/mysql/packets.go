package mysql

import (
	"bytes"
	"crypto/tls"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

func (mc *mysqlConn) readPacket() ([]byte, error) {
	var prevData []byte
	for {
		data, err := mc.buf.readNext(4)
		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

		if data[3] != mc.sequence {
			if data[3] > mc.sequence {
				return nil, ErrPktSyncMul
			}
			return nil, ErrPktSync
		}
		mc.sequence++

		if pktLen == 0 {
			if prevData == nil {
				errLog.Print(ErrMalformPkt)
				mc.Close()
				return nil, ErrInvalidConn
			}

			return prevData, nil
		}

		data, err = mc.buf.readNext(pktLen)
		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		if pktLen < maxPacketSize {
			if prevData == nil {
				return data, nil
			}

			return append(prevData, data...), nil
		}

		prevData = append(prevData, data...)
	}
}

func (mc *mysqlConn) writePacket(data []byte) error {
	pktLen := len(data) - 4

	if pktLen > mc.maxAllowedPacket {
		return ErrPktTooLarge
	}

	if mc.reset {
		mc.reset = false
		conn := mc.netConn
		if mc.rawConn != nil {
			conn = mc.rawConn
		}
		var err error
		if mc.cfg.ReadTimeout != 0 {
			err = conn.SetReadDeadline(time.Time{})
		}
		if err != nil {
			errLog.Print("closing bad idle connection: ", err)
			mc.Close()
			return driver.ErrBadConn
		}
	}

	for {
		var size int
		if pktLen >= maxPacketSize {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = maxPacketSize
		} else {
			data[0] = byte(pktLen)
			data[1] = byte(pktLen >> 8)
			data[2] = byte(pktLen >> 16)
			size = pktLen
		}
		data[3] = mc.sequence

		if mc.writeTimeout > 0 {
			if err := mc.netConn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
				return err
			}
		}

		n, err := mc.netConn.Write(data[:4+size])
		if err == nil && n == 4+size {
			mc.sequence++
			if size != maxPacketSize {
				return nil
			}
			pktLen -= size
			data = data[size:]
			continue
		}

		if err == nil {
			mc.cleanup()
			errLog.Print(ErrMalformPkt)
		} else {
			if cerr := mc.canceled.Value(); cerr != nil {
				return cerr
			}
			if n == 0 && pktLen == len(data)-4 {
				return errBadConnNoWrite
			}
			mc.cleanup()
			errLog.Print(err)
		}
		return ErrInvalidConn
	}
}

func (mc *mysqlConn) readHandshakePacket() (data []byte, plugin string, err error) {
	data, err = mc.readPacket()
	if err != nil {
		if err == ErrInvalidConn {
			return nil, "", driver.ErrBadConn
		}
		return
	}

	if data[0] == iERR {
		return nil, "", mc.handleErrorPacket(data)
	}

	if data[0] < minProtocolVersion {
		return nil, "", fmt.Errorf(
			"unsupported protocol version %d. Version %d or higher is required",
			data[0],
			minProtocolVersion,
		)
	}

	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4
	authData := data[pos : pos+8]
	pos += 8 + 1

	mc.flags = clientFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
	if mc.flags&clientProtocol41 == 0 {
		return nil, "", ErrOldProtocol
	}
	if mc.flags&clientSSL == 0 && mc.cfg.tls != nil {
		if mc.cfg.TLSConfig == "preferred" {
			mc.cfg.tls = nil
		} else {
			return nil, "", ErrNoTLS
		}
	}
	pos += 2

	if len(data) > pos {
		pos += 1 + 2 + 2 + 1 + 10
		authData = append(authData, data[pos:pos+12]...)
		pos += 13

		if end := bytes.IndexByte(data[pos:], 0x00); end != -1 {
			plugin = string(data[pos : pos+end])
		} else {
			plugin = string(data[pos:])
		}

		var b [20]byte
		copy(b[:], authData)
		return b[:], plugin, nil
	}

	var b [8]byte
	copy(b[:], authData)
	return b[:], plugin, nil
}

func (mc *mysqlConn) writeHandshakeResponsePacket(authResp []byte, plugin string) error {
	clientFlags := clientProtocol41 |
		clientSecureConn |
		clientLongPassword |
		clientTransactions |
		clientLocalFiles |
		clientPluginAuth |
		clientMultiResults |
		mc.flags&clientLongFlag

	if mc.cfg.ClientFoundRows {
		clientFlags |= clientFoundRows
	}

	if mc.cfg.tls != nil {
		clientFlags |= clientSSL
	}

	if mc.cfg.MultiStatements {
		clientFlags |= clientMultiStatements
	}

	var authRespLEIBuf [9]byte
	authRespLen := len(authResp)
	authRespLEI := appendLengthEncodedInteger(authRespLEIBuf[:0], uint64(authRespLen))
	if len(authRespLEI) > 1 {
		clientFlags |= clientPluginAuthLenEncClientData
	}

	pktLen := 4 + 4 + 1 + 23 + len(mc.cfg.User) + 1 + len(authRespLEI) + len(authResp) + 21 + 1

	if n := len(mc.cfg.DBName); n > 0 {
		clientFlags |= clientConnectWithDB
		pktLen += n + 1
	}

	data, err := mc.buf.takeSmallBuffer(pktLen + 4)
	if err != nil {
		errLog.Print(err)
		return errBadConnNoWrite
	}

	data[4] = byte(clientFlags)
	data[5] = byte(clientFlags >> 8)
	data[6] = byte(clientFlags >> 16)
	data[7] = byte(clientFlags >> 24)
	data[8] = 0x00
	data[9] = 0x00
	data[10] = 0x00
	data[11] = 0x00

	var found bool
	data[12], found = collations[mc.cfg.Collation]
	if !found {
		return errors.New("unknown collation")
	}

	pos := 13
	for ; pos < 13+23; pos++ {
		data[pos] = 0
	}

	if mc.cfg.tls != nil {
		if err := mc.writePacket(data[:(4+4+1+23)+4]); err != nil {
			return err
		}

		tlsConn := tls.Client(mc.netConn, mc.cfg.tls)
		if err := tlsConn.Handshake(); err != nil {
			return err
		}
		mc.rawConn = mc.netConn
		mc.netConn = tlsConn
		mc.buf.nc = tlsConn
	}

	if len(mc.cfg.User) > 0 {
		pos += copy(data[pos:], mc.cfg.User)
	}
	data[pos] = 0x00
	pos++
	pos += copy(data[pos:], authRespLEI)
	pos += copy(data[pos:], authResp)

	if len(mc.cfg.DBName) > 0 {
		pos += copy(data[pos:], mc.cfg.DBName)
		data[pos] = 0x00
		pos++
	}

	pos += copy(data[pos:], plugin)
	data[pos] = 0x00
	pos++

	return mc.writePacket(data[:pos])
}

func (mc *mysqlConn) writeAuthSwitchPacket(authData []byte) error {
	pktLen := 4 + len(authData)
	data, err := mc.buf.takeSmallBuffer(pktLen)
	if err != nil {
		errLog.Print(err)
		return errBadConnNoWrite
	}

	copy(data[4:], authData)
	return mc.writePacket(data)
}

func (mc *mysqlConn) writeCommandPacket(command byte) error {
	mc.sequence = 0

	data, err := mc.buf.takeSmallBuffer(4 + 1)
	if err != nil {
		errLog.Print(err)
		return errBadConnNoWrite
	}

	data[4] = command

	return mc.writePacket(data)
}

func (mc *mysqlConn) writeCommandPacketStr(command byte, arg string) error {
	mc.sequence = 0

	pktLen := 1 + len(arg)
	data, err := mc.buf.takeBuffer(pktLen + 4)
	if err != nil {
		errLog.Print(err)
		return errBadConnNoWrite
	}

	data[4] = command
	copy(data[5:], arg)

	return mc.writePacket(data)
}

func (mc *mysqlConn) writeCommandPacketUint32(command byte, arg uint32) error {
	mc.sequence = 0

	data, err := mc.buf.takeSmallBuffer(4 + 1 + 4)
	if err != nil {
		errLog.Print(err)
		return errBadConnNoWrite
	}

	data[4] = command
	data[5] = byte(arg)
	data[6] = byte(arg >> 8)
	data[7] = byte(arg >> 16)
	data[8] = byte(arg >> 24)

	return mc.writePacket(data)
}

func (mc *mysqlConn) readAuthResult() ([]byte, string, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, "", err
	}

	switch data[0] {

	case iOK:
		return nil, "", mc.handleOkPacket(data)

	case iAuthMoreData:
		return data[1:], "", err

	case iEOF:
		if len(data) == 1 {
			return nil, "mysql_old_password", nil
		}
		pluginEndIndex := bytes.IndexByte(data, 0x00)
		if pluginEndIndex < 0 {
			return nil, "", ErrMalformPkt
		}
		plugin := string(data[1:pluginEndIndex])
		authData := data[pluginEndIndex+1:]
		return authData, plugin, nil

	default:
		return nil, "", mc.handleErrorPacket(data)
	}
}

func (mc *mysqlConn) readResultOK() error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] == iOK {
		return mc.handleOkPacket(data)
	}
	return mc.handleErrorPacket(data)
}

func (mc *mysqlConn) readResultSetHeaderPacket() (int, error) {
	data, err := mc.readPacket()
	if err == nil {
		switch data[0] {

		case iOK:
			return 0, mc.handleOkPacket(data)

		case iERR:
			return 0, mc.handleErrorPacket(data)

		case iLocalInFile:
			return 0, mc.handleInFileRequest(string(data[1:]))
		}

		// column count
		num, _, n := readLengthEncodedInteger(data)
		if n-len(data) == 0 {
			return int(num), nil
		}

		return 0, ErrMalformPkt
	}
	return 0, err
}

func (mc *mysqlConn) handleErrorPacket(data []byte) error {
	if data[0] != iERR {
		return ErrMalformPkt
	}

	errno := binary.LittleEndian.Uint16(data[1:3])

	if (errno == 1792 || errno == 1290) && mc.cfg.RejectReadOnly {
		mc.Close()
		return driver.ErrBadConn
	}

	pos := 3

	if data[3] == 0x23 {
		pos = 9
	}

	return &MySQLError{
		Number:  errno,
		Message: string(data[pos:]),
	}
}

func readStatus(b []byte) statusFlag {
	return statusFlag(b[0]) | statusFlag(b[1])<<8
}

func (mc *mysqlConn) handleOkPacket(data []byte) error {
	var n, m int

	mc.affectedRows, _, n = readLengthEncodedInteger(data[1:])
	mc.insertId, _, m = readLengthEncodedInteger(data[1+n:])
	mc.status = readStatus(data[1+n+m : 1+n+m+2])
	if mc.status&statusMoreResultsExists != 0 {
		return nil
	}

	return nil
}

func (mc *mysqlConn) readColumns(count int) ([]mysqlField, error) {
	columns := make([]mysqlField, count)

	for i := 0; ; i++ {
		data, err := mc.readPacket()
		if err != nil {
			return nil, err
		}

		if data[0] == iEOF && (len(data) == 5 || len(data) == 1) {
			if i == count {
				return columns, nil
			}
			return nil, fmt.Errorf("column count mismatch n:%d len:%d", count, len(columns))
		}

		pos, err := skipLengthEncodedString(data)
		if err != nil {
			return nil, err
		}

		n, err := skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		if mc.cfg.ColumnsWithAlias {
			tableName, _, n, err := readLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			columns[i].tableName = string(tableName)
		} else {
			n, err = skipLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
		}

		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		name, _, n, err := readLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		columns[i].name = string(name)
		pos += n

		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		pos++

		columns[i].charSet = data[pos]
		pos += 2

		columns[i].length = binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		columns[i].fieldType = fieldType(data[pos])
		pos++

		columns[i].flags = fieldFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2

		columns[i].decimals = data[pos]
	}
}

func (rows *textRows) readRow(dest []driver.Value) error {
	mc := rows.mc

	if rows.rs.done {
		return io.EOF
	}

	data, err := mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] == iEOF && len(data) == 5 {
		rows.mc.status = readStatus(data[3:])
		rows.rs.done = true
		if !rows.HasNextResultSet() {
			rows.mc = nil
		}
		return io.EOF
	}
	if data[0] == iERR {
		rows.mc = nil
		return mc.handleErrorPacket(data)
	}

	var n int
	var isNull bool
	pos := 0

	for i := range dest {
		dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
		pos += n
		if err == nil {
			if !isNull {
				if !mc.parseTime {
					continue
				} else {
					switch rows.rs.columns[i].fieldType {
					case fieldTypeTimestamp, fieldTypeDateTime,
						fieldTypeDate, fieldTypeNewDate:
						dest[i], err = parseDateTime(
							dest[i].([]byte),
							mc.cfg.Loc,
						)
						if err == nil {
							continue
						}
					default:
						continue
					}
				}

			} else {
				dest[i] = nil
				continue
			}
		}
		return err
	}

	return nil
}

func (mc *mysqlConn) readUntilEOF() error {
	for {
		data, err := mc.readPacket()
		if err != nil {
			return err
		}

		switch data[0] {
		case iERR:
			return mc.handleErrorPacket(data)
		case iEOF:
			if len(data) == 5 {
				mc.status = readStatus(data[3:])
			}
			return nil
		}
	}
}

func (stmt *mysqlStmt) readPrepareResultPacket() (uint16, error) {
	data, err := stmt.mc.readPacket()
	if err == nil {
		if data[0] != iOK {
			return 0, stmt.mc.handleErrorPacket(data)
		}

		stmt.id = binary.LittleEndian.Uint32(data[1:5])
		columnCount := binary.LittleEndian.Uint16(data[5:7])
		stmt.paramCount = int(binary.LittleEndian.Uint16(data[7:9]))

		return columnCount, nil
	}
	return 0, err
}

func (stmt *mysqlStmt) writeCommandLongData(paramID int, arg []byte) error {
	maxLen := stmt.mc.maxAllowedPacket - 1
	pktLen := maxLen
	const dataOffset = 1 + 4 + 2

	data := make([]byte, 4+1+4+2+len(arg))

	copy(data[4+dataOffset:], arg)

	for argLen := len(arg); argLen > 0; argLen -= pktLen - dataOffset {
		if dataOffset+argLen < maxLen {
			pktLen = dataOffset + argLen
		}

		stmt.mc.sequence = 0
		data[4] = comStmtSendLongData
		data[5] = byte(stmt.id)
		data[6] = byte(stmt.id >> 8)
		data[7] = byte(stmt.id >> 16)
		data[8] = byte(stmt.id >> 24)
		data[9] = byte(paramID)
		data[10] = byte(paramID >> 8)

		err := stmt.mc.writePacket(data[:4+pktLen])
		if err == nil {
			data = data[pktLen-dataOffset:]
			continue
		}
		return err

	}

	stmt.mc.sequence = 0
	return nil
}

func (stmt *mysqlStmt) writeExecutePacket(args []driver.Value) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"argument count mismatch (got: %d; has: %d)",
			len(args),
			stmt.paramCount,
		)
	}

	const minPktLen = 4 + 1 + 4 + 1 + 4
	mc := stmt.mc

	longDataSize := mc.maxAllowedPacket / (stmt.paramCount + 1)
	if longDataSize < 64 {
		longDataSize = 64
	}

	mc.sequence = 0

	var data []byte
	var err error

	if len(args) == 0 {
		data, err = mc.buf.takeBuffer(minPktLen)
	} else {
		data, err = mc.buf.takeCompleteBuffer()
	}
	if err != nil {
		errLog.Print(err)
		return errBadConnNoWrite
	}

	data[4] = comStmtExecute
	data[5] = byte(stmt.id)
	data[6] = byte(stmt.id >> 8)
	data[7] = byte(stmt.id >> 16)
	data[8] = byte(stmt.id >> 24)
	data[9] = 0x00
	data[10] = 0x01
	data[11] = 0x00
	data[12] = 0x00
	data[13] = 0x00

	if len(args) > 0 {
		pos := minPktLen

		var nullMask []byte
		if maskLen, typesLen := (len(args)+7)/8, 1+2*len(args); pos+maskLen+typesLen >= cap(data) {
			tmp := make([]byte, pos+maskLen+typesLen)
			copy(tmp[:pos], data[:pos])
			data = tmp
			nullMask = data[pos : pos+maskLen]
			pos += maskLen
		} else {
			nullMask = data[pos : pos+maskLen]
			for i := range nullMask {
				nullMask[i] = 0
			}
			pos += maskLen
		}

		data[pos] = 0x01
		pos++
		paramTypes := data[pos:]
		pos += len(args) * 2

		paramValues := data[pos:pos]
		valuesCap := cap(paramValues)

		for i, arg := range args {
			if arg == nil {
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(fieldTypeNULL)
				paramTypes[i+i+1] = 0x00
				continue
			}

			if v, ok := arg.(json.RawMessage); ok {
				arg = []byte(v)
			}

			switch v := arg.(type) {
			case int64:
				paramTypes[i+i] = byte(fieldTypeLongLong)
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(uint64(v))...,
					)
				}

			case uint64:
				paramTypes[i+i] = byte(fieldTypeLongLong)
				paramTypes[i+i+1] = 0x80 // type is unsigned

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						uint64(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(uint64(v))...,
					)
				}

			case float64:
				paramTypes[i+i] = byte(fieldTypeDouble)
				paramTypes[i+i+1] = 0x00

				if cap(paramValues)-len(paramValues)-8 >= 0 {
					paramValues = paramValues[:len(paramValues)+8]
					binary.LittleEndian.PutUint64(
						paramValues[len(paramValues)-8:],
						math.Float64bits(v),
					)
				} else {
					paramValues = append(paramValues,
						uint64ToBytes(math.Float64bits(v))...,
					)
				}

			case bool:
				paramTypes[i+i] = byte(fieldTypeTiny)
				paramTypes[i+i+1] = 0x00

				if v {
					paramValues = append(paramValues, 0x01)
				} else {
					paramValues = append(paramValues, 0x00)
				}

			case []byte:
				if v != nil {
					paramTypes[i+i] = byte(fieldTypeString)
					paramTypes[i+i+1] = 0x00

					if len(v) < longDataSize {
						paramValues = appendLengthEncodedInteger(paramValues,
							uint64(len(v)),
						)
						paramValues = append(paramValues, v...)
					} else {
						if err := stmt.writeCommandLongData(i, v); err != nil {
							return err
						}
					}
					continue
				}

				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(fieldTypeNULL)
				paramTypes[i+i+1] = 0x00

			case string:
				paramTypes[i+i] = byte(fieldTypeString)
				paramTypes[i+i+1] = 0x00

				if len(v) < longDataSize {
					paramValues = appendLengthEncodedInteger(paramValues,
						uint64(len(v)),
					)
					paramValues = append(paramValues, v...)
				} else {
					if err := stmt.writeCommandLongData(i, []byte(v)); err != nil {
						return err
					}
				}

			case time.Time:
				paramTypes[i+i] = byte(fieldTypeString)
				paramTypes[i+i+1] = 0x00

				var a [64]byte
				var b = a[:0]

				if v.IsZero() {
					b = append(b, "0000-00-00"...)
				} else {
					b, err = appendDateTime(b, v.In(mc.cfg.Loc))
					if err != nil {
						return err
					}
				}

				paramValues = appendLengthEncodedInteger(paramValues,
					uint64(len(b)),
				)
				paramValues = append(paramValues, b...)

			default:
				return fmt.Errorf("cannot convert type: %T", arg)
			}
		}

		if valuesCap != cap(paramValues) {
			data = append(data[:pos], paramValues...)
			if err = mc.buf.store(data); err != nil {
				errLog.Print(err)
				return errBadConnNoWrite
			}
		}

		pos += len(paramValues)
		data = data[:pos]
	}

	return mc.writePacket(data)
}

func (mc *mysqlConn) discardResults() error {
	for mc.status&statusMoreResultsExists != 0 {
		resLen, err := mc.readResultSetHeaderPacket()
		if err != nil {
			return err
		}
		if resLen > 0 {
			if err := mc.readUntilEOF(); err != nil {
				return err
			}
			if err := mc.readUntilEOF(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rows *binaryRows) readRow(dest []driver.Value) error {
	data, err := rows.mc.readPacket()
	if err != nil {
		return err
	}

	if data[0] != iOK {
		if data[0] == iEOF && len(data) == 5 {
			rows.mc.status = readStatus(data[3:])
			rows.rs.done = true
			if !rows.HasNextResultSet() {
				rows.mc = nil
			}
			return io.EOF
		}
		mc := rows.mc
		rows.mc = nil
		return mc.handleErrorPacket(data)
	}

	pos := 1 + (len(dest)+7+2)>>3
	nullMask := data[1:pos]

	for i := range dest {
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		switch rows.rs.columns[i].fieldType {
		case fieldTypeNULL:
			dest[i] = nil
			continue
		case fieldTypeTiny:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(data[pos])
			} else {
				dest[i] = int64(int8(data[pos]))
			}
			pos++
			continue

		case fieldTypeShort, fieldTypeYear:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
			} else {
				dest[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
			}
			pos += 2
			continue

		case fieldTypeInt24, fieldTypeLong:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			} else {
				dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
			}
			pos += 4
			continue

		case fieldTypeLongLong:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				val := binary.LittleEndian.Uint64(data[pos : pos+8])
				if val > math.MaxInt64 {
					dest[i] = uint64ToString(val)
				} else {
					dest[i] = int64(val)
				}
			} else {
				dest[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			}
			pos += 8
			continue

		case fieldTypeFloat:
			dest[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			continue

		case fieldTypeDouble:
			dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			continue
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON:
			var isNull bool
			var n int
			dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return err

		case
			fieldTypeDate, fieldTypeNewDate,
			fieldTypeTime,
			fieldTypeTimestamp, fieldTypeDateTime:

			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			switch {
			case isNull:
				dest[i] = nil
				continue
			case rows.rs.columns[i].fieldType == fieldTypeTime:
				var dstlen uint8
				switch decimals := rows.rs.columns[i].decimals; decimals {
				case 0x00, 0x1f:
					dstlen = 8
				case 1, 2, 3, 4, 5, 6:
					dstlen = 8 + 1 + decimals
				default:
					return fmt.Errorf(
						"protocol error, illegal decimals value %d",
						rows.rs.columns[i].decimals,
					)
				}
				dest[i], err = formatBinaryTime(data[pos:pos+int(num)], dstlen)
			case rows.mc.parseTime:
				dest[i], err = parseBinaryDateTime(num, data[pos:], rows.mc.cfg.Loc)
			default:
				var dstlen uint8
				if rows.rs.columns[i].fieldType == fieldTypeDate {
					dstlen = 10
				} else {
					switch decimals := rows.rs.columns[i].decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 19
					case 1, 2, 3, 4, 5, 6:
						dstlen = 19 + 1 + decimals
					default:
						return fmt.Errorf(
							"protocol error, illegal decimals value %d",
							rows.rs.columns[i].decimals,
						)
					}
				}
				dest[i], err = formatBinaryDateTime(data[pos:pos+int(num)], dstlen)
			}

			if err == nil {
				pos += int(num)
				continue
			} else {
				return err
			}
		default:
			return fmt.Errorf("unknown field type %d", rows.rs.columns[i].fieldType)
		}
	}

	return nil
}
