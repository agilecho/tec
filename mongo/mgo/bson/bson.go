package bson

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Getter interface {
	GetBSON() (interface{}, error)
}

type Setter interface {
	SetBSON(raw Raw) error
}

var SetZero = errors.New("set to zero")

type M map[string]interface{}
type D []DocElem
type DocElem struct {
	Name  string
	Value interface{}
}

func (d D) Map() (m M) {
	m = make(M, len(d))
	for _, item := range d {
		m[item.Name] = item.Value
	}
	return m
}

type Raw struct {
	Kind byte
	Data []byte
}

type RawD []RawDocElem

type RawDocElem struct {
	Name  string
	Value Raw
}

type ObjectId string

func ObjectIdHex(s string) ObjectId {
	d, err := hex.DecodeString(s)
	if err != nil || len(d) != 12 {
		panic(fmt.Sprintf("invalid input to ObjectIdHex: %q", s))
	}
	return ObjectId(d)
}

func IsObjectIdHex(s string) bool {
	if len(s) != 24 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

var objectIdCounter uint32 = readRandomUint32()

func readRandomUint32() uint32 {
	return uint32(time.Now().UnixNano())
}

var machineId = readMachineId()
var processId = os.Getpid()

func readMachineId() []byte {
	var sum [3]byte
	id := sum[:]
	hostname, err1 := os.Hostname()
	if err1 != nil {
		n := uint32(time.Now().UnixNano())
		sum[0] = byte(n >> 0)
		sum[1] = byte(n >> 8)
		sum[2] = byte(n >> 16)
		return id
	}
	hw := md5.New()
	hw.Write([]byte(hostname))
	copy(id, hw.Sum(nil))
	return id
}

func NewObjectId() ObjectId {
	var b [12]byte
	binary.BigEndian.PutUint32(b[:], uint32(time.Now().Unix()))
	b[4] = machineId[0]
	b[5] = machineId[1]
	b[6] = machineId[2]
	b[7] = byte(processId >> 8)
	b[8] = byte(processId)
	i := atomic.AddUint32(&objectIdCounter, 1)
	b[9] = byte(i >> 16)
	b[10] = byte(i >> 8)
	b[11] = byte(i)
	return ObjectId(b[:])
}

func NewObjectIdWithTime(t time.Time) ObjectId {
	var b [12]byte
	binary.BigEndian.PutUint32(b[:4], uint32(t.Unix()))
	return ObjectId(string(b[:]))
}

func (id ObjectId) String() string {
	return fmt.Sprintf(`ObjectIdHex("%x")`, string(id))
}

func (id ObjectId) Hex() string {
	return hex.EncodeToString([]byte(id))
}

func (id ObjectId) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%x"`, string(id))), nil
}

var nullBytes = []byte("null")

func (id *ObjectId) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && (data[0] == '{' || data[0] == 'O') {
		var v struct {
			Id   json.RawMessage `json:"$oid"`
			Func struct {
				Id json.RawMessage
			} `json:"$oidFunc"`
		}
		err := jdec(data, &v)
		if err == nil {
			if len(v.Id) > 0 {
				data = []byte(v.Id)
			} else {
				data = []byte(v.Func.Id)
			}
		}
	}
	if len(data) == 2 && data[0] == '"' && data[1] == '"' || bytes.Equal(data, nullBytes) {
		*id = ""
		return nil
	}
	if len(data) != 26 || data[0] != '"' || data[25] != '"' {
		return errors.New(fmt.Sprintf("invalid ObjectId in JSON: %s", string(data)))
	}
	var buf [12]byte
	_, err := hex.Decode(buf[:], data[1:25])
	if err != nil {
		return errors.New(fmt.Sprintf("invalid ObjectId in JSON: %s (%s)", string(data), err))
	}
	*id = ObjectId(string(buf[:]))
	return nil
}

func (id ObjectId) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%x", string(id))), nil
}

func (id *ObjectId) UnmarshalText(data []byte) error {
	if len(data) == 1 && data[0] == ' ' || len(data) == 0 {
		*id = ""
		return nil
	}
	if len(data) != 24 {
		return fmt.Errorf("invalid ObjectId: %s", data)
	}
	var buf [12]byte
	_, err := hex.Decode(buf[:], data[:])
	if err != nil {
		return fmt.Errorf("invalid ObjectId: %s (%s)", data, err)
	}
	*id = ObjectId(string(buf[:]))
	return nil
}

func (id ObjectId) Valid() bool {
	return len(id) == 12
}

func (id ObjectId) byteSlice(start, end int) []byte {
	if len(id) != 12 {
		panic(fmt.Sprintf("invalid ObjectId: %q", string(id)))
	}
	return []byte(string(id)[start:end])
}

func (id ObjectId) Time() time.Time {
	secs := int64(binary.BigEndian.Uint32(id.byteSlice(0, 4)))
	return time.Unix(secs, 0)
}

func (id ObjectId) Machine() []byte {
	return id.byteSlice(4, 7)
}

func (id ObjectId) Pid() uint16 {
	return binary.BigEndian.Uint16(id.byteSlice(7, 9))
}

func (id ObjectId) Counter() int32 {
	b := id.byteSlice(9, 12)
	return int32(uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]))
}

type Symbol string

func Now() time.Time {
	return time.Unix(0, time.Now().UnixNano()/1e6*1e6)
}

type MongoTimestamp int64
type orderKey int64

var MaxKey = orderKey(1<<63 - 1)
var MinKey = orderKey(-1 << 63)

type undefined struct{}

var Undefined undefined

type Binary struct {
	Kind byte
	Data []byte
}

type RegEx struct {
	Pattern string
	Options string
}

type JavaScript struct {
	Code  string
	Scope interface{}
}

type DBPointer struct {
	Namespace string
	Id        ObjectId
}

const initialBufferSize = 64

func handleErr(err *error) {
	if r := recover(); r != nil {
		if _, ok := r.(runtime.Error); ok {
			panic(r)
		} else if _, ok := r.(externalPanic); ok {
			panic(r)
		} else if s, ok := r.(string); ok {
			*err = errors.New(s)
		} else if e, ok := r.(error); ok {
			*err = e
		} else {
			panic(r)
		}
	}
}

func Marshal(in interface{}) (out []byte, err error) {
	defer handleErr(&err)
	e := &encoder{make([]byte, 0, initialBufferSize)}
	e.addDoc(reflect.ValueOf(in))
	return e.out, nil
}

func Unmarshal(in []byte, out interface{}) (err error) {
	if raw, ok := out.(*Raw); ok {
		raw.Kind = 3
		raw.Data = in
		return nil
	}
	defer handleErr(&err)
	v := reflect.ValueOf(out)
	switch v.Kind() {
	case reflect.Ptr:
		fallthrough
	case reflect.Map:
		d := newDecoder(in)
		d.readDocTo(v)
	case reflect.Struct:
		return errors.New("Unmarshal can't deal with struct values. Use a pointer.")
	default:
		return errors.New("Unmarshal needs a map or a pointer to a struct.")
	}
	return nil
}

func (raw Raw) Unmarshal(out interface{}) (err error) {
	defer handleErr(&err)
	v := reflect.ValueOf(out)
	switch v.Kind() {
	case reflect.Ptr:
		v = v.Elem()
		fallthrough
	case reflect.Map:
		d := newDecoder(raw.Data)
		good := d.readElemTo(v, raw.Kind)
		if !good {
			return &TypeError{v.Type(), raw.Kind}
		}
	case reflect.Struct:
		return errors.New("Raw Unmarshal can't deal with struct values. Use a pointer.")
	default:
		return errors.New("Raw Unmarshal needs a map or a valid pointer.")
	}
	return nil
}

type TypeError struct {
	Type reflect.Type
	Kind byte
}

func (e *TypeError) Error() string {
	return fmt.Sprintf("BSON kind 0x%02x isn't compatible with type %s", e.Kind, e.Type.String())
}

type structInfo struct {
	FieldsMap  map[string]fieldInfo
	FieldsList []fieldInfo
	InlineMap  int
	Zero       reflect.Value
}

type fieldInfo struct {
	Key       string
	Num       int
	OmitEmpty bool
	MinSize   bool
	Inline    []int
}

var structMap = make(map[reflect.Type]*structInfo)
var structMapMutex sync.RWMutex

type externalPanic string

func (e externalPanic) String() string {
	return string(e)
}

func getStructInfo(st reflect.Type) (*structInfo, error) {
	structMapMutex.RLock()
	sinfo, found := structMap[st]
	structMapMutex.RUnlock()
	if found {
		return sinfo, nil
	}
	n := st.NumField()
	fieldsMap := make(map[string]fieldInfo)
	fieldsList := make([]fieldInfo, 0, n)
	inlineMap := -1
	for i := 0; i != n; i++ {
		field := st.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}

		info := fieldInfo{Num: i}

		tag := field.Tag.Get("bson")
		if tag == "" && strings.Index(string(field.Tag), ":") < 0 {
			tag = string(field.Tag)
		}
		if tag == "-" {
			continue
		}

		inline := false
		fields := strings.Split(tag, ",")
		if len(fields) > 1 {
			for _, flag := range fields[1:] {
				switch flag {
				case "omitempty":
					info.OmitEmpty = true
				case "minsize":
					info.MinSize = true
				case "inline":
					inline = true
				default:
					msg := fmt.Sprintf("Unsupported flag %q in tag %q of type %s", flag, tag, st)
					panic(externalPanic(msg))
				}
			}
			tag = fields[0]
		}

		if inline {
			switch field.Type.Kind() {
			case reflect.Map:
				if inlineMap >= 0 {
					return nil, errors.New("Multiple ,inline maps in struct " + st.String())
				}
				if field.Type.Key() != reflect.TypeOf("") {
					return nil, errors.New("Option ,inline needs a map with string keys in struct " + st.String())
				}
				inlineMap = info.Num
			case reflect.Struct:
				sinfo, err := getStructInfo(field.Type)
				if err != nil {
					return nil, err
				}
				for _, finfo := range sinfo.FieldsList {
					if _, found := fieldsMap[finfo.Key]; found {
						msg := "Duplicated key '" + finfo.Key + "' in struct " + st.String()
						return nil, errors.New(msg)
					}
					if finfo.Inline == nil {
						finfo.Inline = []int{i, finfo.Num}
					} else {
						finfo.Inline = append([]int{i}, finfo.Inline...)
					}
					fieldsMap[finfo.Key] = finfo
					fieldsList = append(fieldsList, finfo)
				}
			default:
				panic("Option ,inline needs a struct value or map field")
			}
			continue
		}

		if tag != "" {
			info.Key = tag
		} else {
			info.Key = strings.ToLower(field.Name)
		}

		if _, found = fieldsMap[info.Key]; found {
			msg := "Duplicated key '" + info.Key + "' in struct " + st.String()
			return nil, errors.New(msg)
		}

		fieldsList = append(fieldsList, info)
		fieldsMap[info.Key] = info
	}
	sinfo = &structInfo{
		fieldsMap,
		fieldsList,
		inlineMap,
		reflect.New(st).Elem(),
	}
	structMapMutex.Lock()
	structMap[st] = sinfo
	structMapMutex.Unlock()
	return sinfo, nil
}
