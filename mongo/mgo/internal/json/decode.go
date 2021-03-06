package json

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

type Unmarshaler interface {
	UnmarshalJSON([]byte) error
}

type UnmarshalTypeError struct {
	Value  string
	Type   reflect.Type
	Offset int64
}

func (e *UnmarshalTypeError) Error() string {
	return "json: cannot unmarshal " + e.Value + " into Go value of type " + e.Type.String()
}

type UnmarshalFieldError struct {
	Key   string
	Type  reflect.Type
	Field reflect.StructField
}

func (e *UnmarshalFieldError) Error() string {
	return "json: cannot unmarshal object key " + strconv.Quote(e.Key) + " into unexported field " + e.Field.Name + " of type " + e.Type.String()
}

type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "json: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "json: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "json: Unmarshal(nil " + e.Type.String() + ")"
}

func (d *decodeState) unmarshal(v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(v)}
	}

	d.scan.reset()
	d.value(rv)
	return d.savedError
}

type Number string

func (n Number) String() string { return string(n) }

func (n Number) Float64() (float64, error) {
	return strconv.ParseFloat(string(n), 64)
}

func (n Number) Int64() (int64, error) {
	return strconv.ParseInt(string(n), 10, 64)
}

func isValidNumber(s string) bool {
	if s == "" {
		return false
	}

	if s[0] == '-' {
		s = s[1:]
		if s == "" {
			return false
		}
	}

	switch {
	default:
		return false

	case s[0] == '0':
		s = s[1:]

	case '1' <= s[0] && s[0] <= '9':
		s = s[1:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
		}
	}

	if len(s) >= 2 && s[0] == '.' && '0' <= s[1] && s[1] <= '9' {
		s = s[2:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
		}
	}

	if len(s) >= 2 && (s[0] == 'e' || s[0] == 'E') {
		s = s[1:]
		if s[0] == '+' || s[0] == '-' {
			s = s[1:]
			if s == "" {
				return false
			}
		}
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
		}
	}

	return s == ""
}

type decodeState struct {
	data       []byte
	off        int
	scan       scanner
	nextscan   scanner
	savedError error
	useNumber  bool
	ext        Extension
}

var errPhase = errors.New("JSON decoder out of sync - data changing underfoot?")

func (d *decodeState) init(data []byte) *decodeState {
	d.data = data
	d.off = 0
	d.savedError = nil
	return d
}

func (d *decodeState) error(err error) {
	panic(err)
}

func (d *decodeState) saveError(err error) {
	if d.savedError == nil {
		d.savedError = err
	}
}

func (d *decodeState) next() []byte {
	c := d.data[d.off]
	item, rest, err := nextValue(d.data[d.off:], &d.nextscan)
	if err != nil {
		d.error(err)
	}
	d.off = len(d.data) - len(rest)

	if c == '{' {
		d.scan.step(&d.scan, '}')
	} else if c == '[' {
		d.scan.step(&d.scan, ']')
	} else {
		d.scan.step(&d.scan, '(')
		d.scan.step(&d.scan, ')')
	}

	return item
}

func (d *decodeState) scanWhile(op int) int {
	var newOp int
	for {
		if d.off >= len(d.data) {
			newOp = d.scan.eof()
			d.off = len(d.data) + 1
		} else {
			c := d.data[d.off]
			d.off++
			newOp = d.scan.step(&d.scan, c)
		}
		if newOp != op {
			break
		}
	}
	return newOp
}

func (d *decodeState) value(v reflect.Value) {
	if !v.IsValid() {
		_, rest, err := nextValue(d.data[d.off:], &d.nextscan)
		if err != nil {
			d.error(err)
		}
		d.off = len(d.data) - len(rest)

		if d.scan.redo {
			d.scan.redo = false
			d.scan.step = stateBeginValue
		}

		d.scan.step(&d.scan, '"')
		d.scan.step(&d.scan, '"')

		n := len(d.scan.parseState)
		if n > 0 && d.scan.parseState[n-1] == parseObjectKey {
			d.scan.step(&d.scan, ':')
			d.scan.step(&d.scan, '"')
			d.scan.step(&d.scan, '"')
			d.scan.step(&d.scan, '}')
		}

		return
	}

	switch op := d.scanWhile(scanSkipSpace); op {
	default:
		d.error(errPhase)

	case scanBeginArray:
		d.array(v)

	case scanBeginObject:
		d.object(v)

	case scanBeginLiteral:
		d.literal(v)

	case scanBeginName:
		d.name(v)
	}
}

type unquotedValue struct{}

func (d *decodeState) valueQuoted() interface{} {
	switch op := d.scanWhile(scanSkipSpace); op {
	default:
		d.error(errPhase)

	case scanBeginArray:
		d.array(reflect.Value{})

	case scanBeginObject:
		d.object(reflect.Value{})

	case scanBeginName:
		switch v := d.nameInterface().(type) {
		case nil, string:
			return v
		}

	case scanBeginLiteral:
		switch v := d.literalInterface().(type) {
		case nil, string:
			return v
		}
	}
	return unquotedValue{}
}

func (d *decodeState) indirect(v reflect.Value, decodingNull bool) (Unmarshaler, encoding.TextUnmarshaler, reflect.Value) {
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		v = v.Addr()
	}

	for {
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Ptr) {
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if v.Elem().Kind() != reflect.Ptr && decodingNull && v.CanSet() {
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		if v.Type().NumMethod() > 0 {
			if u, ok := v.Interface().(Unmarshaler); ok {
				return u, nil, v
			}
			if u, ok := v.Interface().(encoding.TextUnmarshaler); ok {
				return nil, u, v
			}
		}
		v = v.Elem()
	}

	return nil, nil, v
}

func (d *decodeState) array(v reflect.Value) {
	u, ut, pv := d.indirect(v, false)
	if u != nil {
		d.off--
		err := u.UnmarshalJSON(d.next())
		if err != nil {
			d.error(err)
		}
		return
	}
	if ut != nil {
		d.saveError(&UnmarshalTypeError{"array", v.Type(), int64(d.off)})
		d.off--
		d.next()
		return
	}

	v = pv

	switch v.Kind() {
	case reflect.Interface:
		if v.NumMethod() == 0 {
			v.Set(reflect.ValueOf(d.arrayInterface()))
			return
		}
		fallthrough
	default:
		d.saveError(&UnmarshalTypeError{"array", v.Type(), int64(d.off)})
		d.off--
		d.next()
		return
	case reflect.Array:
	case reflect.Slice:
		break
	}

	i := 0
	for {
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			break
		}

		d.off--
		d.scan.undo(op)

		if v.Kind() == reflect.Slice {
			if i >= v.Cap() {
				newcap := v.Cap() + v.Cap()/2
				if newcap < 4 {
					newcap = 4
				}
				newv := reflect.MakeSlice(v.Type(), v.Len(), newcap)
				reflect.Copy(newv, v)
				v.Set(newv)
			}
			if i >= v.Len() {
				v.SetLen(i + 1)
			}
		}

		if i < v.Len() {
			d.value(v.Index(i))
		} else {
			d.value(reflect.Value{})
		}
		i++

		op = d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			break
		}
		if op != scanArrayValue {
			d.error(errPhase)
		}
	}

	if i < v.Len() {
		if v.Kind() == reflect.Array {
			z := reflect.Zero(v.Type().Elem())
			for ; i < v.Len(); i++ {
				v.Index(i).Set(z)
			}
		} else {
			v.SetLen(i)
		}
	}
	if i == 0 && v.Kind() == reflect.Slice {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}
}

var nullLiteral = []byte("null")
var textUnmarshalerType = reflect.TypeOf(new(encoding.TextUnmarshaler)).Elem()

func (d *decodeState) object(v reflect.Value) {
	u, ut, pv := d.indirect(v, false)
	if d.storeKeyed(pv) {
		return
	}

	if u != nil {
		d.off--
		err := u.UnmarshalJSON(d.next())
		if err != nil {
			d.error(err)
		}
		return
	}

	if ut != nil {
		d.saveError(&UnmarshalTypeError{"object", v.Type(), int64(d.off)})
		d.off--
		d.next()
		return
	}

	v = pv
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		v.Set(reflect.ValueOf(d.objectInterface()))
		return
	}

	switch v.Kind() {
	case reflect.Map:
		t := v.Type()
		if t.Key().Kind() != reflect.String &&
			!reflect.PtrTo(t.Key()).Implements(textUnmarshalerType) {
			d.saveError(&UnmarshalTypeError{"object", v.Type(), int64(d.off)})
			d.off--
			d.next()
			return
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
	case reflect.Struct:

	default:
		d.saveError(&UnmarshalTypeError{"object", v.Type(), int64(d.off)})
		d.off--
		d.next()
		return
	}

	var mapElem reflect.Value

	empty := true
	for {
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndObject {
			if !empty && !d.ext.trailingCommas {
				d.syntaxError("beginning of object key string")
			}
			break
		}

		empty = false

		if op == scanBeginName {
			if !d.ext.unquotedKeys {
				d.syntaxError("beginning of object key string")
			}
		} else if op != scanBeginLiteral {
			d.error(errPhase)
		}
		unquotedKey := op == scanBeginName

		start := d.off - 1
		op = d.scanWhile(scanContinue)
		item := d.data[start : d.off-1]
		var key []byte
		if unquotedKey {
			key = item
		} else {
			var ok bool
			key, ok = unquoteBytes(item)
			if !ok {
				d.error(errPhase)
			}
		}

		var subv reflect.Value
		destring := false

		if v.Kind() == reflect.Map {
			elemType := v.Type().Elem()
			if !mapElem.IsValid() {
				mapElem = reflect.New(elemType).Elem()
			} else {
				mapElem.Set(reflect.Zero(elemType))
			}
			subv = mapElem
		} else {
			var f *field
			fields := cachedTypeFields(v.Type())
			for i := range fields {
				ff := &fields[i]
				if bytes.Equal(ff.nameBytes, key) {
					f = ff
					break
				}
				if f == nil && ff.equalFold(ff.nameBytes, key) {
					f = ff
				}
			}
			if f != nil {
				subv = v
				destring = f.quoted
				for _, i := range f.index {
					if subv.Kind() == reflect.Ptr {
						if subv.IsNil() {
							subv.Set(reflect.New(subv.Type().Elem()))
						}
						subv = subv.Elem()
					}
					subv = subv.Field(i)
				}
			}
		}

		if op == scanSkipSpace {
			op = d.scanWhile(scanSkipSpace)
		}

		if op != scanObjectKey {
			d.error(errPhase)
		}

		if destring {
			switch qv := d.valueQuoted().(type) {
			case nil:
				d.literalStore(nullLiteral, subv, false)
			case string:
				d.literalStore([]byte(qv), subv, true)
			default:
				d.saveError(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal unquoted value into %v", subv.Type()))
			}
		} else {
			d.value(subv)
		}

		if v.Kind() == reflect.Map {
			kt := v.Type().Key()
			var kv reflect.Value
			switch {
			case kt.Kind() == reflect.String:
				kv = reflect.ValueOf(key).Convert(v.Type().Key())
			case reflect.PtrTo(kt).Implements(textUnmarshalerType):
				kv = reflect.New(v.Type().Key())
				d.literalStore(item, kv, true)
				kv = kv.Elem()
			default:
				panic("json: Unexpected key type")
			}
			v.SetMapIndex(kv, subv)
		}

		op = d.scanWhile(scanSkipSpace)
		if op == scanEndObject {
			break
		}
		if op != scanObjectValue {
			d.error(errPhase)
		}
	}
}

func (d *decodeState) isNull(off int) bool {
	if off+4 >= len(d.data) || d.data[off] != 'n' || d.data[off+1] != 'u' || d.data[off+2] != 'l' || d.data[off+3] != 'l' {
		return false
	}
	d.nextscan.reset()
	for i, c := range d.data[off:] {
		if i > 4 {
			return false
		}
		switch d.nextscan.step(&d.nextscan, c) {
		case scanContinue, scanBeginName:
			continue
		}
		break
	}
	return true
}

func (d *decodeState) name(v reflect.Value) {
	if d.isNull(d.off-1) {
		d.literal(v)
		return
	}

	u, ut, pv := d.indirect(v, false)
	if d.storeKeyed(pv) {
		return
	}
	if u != nil {
		d.off--
		err := u.UnmarshalJSON(d.next())
		if err != nil {
			d.error(err)
		}
		return
	}
	if ut != nil {
		d.saveError(&UnmarshalTypeError{"object", v.Type(), int64(d.off)})
		d.off--
		d.next()
		return
	}

	v = pv

	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		out := d.nameInterface()
		if out == nil {
			v.Set(reflect.Zero(v.Type()))
		} else {
			v.Set(reflect.ValueOf(out))
		}
		return
	}

	nameStart := d.off - 1

	op := d.scanWhile(scanContinue)

	name := d.data[nameStart : d.off-1]
	if op != scanParam {
		d.off--
		d.scan.undo(op)
		if l, ok := d.convertLiteral(name); ok {
			d.storeValue(v, l)
			return
		}
		d.error(&SyntaxError{fmt.Sprintf("json: unknown constant %q", name), int64(d.off)})
	}

	funcName := string(name)
	funcData := d.ext.funcs[funcName]
	if funcData.key == "" {
		d.error(fmt.Errorf("json: unknown function %q", funcName))
	}

	switch v.Kind() {
	case reflect.Map:
		t := v.Type()
		if t.Key().Kind() != reflect.String &&
			!reflect.PtrTo(t.Key()).Implements(textUnmarshalerType) {
			d.saveError(&UnmarshalTypeError{"object", v.Type(), int64(d.off)})
			d.off--
			d.next()
			return
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
	case reflect.Struct:

	default:
		d.saveError(&UnmarshalTypeError{"object", v.Type(), int64(d.off)})
		d.off--
		d.next()
		return
	}

	key := []byte(funcData.key)
	if v.Kind() == reflect.Map {
		elemType := v.Type().Elem()
		v = reflect.New(elemType).Elem()
	} else {
		var f *field
		fields := cachedTypeFields(v.Type())
		for i := range fields {
			ff := &fields[i]
			if bytes.Equal(ff.nameBytes, key) {
				f = ff
				break
			}
			if f == nil && ff.equalFold(ff.nameBytes, key) {
				f = ff
			}
		}
		if f != nil {
			for _, i := range f.index {
				if v.Kind() == reflect.Ptr {
					if v.IsNil() {
						v.Set(reflect.New(v.Type().Elem()))
					}
					v = v.Elem()
				}
				v = v.Field(i)
			}
			if v.Kind() == reflect.Ptr {
				if v.IsNil() {
					v.Set(reflect.New(v.Type().Elem()))
				}
				v = v.Elem()
			}
		}
	}

	u, ut, pv = d.indirect(v, false)
	if u != nil {
		d.off = nameStart
		err := u.UnmarshalJSON(d.next())
		if err != nil {
			d.error(err)
		}
		return
	}

	var mapElem reflect.Value

	for i := 0; ; i++ {
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndParams {
			break
		}

		d.off--
		d.scan.undo(op)

		if i >= len(funcData.args) {
			d.error(fmt.Errorf("json: too many arguments for function %s", funcName))
		}
		key := []byte(funcData.args[i])

		var subv reflect.Value
		destring := false

		if v.Kind() == reflect.Map {
			elemType := v.Type().Elem()
			if !mapElem.IsValid() {
				mapElem = reflect.New(elemType).Elem()
			} else {
				mapElem.Set(reflect.Zero(elemType))
			}
			subv = mapElem
		} else {
			var f *field
			fields := cachedTypeFields(v.Type())
			for i := range fields {
				ff := &fields[i]
				if bytes.Equal(ff.nameBytes, key) {
					f = ff
					break
				}
				if f == nil && ff.equalFold(ff.nameBytes, key) {
					f = ff
				}
			}
			if f != nil {
				subv = v
				destring = f.quoted
				for _, i := range f.index {
					if subv.Kind() == reflect.Ptr {
						if subv.IsNil() {
							subv.Set(reflect.New(subv.Type().Elem()))
						}
						subv = subv.Elem()
					}
					subv = subv.Field(i)
				}
			}
		}

		if destring {
			switch qv := d.valueQuoted().(type) {
			case nil:
				d.literalStore(nullLiteral, subv, false)
			case string:
				d.literalStore([]byte(qv), subv, true)
			default:
				d.saveError(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal unquoted value into %v", subv.Type()))
			}
		} else {
			d.value(subv)
		}

		if v.Kind() == reflect.Map {
			kt := v.Type().Key()
			var kv reflect.Value
			switch {
			case kt.Kind() == reflect.String:
				kv = reflect.ValueOf(key).Convert(v.Type().Key())
			case reflect.PtrTo(kt).Implements(textUnmarshalerType):
				kv = reflect.New(v.Type().Key())
				d.literalStore(key, kv, true)
				kv = kv.Elem()
			default:
				panic("json: Unexpected key type")
			}
			v.SetMapIndex(kv, subv)
		}

		op = d.scanWhile(scanSkipSpace)
		if op == scanEndParams {
			break
		}
		if op != scanParam {
			d.error(errPhase)
		}
	}
}

func (d *decodeState) keyed() (interface{}, bool) {
	if len(d.ext.keyed) == 0 {
		return nil, false
	}

	unquote := false

	d.nextscan.reset()
	var start, end int
	for i, c := range d.data[d.off-1:] {
		switch op := d.nextscan.step(&d.nextscan, c); op {
		case scanSkipSpace, scanContinue, scanBeginObject:
			continue
		case scanBeginLiteral, scanBeginName:
			unquote = op == scanBeginLiteral
			start = i
			continue
		}
		end = i
		break
	}

	name := d.data[d.off-1+start : d.off-1+end]

	var key []byte
	var ok bool
	if unquote {
		key, ok = unquoteBytes(name)
		if !ok {
			d.error(errPhase)
		}
	} else {
		funcData, ok := d.ext.funcs[string(name)]
		if !ok {
			return nil, false
		}
		key = []byte(funcData.key)
	}

	decode, ok := d.ext.keyed[string(key)]
	if !ok {
		return nil, false
	}

	d.off--
	out, err := decode(d.next())
	if err != nil {
		d.error(err)
	}
	return out, true
}

func (d *decodeState) storeKeyed(v reflect.Value) bool {
	keyed, ok := d.keyed()
	if !ok {
		return false
	}
	d.storeValue(v, keyed)
	return true
}

var (
	trueBytes = []byte("true")
	falseBytes = []byte("false")
	nullBytes = []byte("null")
)

func (d *decodeState) storeValue(v reflect.Value, from interface{}) {
	switch from {
	case nil:
		d.literalStore(nullBytes, v, false)
		return
	case true:
		d.literalStore(trueBytes, v, false)
		return
	case false:
		d.literalStore(falseBytes, v, false)
		return
	}
	fromv := reflect.ValueOf(from)
	for fromv.Kind() == reflect.Ptr && !fromv.IsNil() {
		fromv = fromv.Elem()
	}
	fromt := fromv.Type()
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	vt := v.Type()
	if fromt.AssignableTo(vt) {
		v.Set(fromv)
	} else if fromt.ConvertibleTo(vt) {
		v.Set(fromv.Convert(vt))
	} else {
		d.saveError(&UnmarshalTypeError{"object", v.Type(), int64(d.off)})
	}
}

func (d *decodeState) convertLiteral(name []byte) (interface{}, bool) {
	if len(name) == 0 {
		return nil, false
	}
	switch name[0] {
	case 't':
		if bytes.Equal(name, trueBytes) {
			return true, true
		}
	case 'f':
		if bytes.Equal(name, falseBytes) {
			return false, true
		}
	case 'n':
		if bytes.Equal(name, nullBytes) {
			return nil, true
		}
	}
	if l, ok := d.ext.consts[string(name)]; ok {
		return l, true
	}
	return nil, false
}

func (d *decodeState) literal(v reflect.Value) {
	start := d.off - 1
	op := d.scanWhile(scanContinue)

	d.off--
	d.scan.undo(op)

	d.literalStore(d.data[start:d.off], v, false)
}

func (d *decodeState) convertNumber(s string) (interface{}, error) {
	if d.useNumber {
		return Number(s), nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, &UnmarshalTypeError{"number " + s, reflect.TypeOf(0.0), int64(d.off)}
	}
	return f, nil
}

var numberType = reflect.TypeOf(Number(""))

func (d *decodeState) literalStore(item []byte, v reflect.Value, fromQuoted bool) {
	if len(item) == 0 {
		d.saveError(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
		return
	}

	wantptr := item[0] == 'n'
	u, ut, pv := d.indirect(v, wantptr)
	if u != nil {
		err := u.UnmarshalJSON(item)
		if err != nil {
			d.error(err)
		}
		return
	}
	if ut != nil {
		if item[0] != '"' {
			if fromQuoted {
				d.saveError(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.saveError(&UnmarshalTypeError{"string", v.Type(), int64(d.off)})
			}
			return
		}
		s, ok := unquoteBytes(item)
		if !ok {
			if fromQuoted {
				d.error(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.error(errPhase)
			}
		}
		err := ut.UnmarshalText(s)
		if err != nil {
			d.error(err)
		}
		return
	}

	v = pv

	switch c := item[0]; c {
	case 'n':
		switch v.Kind() {
		case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice:
			v.Set(reflect.Zero(v.Type()))
		}
	case 't', 'f':
		value := c == 't'
		switch v.Kind() {
		default:
			if fromQuoted {
				d.saveError(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.saveError(&UnmarshalTypeError{"bool", v.Type(), int64(d.off)})
			}
		case reflect.Bool:
			v.SetBool(value)
		case reflect.Interface:
			if v.NumMethod() == 0 {
				v.Set(reflect.ValueOf(value))
			} else {
				d.saveError(&UnmarshalTypeError{"bool", v.Type(), int64(d.off)})
			}
		}

	case '"':
		s, ok := unquoteBytes(item)
		if !ok {
			if fromQuoted {
				d.error(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.error(errPhase)
			}
		}
		switch v.Kind() {
		default:
			d.saveError(&UnmarshalTypeError{"string", v.Type(), int64(d.off)})
		case reflect.Slice:
			if v.Type().Elem().Kind() != reflect.Uint8 {
				d.saveError(&UnmarshalTypeError{"string", v.Type(), int64(d.off)})
				break
			}
			b := make([]byte, base64.StdEncoding.DecodedLen(len(s)))
			n, err := base64.StdEncoding.Decode(b, s)
			if err != nil {
				d.saveError(err)
				break
			}
			v.SetBytes(b[:n])
		case reflect.String:
			v.SetString(string(s))
		case reflect.Interface:
			if v.NumMethod() == 0 {
				v.Set(reflect.ValueOf(string(s)))
			} else {
				d.saveError(&UnmarshalTypeError{"string", v.Type(), int64(d.off)})
			}
		}

	default:
		if c != '-' && (c < '0' || c > '9') {
			if fromQuoted {
				d.error(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.error(errPhase)
			}
		}
		s := string(item)
		switch v.Kind() {
		default:
			if v.Kind() == reflect.String && v.Type() == numberType {
				v.SetString(s)
				if !isValidNumber(s) {
					d.error(fmt.Errorf("json: invalid number literal, trying to unmarshal %q into Number", item))
				}
				break
			}
			if fromQuoted {
				d.error(fmt.Errorf("json: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
			} else {
				d.error(&UnmarshalTypeError{"number", v.Type(), int64(d.off)})
			}
		case reflect.Interface:
			n, err := d.convertNumber(s)
			if err != nil {
				d.saveError(err)
				break
			}
			if v.NumMethod() != 0 {
				d.saveError(&UnmarshalTypeError{"number", v.Type(), int64(d.off)})
				break
			}
			v.Set(reflect.ValueOf(n))

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil || v.OverflowInt(n) {
				d.saveError(&UnmarshalTypeError{"number " + s, v.Type(), int64(d.off)})
				break
			}
			v.SetInt(n)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil || v.OverflowUint(n) {
				d.saveError(&UnmarshalTypeError{"number " + s, v.Type(), int64(d.off)})
				break
			}
			v.SetUint(n)

		case reflect.Float32, reflect.Float64:
			n, err := strconv.ParseFloat(s, v.Type().Bits())
			if err != nil || v.OverflowFloat(n) {
				d.saveError(&UnmarshalTypeError{"number " + s, v.Type(), int64(d.off)})
				break
			}
			v.SetFloat(n)
		}
	}
}

func (d *decodeState) valueInterface() interface{} {
	switch d.scanWhile(scanSkipSpace) {
	default:
		d.error(errPhase)
		panic("unreachable")
	case scanBeginArray:
		return d.arrayInterface()
	case scanBeginObject:
		return d.objectInterface()
	case scanBeginLiteral:
		return d.literalInterface()
	case scanBeginName:
		return d.nameInterface()
	}
}

func (d *decodeState) syntaxError(expected string) {
	msg := fmt.Sprintf("invalid character '%c' looking for %s", d.data[d.off-1], expected)
	d.error(&SyntaxError{msg, int64(d.off)})
}

func (d *decodeState) arrayInterface() []interface{} {
	var v = make([]interface{}, 0)
	for {
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			if len(v) > 0 && !d.ext.trailingCommas {
				d.syntaxError("beginning of value")
			}
			break
		}

		d.off--
		d.scan.undo(op)

		v = append(v, d.valueInterface())

		op = d.scanWhile(scanSkipSpace)
		if op == scanEndArray {
			break
		}
		if op != scanArrayValue {
			d.error(errPhase)
		}
	}
	return v
}

func (d *decodeState) objectInterface() interface{} {
	v, ok := d.keyed()
	if ok {
		return v
	}

	m := make(map[string]interface{})
	for {
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndObject {
			if len(m) > 0 && !d.ext.trailingCommas {
				d.syntaxError("beginning of object key string")
			}
			break
		}
		if op == scanBeginName {
			if !d.ext.unquotedKeys {
				d.syntaxError("beginning of object key string")
			}
		} else if op != scanBeginLiteral {
			d.error(errPhase)
		}
		unquotedKey := op == scanBeginName

		start := d.off - 1
		op = d.scanWhile(scanContinue)
		item := d.data[start : d.off-1]
		var key string
		if unquotedKey {
			key = string(item)
		} else {
			var ok bool
			key, ok = unquote(item)
			if !ok {
				d.error(errPhase)
			}
		}

		if op == scanSkipSpace {
			op = d.scanWhile(scanSkipSpace)
		}
		if op != scanObjectKey {
			d.error(errPhase)
		}

		m[key] = d.valueInterface()

		op = d.scanWhile(scanSkipSpace)
		if op == scanEndObject {
			break
		}
		if op != scanObjectValue {
			d.error(errPhase)
		}
	}
	return m
}

func (d *decodeState) literalInterface() interface{} {
	start := d.off - 1
	op := d.scanWhile(scanContinue)

	d.off--
	d.scan.undo(op)
	item := d.data[start:d.off]

	switch c := item[0]; c {
	case 'n':
		return nil

	case 't', 'f':
		return c == 't'

	case '"':
		s, ok := unquote(item)
		if !ok {
			d.error(errPhase)
		}
		return s

	default:
		if c != '-' && (c < '0' || c > '9') {
			d.error(errPhase)
		}
		n, err := d.convertNumber(string(item))
		if err != nil {
			d.saveError(err)
		}
		return n
	}
}

func (d *decodeState) nameInterface() interface{} {
	v, ok := d.keyed()
	if ok {
		return v
	}

	nameStart := d.off - 1

	op := d.scanWhile(scanContinue)

	name := d.data[nameStart : d.off-1]
	if op != scanParam {
		d.off--
		d.scan.undo(op)
		if l, ok := d.convertLiteral(name); ok {
			return l
		}
		d.error(&SyntaxError{fmt.Sprintf("json: unknown constant %q", name), int64(d.off)})
	}

	funcName := string(name)
	funcData := d.ext.funcs[funcName]
	if funcData.key == "" {
		d.error(fmt.Errorf("json: unknown function %q", funcName))
	}

	m := make(map[string]interface{})
	for i := 0; ; i++ {
		op := d.scanWhile(scanSkipSpace)
		if op == scanEndParams {
			break
		}

		d.off--
		d.scan.undo(op)

		if i >= len(funcData.args) {
			d.error(fmt.Errorf("json: too many arguments for function %s", funcName))
		}
		m[funcData.args[i]] = d.valueInterface()
		op = d.scanWhile(scanSkipSpace)
		if op == scanEndParams {
			break
		}
		if op != scanParam {
			d.error(errPhase)
		}
	}
	return map[string]interface{}{funcData.key: m}
}

func getu4(s []byte) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	r, err := strconv.ParseUint(string(s[2:6]), 16, 64)
	if err != nil {
		return -1
	}
	return rune(r)
}

func unquote(s []byte) (t string, ok bool) {
	s, ok = unquoteBytes(s)
	t = string(s)
	return
}

func unquoteBytes(s []byte) (t []byte, ok bool) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return
	}

	s = s[1 : len(s)-1]

	r := 0
	for r < len(s) {
		c := s[r]
		if c == '\\' || c == '"' || c < ' ' {
			break
		}
		if c < utf8.RuneSelf {
			r++
			continue
		}
		rr, size := utf8.DecodeRune(s[r:])
		if rr == utf8.RuneError && size == 1 {
			break
		}
		r += size
	}

	if r == len(s) {
		return s, true
	}

	b := make([]byte, len(s)+2*utf8.UTFMax)
	w := copy(b, s[0:r])
	for r < len(s) {
		if w >= len(b)-2*utf8.UTFMax {
			nb := make([]byte, (len(b)+utf8.UTFMax)*2)
			copy(nb, b[0:w])
			b = nb
		}
		switch c := s[r]; {
		case c == '\\':
			r++
			if r >= len(s) {
				return
			}
			switch s[r] {
			default:
				return
			case '"', '\\', '/', '\'':
				b[w] = s[r]
				r++
				w++
			case 'b':
				b[w] = '\b'
				r++
				w++
			case 'f':
				b[w] = '\f'
				r++
				w++
			case 'n':
				b[w] = '\n'
				r++
				w++
			case 'r':
				b[w] = '\r'
				r++
				w++
			case 't':
				b[w] = '\t'
				r++
				w++
			case 'u':
				r--
				rr := getu4(s[r:])
				if rr < 0 {
					return
				}
				r += 6
				if utf16.IsSurrogate(rr) {
					rr1 := getu4(s[r:])
					if dec := utf16.DecodeRune(rr, rr1); dec != unicode.ReplacementChar {
						r += 6
						w += utf8.EncodeRune(b[w:], dec)
						break
					}
					rr = unicode.ReplacementChar
				}
				w += utf8.EncodeRune(b[w:], rr)
			}
		case c == '"', c < ' ':
			return
		case c < utf8.RuneSelf:
			b[w] = c
			r++
			w++
		default:
			rr, size := utf8.DecodeRune(s[r:])
			r += size
			w += utf8.EncodeRune(b[w:], rr)
		}
	}
	
	return b[0:w], true
}
