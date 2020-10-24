package json

import (
	"reflect"
)

type Extension struct {
	funcs  map[string]funcExt
	consts map[string]interface{}
	keyed  map[string]func([]byte) (interface{}, error)
	encode map[reflect.Type]func(v interface{}) ([]byte, error)

	unquotedKeys   bool
	trailingCommas bool
}

type funcExt struct {
	key  string
	args []string
}

func (dec *Decoder) Extend(ext *Extension) { dec.d.ext = *ext }

func (enc *Encoder) Extend(ext *Extension) { enc.ext = *ext }

func (e *Extension) Extend(ext *Extension) {
	for name, fext := range ext.funcs {
		e.DecodeFunc(name, fext.key, fext.args...)
	}
	for name, value := range ext.consts {
		e.DecodeConst(name, value)
	}
	for key, decode := range ext.keyed {
		e.DecodeKeyed(key, decode)
	}
	for typ, encode := range ext.encode {
		if e.encode == nil {
			e.encode = make(map[reflect.Type]func(v interface{}) ([]byte, error))
		}
		e.encode[typ] = encode
	}
}

func (e *Extension) DecodeFunc(name string, key string, args ...string) {
	if e.funcs == nil {
		e.funcs = make(map[string]funcExt)
	}
	e.funcs[name] = funcExt{key, args}
}

func (e *Extension) DecodeConst(name string, value interface{}) {
	if e.consts == nil {
		e.consts = make(map[string]interface{})
	}
	e.consts[name] = value
}

func (e *Extension) DecodeKeyed(key string, decode func(data []byte) (interface{}, error)) {
	if e.keyed == nil {
		e.keyed = make(map[string]func([]byte) (interface{}, error))
	}
	e.keyed[key] = decode
}

func (e *Extension) DecodeUnquotedKeys(accept bool) {
	e.unquotedKeys = accept
}

func (e *Extension) DecodeTrailingCommas(accept bool) {
	e.trailingCommas = accept
}

func (e *Extension) EncodeType(sample interface{}, encode func(v interface{}) ([]byte, error)) {
	if e.encode == nil {
		e.encode = make(map[reflect.Type]func(v interface{}) ([]byte, error))
	}
	e.encode[reflect.TypeOf(sample)] = encode
}
