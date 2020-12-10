package db

import (
	"database/sql"
	"reflect"
	"strings"
	"time"
)

func extractStructFun(model interface{}) func(rows *sql.Rows) {
	return func(rows *sql.Rows) {
		modelType := reflect.TypeOf(model)
		modelVal := reflect.ValueOf(model)

		if modelType.Kind() != reflect.Ptr {
			return
		}

		stTypeInd := modelType.Elem()

		if rows == nil {
			return
		}

		defer rows.Close()

		v := reflect.New(stTypeInd)

		tagList := extractTagInfo(v)
		if tagList == nil {
			return
		}

		fields, err := rows.Columns()
		if err != nil {
			return
		}

		refs := make([]interface{}, len(fields))

		for i, field := range fields {
			if f, ok := tagList[field]; ok {
				refs[i] = f.Addr().Interface()
			} else {
				refs[i] = new(interface{})
			}
		}

		if !rows.Next() {
			return
		}

		if err := rows.Scan(refs...); err != nil {
			return
		}

		modelVal.Elem().Set(v.Elem())

		return
	}
}

func extractStructListFun(models []interface{}) func(rows *sql.Rows) {
	return nil
}

func extractTagInfo(model interface{}) map[string]reflect.Value {
	stVal := reflect.Indirect(reflect.ValueOf(model))

	if stVal.Kind() != reflect.Struct {
		return nil
	}

	tagList := make(map[string]reflect.Value)

	for i := 0; i < stVal.NumField(); i++ {
		v := stVal.Field(i)

		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				var typ reflect.Type
				if v.Type().Kind() == reflect.Ptr {
					typ = v.Type().Elem()
				} else {
					typ = v.Type()
				}
				vv := reflect.New(typ)
				v.Set(vv)
			}

			if v.Elem().Kind() == reflect.Struct {
				t := extractTagInfo(v.Elem())
				if t == nil {
					return nil
				}

				for k, ptr := range t {
					if _, ok := tagList[k]; ok {
						return nil
					}

					tagList[k] = ptr
				}
			}
		} else if v.Kind() == reflect.Map && v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		} else if v.Kind() == reflect.Struct {
			var ignore bool
			switch v.Interface().(type) {
			case time.Time:
				ignore = true
			case sql.NullTime:
				ignore = true
			case sql.NullString:
				ignore = true
			case sql.NullBool:
				ignore = true
			case sql.NullInt64:
				ignore = true
			case sql.NullInt32:
				ignore = true
			case sql.NullFloat64:
				ignore = true
			}

			if !ignore {
				t := extractTagInfo(v)
				if t == nil {
					return nil
				}

				for k, ptr := range t {
					if _, ok := tagList[k]; ok {
						return nil
					}
					tagList[k] = ptr
				}
			}
		}

		tagName := stVal.Type().Field(i).Tag.Get("db")
		if tagName != "" {
			attr := strings.Split(tagName, ";")
			column := attr[0]
			if _, ok := tagList[column]; ok {
				return nil
			}

			tagList[column] = v
		}
	}

	return tagList
}

func extractRow(tagInfo map[string]reflect.Value) Row {
	return nil
}