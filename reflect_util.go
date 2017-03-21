package picosql

import (
	"reflect"
	"time"

	"errors"
	"strconv"
)

func setValue(field reflect.Value, v interface{}) error {
	switch nv := v.(type) {
	case string:
		field.SetString(nv)
	case int:
		if field.Kind() == reflect.Bool {
			field.SetBool(nv == 1)
		} else {
			field.SetInt(int64(nv))
		}
	case int64:
		if field.Kind() == reflect.Bool {
			field.SetBool(nv == 1)
		} else {
			field.SetInt(nv)
		}
	case float64:
		field.SetFloat(nv)
	case time.Time:
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&nv))
			break
		}
		field.Set(reflect.ValueOf(nv))
	case bool:
		field.SetBool(nv)
	case []byte:

		if field.Kind() == reflect.String {
			field.SetString(string(nv))
		} else if field.Kind() == reflect.Slice {
			field.SetBytes(nv)
		} else if field.Kind() == reflect.Int {
			str := string(nv)
			i, _ := strconv.Atoi(str)
			field.SetInt(int64(i))
		} else if field.Kind() == reflect.Int64 {
			str := string(nv)
			i, _ := strconv.Atoi(str)
			field.SetInt(int64(i))
		} else if field.Kind() == reflect.Float64 {
			str := string(nv)
			i, _ := strconv.ParseFloat(str, 64)
			field.SetFloat(i)
		} else {
			return errors.New("Invalid Field type for bytes:")
		}
	}
	return nil
}
