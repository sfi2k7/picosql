package picosql

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"
)

const maxRetries = 3

var (
	connectionError = errors.New("No Connection is available")
)

type Sql struct {
	IsOpen  bool
	retries int
	db      *sql.DB
	cs      string
	driver  string
}

func (m *Sql) open() error {
	if m.IsOpen && m.db.Ping() == nil {
		return nil
	}

	db, err := sql.Open(m.driver, m.cs)
	if err != nil {
		m.IsOpen = false
		return nil
	}

	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	m.db = db
	err = db.Ping()
	if err != nil {
		m.IsOpen = false
		return err
	}

	m.IsOpen = true
	return nil
}

func (m *Sql) Count(query string, args ...interface{}) (int64, error) {

	m.open()

	if !m.IsOpen {
		return 0, connectionError
	}

	res := m.db.QueryRow(query, args...)

	var count int64
	err := res.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (m *Sql) Insert(query string, args ...interface{}) (int64, error) {
	m.open()

	if !m.IsOpen {
		return 0, connectionError
	}

	res, err := m.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	lastId, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastId, nil
}

func (m *Sql) Update(query string, args ...interface{}) (int64, error) {
	m.open()

	if !m.IsOpen {
		return 0, connectionError
	}

	res, err := m.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return affected, nil
}

func (m *Sql) Select(targets interface{}, query string, args ...interface{}) error {
	m.open()

	if !m.IsOpen {
		return connectionError
	}

	res, err := m.db.Query(query, args...)
	if err != nil {
		return err
	}

	defer res.Close()

	sliceValue := reflect.ValueOf(targets).Elem()
	itemType := sliceValue.Type()
	elementType := itemType.Elem()

	for res.Next() {
		columns, err := res.Columns()
		if err != nil {
			return err
		}

		result := make([]interface{}, len(columns))

		for x := 0; x < len(result); x++ {
			result[x] = new(interface{})
		}

		err = res.Scan(result...)

		if err != nil {
			return err
		}

		target := reflect.New(elementType.Elem())
		v := target.Elem()
		//v := SetAble // reflect.ValueOf(target)
		//v = v.Elem()
		//fmt.Println("V", v)
		t := v.Type()

		for i, c := range columns {
			cv := *(result[i].(*interface{}))
			var field reflect.Value
			for x := 0; x < v.NumField(); x++ {
				tag := t.Field(x).Tag.Get("db")
				if len(tag) == 0 {
					continue
				}
				splitted := strings.Split(tag, ",")
				if len(splitted) == 0 {
					continue
				}

				if c != splitted[0] {
					continue
				}

				field = v.Field(i)
				break
			}

			if !field.IsValid() {
				continue
			}

			switch nv := cv.(type) {
			case string:
				field.SetString(nv)
			case int:
				field.SetInt(int64(nv))
			case int64:
				field.SetInt(nv)
			case time.Time:
				field.Set(reflect.ValueOf(nv))
			case bool:
				field.SetBool(nv)
			case []byte:
				field.SetBytes(nv)
			}
		}
		sliceValue.Set(reflect.Append(sliceValue, target))
	}
	return nil
}

func (m *Sql) Get(target interface{}, query string, args ...interface{}) error {
	m.open()

	if !m.IsOpen {
		return connectionError
	}

	res, err := m.db.Query(query, args...)
	if err != nil {
		return err
	}

	defer res.Close()
	if !res.Next() {
		return errors.New("No result in result set")
	}

	columns, err := res.Columns()
	if err != nil {
		return err
	}

	result := make([]interface{}, len(columns))

	for x := 0; x < len(result); x++ {
		result[x] = new(interface{})
	}

	err = res.Scan(result...)

	if err != nil {
		return err
	}

	v := reflect.ValueOf(target)
	v = v.Elem()
	t := v.Type()

	for i, c := range columns {
		cv := *(result[i].(*interface{}))
		var field reflect.Value
		for x := 0; x < v.NumField(); x++ {
			tag := t.Field(x).Tag.Get("db")
			if len(tag) == 0 {
				//fmt.Println("Tag is 0")
				continue
			}
			splitted := strings.Split(tag, ",")
			if len(splitted) == 0 {
				continue
			}

			if c != splitted[0] {
				continue
			}

			field = v.Field(i)
			break
		}

		if !field.IsValid() {
			continue
		}

		switch nv := cv.(type) {
		case string:
			field.SetString(nv)
		case int:
			field.SetInt(int64(nv))
		case int64:
			field.SetInt(nv)
		case time.Time:
			field.Set(reflect.ValueOf(nv))
		case bool:
			field.SetBool(nv)
		case []byte:
			field.SetBytes(nv)
		}
	}

	return nil
}

func (m *Sql) Map(query string, args ...interface{}) (map[string]interface{}, error) {
	m.open()

	if !m.IsOpen {
		return nil, connectionError
	}

	res, err := m.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer res.Close()
	if !res.Next() {
		return nil, errors.New("No result in result set")
	}

	columns, err := res.Columns()
	if err != nil {
		return nil, err
	}

	mp := make(map[string]interface{})
	result := make([]interface{}, len(columns))

	for x := 0; x < len(result); x++ {
		result[x] = new(interface{})
	}

	err = res.Scan(result...)
	if err != nil {
		return nil, err
	}

	for i, c := range columns {
		v := result[i]
		mp[c] = *(v.(*interface{}))
	}
	return mp, nil
}

func (m *Sql) Maps(query string, args ...interface{}) ([]map[string]interface{}, error) {
	m.open()

	if !m.IsOpen {
		return nil, connectionError
	}

	res, err := m.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer res.Close()
	var mps []map[string]interface{}

	for res.Next() {
		columns, err := res.Columns()
		if err != nil {
			return nil, err
		}

		mp := make(map[string]interface{})
		result := make([]interface{}, len(columns))

		for x := 0; x < len(result); x++ {
			result[x] = new(interface{})
		}

		err = res.Scan(result...)
		if err != nil {
			return nil, err
		}

		for i, c := range columns {
			v := result[i]
			mp[c] = *(v.(*interface{}))
		}
		mps = append(mps, mp)
	}

	return mps, nil
}

func (m *Sql) Ping() error {
	m.open()

	return m.db.Ping()
}

func (m *Sql) Close() {

	if m.IsOpen {
		m.db.Close()
	}
	m.db = nil
}

func New(driver, cs string) (*Sql, error) {
	s := &Sql{cs: cs, driver: driver}
	return s, s.open()
}
