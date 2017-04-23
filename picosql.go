package picosql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

const maxRetries = 3

var (
	connectionError = errors.New("No Connection is available")
	missingField    = errors.New("Missing parameter in target :")
	driverLock      sync.Mutex
)

type Sql struct {
	IsOpen  bool
	retries int
	db      *sql.DB
	cs      string
	driver  string
	tm      tagMapper
	isClone bool
}

// func (m *Sql) fillNamedParameters(elementType reflect.Type, query string) (string, []interface{}) {
// 	s, pars := ExtractNamedParameters(query)
// 	tm:=

// 	return "", nil
// }

func (m *Sql) SetMaxIdleConns(n int) {
	m.db.SetMaxIdleConns(n)
}

func (m *Sql) SetMaxOpenConns(n int) {
	m.db.SetMaxOpenConns(n)
}

func (m *Sql) SetConnMaxLifetime(d time.Duration) {
	m.db.SetConnMaxLifetime(d)
}

func (m *Sql) open() error {
	driverLock.Lock()
	defer driverLock.Unlock()

	if m.IsOpen {
		return nil
	}

	if m.db != nil && m.db.Ping() == nil {
		return nil
	}

	db, err := sql.Open(m.driver, m.cs)
	if err != nil {
		m.IsOpen = false
		return err
	}

	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(40)
	db.SetConnMaxLifetime(time.Minute * 3)

	m.db = db
	err = db.Ping()
	if err != nil {
		m.IsOpen = false
		return err
	}

	m.IsOpen = true
	m.tm = make(tagMapper)
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

func (m *Sql) NamedExec(query string, args interface{}) (int64, error) {
	m.open()

	if !m.IsOpen {
		return 0, connectionError
	}

	v := reflect.ValueOf(args)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	t := v.Type()

	q, param := ExtractNamedParameters(query)

	tm := m.tm.get(t)
	data := make([]interface{}, len(param))
	//fmt.Println(tm)
	for i, p := range param {
		fn, ok := tm[p]
		if !ok {
			//fmt.Println(p, tm)
			return 0, errors.New(missingField.Error() + p)
		}
		f := v.FieldByName(fn)
		data[i] = f.Interface()
	}

	res, err := m.db.Exec(q, data...)

	if err != nil {
		return 0, err
	}
	if strings.ToLower(q[0:6]) == "insert" {
		lastID, err := res.LastInsertId()
		//fmt.Println("Getting last ID")
		if err != nil {
			return 0, err
		}
		return lastID, nil
	}
	//fmt.Println("Getting Affected")
	affected, err := res.RowsAffected()

	if err != nil {
		return 0, err
	}

	return affected, nil
}
func (m *Sql) createTransection() (*sql.Tx, error) {
	m.open()

	if !m.IsOpen {
		return nil, nil
	}
	return m.db.Begin()
}

func (m *Sql) CommitOrRollback(tx *sql.Tx) bool {
	err := tx.Commit()
	if err != nil {
		tx.Rollback()
		return false
	}
	return true
}

func (m *Sql) NamedExecTransection(tx *sql.Tx, query string, args interface{}) (int64, error) {
	m.open()

	if !m.IsOpen {
		return 0, connectionError
	}

	v := reflect.ValueOf(args)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	t := v.Type()

	q, param := ExtractNamedParameters(query)

	tm := m.tm.get(t)
	data := make([]interface{}, len(param))
	//fmt.Println(tm)
	for i, p := range param {
		fn, ok := tm[p]
		if !ok {
			//fmt.Println(p, tm)
			return 0, errors.New(missingField.Error() + p)
		}
		f := v.FieldByName(fn)
		data[i] = f.Interface()
	}

	res, err := tx.Exec(q, data...)

	if err != nil {
		return 0, err
	}
	if strings.ToLower(q[0:6]) == "insert" {
		lastID, err := res.LastInsertId()
		//fmt.Println("Getting last ID")
		if err != nil {
			return 0, err
		}
		return lastID, nil
	}
	//fmt.Println("Getting Affected")
	affected, err := res.RowsAffected()

	if err != nil {
		return 0, err
	}

	return affected, nil
}

func (m *Sql) NamedInsertAll(query string, args interface{}) ([]int64, error) {
	m.open()

	if !m.IsOpen {
		return []int64{}, connectionError
	}
	//Validation Example

	var ids []int64
	v := reflect.ValueOf(args)
	if v.Kind() != reflect.Slice {
		return ids, errors.New("Input parameter must be a slice")
	}

	l := v.Len()

	if l == 0 {
		return ids, errors.New("Missing required parameters")
	}

	sample := v.Index(l - 1).Elem()

	if sample.Kind() != reflect.Struct {
		return ids, errors.New("Must provide a slice of structs")
	}

	tm := m.tm.get(sample.Type())

	q, param := ExtractNamedParameters(query)
	tx, _ := m.db.Begin()
	for x := 0; x < l; x++ {
		single := v.Index(x)
		sv := single.Elem()
		data := make([]interface{}, len(param))
		for i, p := range param {
			fn, ok := tm[p]
			if !ok {
				continue
			}
			f := sv.FieldByName(fn)
			data[i] = f.Interface()
		}
		//fmt.Println(q, data)
		res, err := tx.Exec(q, data...)

		if err != nil {
			return ids, err
		}
		lastID, err := res.LastInsertId()
		if err != nil {
			return ids, err
		}
		ids = append(ids, lastID)
	}
	err := tx.Commit()

	if err != nil {
		tx.Rollback()
	}

	return ids, nil
}

func (m *Sql) NamedUpdateAll(query string, args interface{}) (int64, error) {
	m.open()

	if !m.IsOpen {
		return 0, connectionError
	}

	//Validation Example

	var totalAffected int64
	v := reflect.ValueOf(args)
	if v.Kind() != reflect.Slice {
		return totalAffected, errors.New("Input parameter must be a slice")
	}

	l := v.Len()

	if l == 0 {
		return totalAffected, errors.New("Missing required parameters")
	}

	sample := v.Index(l - 1).Elem()

	if sample.Kind() != reflect.Struct {
		return totalAffected, errors.New("Must provide a slice of structs")
	}

	tm := m.tm.get(sample.Type())
	//fmt.Println(tm)

	q, param := ExtractNamedParameters(query)
	tx, _ := m.db.Begin()
	for x := 0; x < l; x++ {
		single := v.Index(x)
		sv := single.Elem()
		data := make([]interface{}, len(param))
		for i, p := range param {
			fn, ok := tm[p]
			if !ok {
				continue
			}
			f := sv.FieldByName(fn)
			data[i] = f.Interface()
		}

		res, err := tx.Exec(q, data...)

		if err != nil {
			return totalAffected, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return totalAffected, err
		}
		totalAffected += affected
	}
	err := tx.Commit()
	if err != nil {
		tx.Rollback()
	}
	return totalAffected, nil
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

	lastID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastID, nil
}

func (m *Sql) Exec(query string, args ...interface{}) (sql.Result, error) {
	m.open()

	if !m.IsOpen {
		return nil, connectionError
	}

	return m.db.Exec(query, args...)
}

func (m *Sql) Query(query string, args ...interface{}) (*sql.Rows, error) {
	m.open()

	if !m.IsOpen {
		return nil, connectionError
	}

	return m.db.Query(query, args...)
}

func (m *Sql) QueryRow(query string, args ...interface{}) *sql.Row {
	m.open()

	if !m.IsOpen {
		return nil
	}

	return m.db.QueryRow(query, args...)
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

	if elementType.Kind() == reflect.Ptr {
		elementType = elementType.Elem()
	}

	isPrimitive := elementType.Kind() != reflect.Struct

	var tm map[string]string
	if !isPrimitive {
		tm = m.tm.get(elementType)
	}
	//fmt.Println(tm)
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

		if isPrimitive {
			target := reflect.New(elementType)
			setValue(target.Elem(), *(result[0].(*interface{})))
			sliceValue.Set(reflect.Append(sliceValue, target.Elem()))
			continue
		}

		target := reflect.New(elementType)
		v := target.Elem()

		for i, c := range columns {
			cv := *(result[i].(*interface{}))
			var field reflect.Value
			fn, ok := tm[c]
			if !ok {
				continue
			}
			field = v.FieldByName(fn)

			if !field.IsValid() {
				continue
			}

			err := setValue(field, cv)
			if err != nil {
				fmt.Println(err)
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
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	t := v.Type()
	if v.Kind() == reflect.Struct {
		tm := m.tm.get(t)

		for i, c := range columns {
			cv := *(result[i].(*interface{}))
			fn, ok := tm[c]

			if !ok {
				continue
			}

			field := v.FieldByName(fn)

			if !field.IsValid() {
				continue
			}

			setValue(field, cv)
		}
	} else {
		cv := *(result[0].(*interface{}))
		fmt.Println(cv)
		setValue(v, cv)
		//v.Set(reflect.ValueOf(cv))
	}

	return nil
}

func (m *Sql) Slice(query string, args ...interface{}) ([]interface{}, error) {
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

	rs := make([]interface{}, len(columns))
	result := make([]interface{}, len(columns))

	for x := 0; x < len(result); x++ {
		result[x] = new(interface{})
	}

	err = res.Scan(result...)

	if err != nil {
		return nil, err
	}

	for i := range columns {
		rs[i] = *(result[i].(*interface{}))
	}
	return rs, nil
}

func (m *Sql) Slices(query string, args ...interface{}) ([][]interface{}, error) {
	m.open()

	if !m.IsOpen {
		return nil, connectionError
	}

	res, err := m.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer res.Close()
	var scs [][]interface{}

	for res.Next() {
		columns, err := res.Columns()
		if err != nil {
			return nil, err
		}

		sc := make([]interface{}, len(columns))
		result := make([]interface{}, len(columns))

		for x := 0; x < len(result); x++ {
			result[x] = new(interface{})
		}

		err = res.Scan(result...)
		if err != nil {
			return nil, err
		}

		for i := range columns {
			sc[i] = *(result[i].(*interface{}))
		}
		scs = append(scs, sc)
	}

	return scs, nil
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

	if m.isClone {
		m.IsOpen = false
		m.cs = ""
		m.db = nil
		m.driver = ""
		m.tm = nil
		return
	}

	if m.IsOpen {
		m.db.Close()
	}
	m.db = nil
}

func (m *Sql) Clone() (*Sql, error) {
	s := &Sql{
		IsOpen:  m.IsOpen,
		cs:      m.cs,
		db:      m.db,
		retries: m.retries,
		tm:      m.tm,
		isClone: true,
	}

	if s.db.Ping() != nil {
		m.IsOpen = false
		s.open()
		if s.Ping() != nil {
			return nil, connectionError
		}

	}
	return s, nil
}

func New(driver, cs string) (*Sql, error) {
	s := &Sql{cs: cs, driver: driver}
	return s, s.open()
}
