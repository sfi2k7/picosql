package picosql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sfi2k7/blueutil"
)

const maxRetries = 3

var (
	connectionError = errors.New("No Connection is available")
	missingField    = errors.New("Missing parameter in target :")
	driverLock      sync.Mutex
)

type ColumnDefinition struct {
	ColumnName      string          `db:"column_name"`
	OrdinalPosition string          `db:"ordinal_position"`
	DataType        string          `db:"data_type"`
	ColumnType      string          `db:"column_type"`
	SqlColumnType   *sql.ColumnType `db:"-"`
}

type TableStructure struct {
	TableName    string
	DatabaseName string
	Columns      []*ColumnDefinition
}

type Sql struct {
	IsOpen  bool
	retries int
	db      *sql.DB
	cs      string
	driver  string
	tm      tagMapper
	isClone bool
}

type ColumnInfo struct {
	ID            string                  `bson:"_id"`
	ViewName      string                  `bson:"viewName"`
	DatabaseName  string                  `bson:"dbName"`
	Columns       []string                `bson:"columns"`
	ColumnTypes   []*sql.ColumnType       `bson:"-"`
	Simplified    []*ColumnTypeSimplified `bson:"simplified"`
	PrimaryColumn string                  `bson:"primaryColumn"`
}

type ColumnTypeSimplified struct {
	Index      int    `bson:"idx"`
	Name       string `bson:"name"`
	Length     int    `bson:"length"`
	DBType     string `bson:"dbtype"`
	IsNullable bool   `bson:"isNullable"`
	Precison   int    `bson:"prec"`
	Scale      int    `bson:"scale"`
	ScanType   string `bson:"scanType"`
}

type TableInfo struct {
	Name             string      `db:"Name"`
	Engine           string      `db:"Engine"`
	Version          int         `db:"Version"`
	RowFormat        string      `db:"Row_format"`
	Rows             int64       `db:"Rows"`
	AverageRowLength int64       `db:"Average_row_length"`
	DataLength       int64       `db:"Data_length"`
	MaxDataLength    int64       `db:"Max_data_length"`
	IndexLength      int64       `db:"Index_length"`
	DataFree         int64       `db:"Data_free"`
	AutoIncrement    interface{} `db:"Auto_increment"`
	CreateTime       time.Time   `db:"Create_time"`
	UpdateTime       time.Time   `db:"Update_time"`
	Collation        string      `db:"Collation"`
	Checksum         string      `db:"Checksum"`
	CreateOptions    string      `db:"Create_options"`
	Comments         string      `db:"Comments"`
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

	if m.IsOpen && m.db.Ping() == nil {
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

	// db.SetMaxIdleConns(5)
	// db.SetMaxOpenConns(40)
	// db.SetConnMaxLifetime(0)

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

func (m *Sql) IsEmptyResultError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Index(err.Error(), "No result in result set") != 0 {
		return false
	}
	return true
}

func (m *Sql) GetTableInfo(tn string) (*TableInfo, error) {
	//show table status where name = 'Business'
	q := fmt.Sprintf(`show table status where name = '%s'`, tn)
	var single TableInfo
	err := m.Get(&single, q)
	if err != nil {
		return nil, err
	}
	return &single, nil
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

func (m *Sql) RCount(t string) (int64, error) {
	q := `SELECT COUNT(*) FROM ` + t

	m.open()

	if !m.IsOpen {
		return 0, connectionError
	}

	res := m.db.QueryRow(q)

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

func (m *Sql) CreateTransection() (*sql.Tx, error) {
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

func (m *Sql) Slice(query string, args ...interface{}) ([]interface{}, []*sql.ColumnType, error) {
	m.open()

	if !m.IsOpen {
		return nil, nil, connectionError
	}

	res, err := m.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}

	defer res.Close()
	if !res.Next() {
		return nil, nil, errors.New("No result in result set")
	}

	columns, err := res.Columns()

	if err != nil {
		return nil, nil, err
	}

	types, err := res.ColumnTypes()

	if err != nil {
		return nil, nil, err
	}

	rs := make([]interface{}, len(columns))
	result := make([]interface{}, len(columns))

	for x := 0; x < len(result); x++ {
		result[x] = new(interface{})
	}

	err = res.Scan(result...)

	if err != nil {
		return nil, nil, err
	}

	for i := range columns {
		rs[i] = *(result[i].(*interface{}))
		switch v := rs[i].(type) {
		case []uint8:
			rs[i] = string(v)
		}
	}
	return rs, types, nil
}

func (m *Sql) Slices(query string, args ...interface{}) ([][]interface{}, []*sql.ColumnType, error) {
	m.open()

	if !m.IsOpen {
		return nil, nil, connectionError
	}

	res, err := m.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}

	defer res.Close()
	var scs [][]interface{}

	columns, err := res.Columns()
	if err != nil {
		return nil, nil, err
	}

	types, err := res.ColumnTypes()

	if err != nil {
		return nil, nil, err
	}
	for res.Next() {

		sc := make([]interface{}, len(columns))
		result := make([]interface{}, len(columns))

		for x := 0; x < len(result); x++ {
			result[x] = new(interface{})
		}

		err = res.Scan(result...)
		if err != nil {
			return nil, nil, err
		}

		for i := range columns {
			sc[i] = *(result[i].(*interface{}))
			switch v := sc[i].(type) {
			case []uint8:
				sc[i] = string(v)
			}
		}
		scs = append(scs, sc)
	}

	return scs, types, nil
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
		switch v := mp[c].(type) {
		case []uint8:
			mp[c] = string(v)
		}
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
			switch v := mp[c].(type) {
			case []uint8:
				mp[c] = string(v)
			}
		}
		mps = append(mps, mp)
	}

	return mps, nil
}

func (m *Sql) ListTables(dbName string) ([]string, error) {
	//sql := `SELECT TABLE_NAME from information_schema WHERE TABLE_SCHEMA = '` + dbName + "'"
	sql := `SHOW tables;`
	var tables []string
	res, err := m.Query(sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	for res.Next() {
		var tableName string
		res.Scan(&tableName)
		tables = append(tables, tableName)
	}
	return tables, nil
}

// func (m *Sql) DeleteColumn(tableName, columnName string) error {
// 	sql := fmt.Sprintf("ALTER TABLE '%s' DROP COLUMN %s", tableName, columnName)
// 	_, err := m.Exec(sql)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (m *Sql) AddColumn(column *ColumnDefinition, after, tableName string) error {
// 	def := `ADD COLUMN ` + ColumnDefinitionStringBasedOnType(column.ColumnName, column.ColumnType)
// 	def = def[0 : len(def)-1]
// 	if len(after) > 0 {
// 		def += " AFTER " + after
// 	}
// 	sql := fmt.Sprintf("ALTER TABLE `%s` %s", tableName, def)
// 	_, err := m.Exec(sql)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (m *Sql) simpleQuery(query string) error {
	rows, err := m.Query(query)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer rows.Close()

	if rows.Next() {
		return nil
	}
	return errors.New("No Response from DB")
}

func (m *Sql) CreateDatabase(dbName string) error {
	q := "CREATE DATABASE `" + dbName + "`  DEFAULT CHARACTER SET latin1"
	res, err := m.Exec(q)
	if err != nil {
		return err
	}
	res.LastInsertId()
	res.RowsAffected()

	return nil
}

func (m *Sql) UserExists(userName string) bool {
	res, err := m.Query("SELECT User from mysql.user WHERE User = ? LIMIT 1", userName)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer res.Close()
	for res.Next() {
		var user string
		res.Scan(&user)
		return user == userName
	}
	return false
}

func (m *Sql) createUser(userName, password string) error {
	if m.UserExists(userName) {
		return nil
	}
	sql := "CREATE USER '" + userName + "'@'%' IDENTIFIED BY '" + password + "'"
	_, err := m.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *Sql) AssignPermissions(db, userName string) error {
	sql := fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%' WITH GRANT OPTION;", db, userName)
	_, err := m.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *Sql) DatabaseExists(db string) (bool, error) {
	sql := "show databases"
	res, err := m.Query(sql)
	if err != nil {
		return false, err
	}
	defer res.Close()
	for res.Next() {
		var dbName string
		res.Scan(&dbName)
		if strings.ToLower(dbName) == strings.ToLower(db) {
			fmt.Println(dbName, db)
			return true, nil
		}
	}
	return false, nil
}

func (m *Sql) GetCurrentStructure(dbName, tableName string) (*TableStructure, error) {
	sql := fmt.Sprintf(`select column_name,ordinal_position,data_type,column_type from information_schema.COLUMNS where table_schema = '%s' and table_name = '%s'`, dbName, tableName)
	mps, err := m.Maps(sql)
	if err != nil {
		return nil, err
	}

	table := &TableStructure{TableName: tableName, DatabaseName: dbName}
	for _, v := range mps {
		var column ColumnDefinition
		blueutil.FillStruct(v, &column)
		table.Columns = append(table.Columns, &column)
	}
	return table, nil

	// res, err := m.Queryx(sql)
	// if err != nil {
	// 	return nil, err
	// }
	// defer res.Close()
	// table := &TableStructure{}
	// for res.Next() {
	// 	var column ColumnDefinition
	// 	res.StructScan(&column)
	// 	table.Columns = append(table.Columns, &column)
	// }
	// table.Name = tableName
	// return table, nil
}

func (m *Sql) DropTable(tableName string) error {
	sql := `DROP TABLE ` + tableName
	_, err := m.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *Sql) columnDefinitionStringBasedOnType(c string, t *ColumnTypeSimplified) string {
	l := t.Length
	if l == 0 {
		l = 100
	}

	if l > 1024 {
		l = 1024
	}

	if strings.Contains(c, "url") && l < 255 {
		l = 500
	}

	p := 20
	s := 5

	// if t.Precison == 0 || t.Scale == 0 {
	// 	p = 20
	// 	s = 5
	// }

	switch t.DBType {
	case "DATETIME":
		fallthrough
	case "DATE":
		return "`" + c + "` DATE DEFAULT NULL,"
	// case "DATETIME":
	// 	return "`" + c + "` DATETIME DEFAULT NULL,"
	case "INT":
		return "`" + c + "` INT(11) DEFAULT NULL,"
	case "BIGINT":
		return "`" + c + "` BIGINT(20) DEFAULT NULL,"
	case "NTEXT":
		fallthrough
	case "VARCHAR":
		fallthrough
	case "CHAR":
		fallthrough
	case "NVARCHAR":
		return "`" + c + "` VARCHAR(" + strconv.Itoa(int(l)) + ") DEFAULT NULL,"
	case "TEXT":
		return "`" + c + "` TEXT DEFAULT NULL,"
	case "BIT":
		return "`" + c + "` BIT DEFAULT NULL,"
	case "MONEY":
		fallthrough
	case "DECIMAL":
		return "`" + c + "` DECIMAL(" + strconv.Itoa(int(p)) + "," + strconv.Itoa(int(s)) + ") DEFAULT NULL,"
	}
	return ""
}

func (m *Sql) DropTableNew(db, tableName string) error {
	sql := fmt.Sprintf("Drop Table `%s`.`%s`", db, tableName)
	_, err := m.Exec(sql)
	return err
}

func (m *Sql) CreateUniqueIndex(db, tableName, keyField string) error {
	var sb string
	sb += fmt.Sprintf("ALTER TABLE `%s`.`%s` ", db, tableName)
	sb += "ADD UNIQUE INDEX `basic` ("
	var keys = strings.Split(keyField, ",")
	for i := 0; i < len(keys); i++ {
		sb += fmt.Sprintf("`%s` ASC,", keys[i])
	}
	sb = sb[0 : len(sb)-1]
	sb += ");"
	_, err := m.Exec(sb)
	if err != nil {
		return err
	}
	return nil
}

func (m *Sql) CreateTable(tableName string, columns []string, types []*ColumnTypeSimplified, keyField string) error {
	//fields := strings.Split(header, ",")
	sql := " CREATE TABLE `" + tableName + "` ("
	for i, f := range columns {
		cd := m.columnDefinitionStringBasedOnType(f, types[i])
		if f == keyField {
			cd = strings.Replace(cd, "DEFAULT NULL", "NOT NULL", -1)
		}
		sql += cd
	}

	sql += "`current_hash` VARCHAR(25) DEFAULT NULL,"
	if len(keyField) > 0 {
		if strings.Contains(keyField, ",") {
			sql = sql[0 : len(sql)-1]
		} else {
			sql += "PRIMARY KEY (`" + keyField + "`)"
		}
	} else {
		sql = sql[0 : len(sql)-1]
	}
	sql += ` ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	_, err := m.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (m *Sql) CreateTableNoHashOrKey(tableName string, columns []string, types []*ColumnTypeSimplified) error {

	//fields := strings.Split(header, ",")
	sql := " CREATE TABLE `" + tableName + "` ("
	for i, f := range columns {
		cd := m.columnDefinitionStringBasedOnType(f, types[i])
		sql += cd
	}

	sql = sql[0 : len(sql)-1]
	sql += ` ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	_, err := m.Exec(sql)
	if err != nil {
		return err
	}
	return nil
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

// public class ColumnInfo
//  {
// 	[JsonProperty(propertyName:"_id")]
// 	public string ID    {get;set;}
// 	[JsonProperty(propertyName:"viewName")]
// 	public string ViewName    {get;set;}
// 	[JsonProperty(propertyName:"dbName")]
// 	public string DatabaseName {get;set;}
// 	[JsonProperty(propertyName:"columns")]
// 	public List<string> Columns {get;set;}
// 	[JsonProperty(propertyName:"simplified")]
// 	public List<ColumnTypeSimplified> Simplified    {get;set;}
// 	[JsonProperty(propertyName:"primaryColumn")]
// 	public string PrimaryColumn    {get;set;}
// }

// public class ColumnTypeSimplified
//  {
// 	[JsonProperty(propertyName:"idx")]
// 	public int Index {get;set;}
// 	[JsonProperty(propertyName:"name")]
// 	public string Name {get;set;}
// 	[JsonProperty(propertyName:"length")]
// 	public int Length {get;set;}
// 	[JsonProperty(propertyName:"dbtype")]
// 	public string DBType{get;set;}
// 	[JsonProperty(propertyName:"isNullable")]
// 	public string IsNullable {get;set;}
// 	[JsonProperty(propertyName:"prec")]
// 	public int Precison{get;set;}
// 	[JsonProperty(propertyName:"scale")]
// 	public int Scale  {get;set;}
// 	[JsonProperty(propertyName:"scanType")]
// 	public string ScanType {get;set;}
// }
