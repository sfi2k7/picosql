package picosql

import (
	"reflect"
	"strings"
	"sync"
)

type tagMapper map[string]map[string]string

var tagHelperLock sync.Mutex

func (tm tagMapper) get(target interface{}) map[string]string {
	tn := reflect.TypeOf(target).Elem()
	if !tm.has(target) {
		tagHelperLock.Lock()

		tm.build(target)

		tagHelperLock.Unlock()
	}
	return tm[tn.Name()]
}

func (tm tagMapper) build(target interface{}) {
	t := reflect.TypeOf(target).Elem()
	tname := t.Name()

	m := make(map[string]string)
	for x := 0; x < t.NumField(); x++ {
		f := t.Field(x)
		fname := f.Name
		tags := strings.Split(f.Tag.Get("db"), ",")
		tag := fname
		if len(tags) > 0 {
			tag = strings.TrimSpace(tags[0])
			if len(tag) == 0 {
				tag = fname
			}
		}
		m[tag] = fname
	}

	tm[tname] = m
}

func (tm tagMapper) has(target interface{}) bool {
	name := reflect.TypeOf(target).Name()
	_, ok := tm[name]
	return ok
}
