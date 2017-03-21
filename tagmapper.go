package picosql

import (
	"reflect"
	"strings"
	"sync"
)

const (
	tagPrefix = "db"
)

type tagMapper map[string]map[string]string

var tagHelperLock sync.Mutex

func (tm tagMapper) get(target reflect.Type) map[string]string {
	tn := target.Name()
	if !tm.has(target) {
		tagHelperLock.Lock()

		tm.build(target)

		tagHelperLock.Unlock()
	}
	return tm[tn]
}

func (tm tagMapper) build(target reflect.Type) {
	t := target
	tname := t.Name()

	m := make(map[string]string)
	for x := 0; x < t.NumField(); x++ {
		f := t.Field(x)
		fname := f.Name
		tags := strings.Split(f.Tag.Get(tagPrefix), ",")
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

func (tm tagMapper) has(target reflect.Type) bool {
	name := target.Name()
	_, ok := tm[name]
	return ok
}
