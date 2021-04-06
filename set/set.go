package set

import (
	"fmt"
	"sync"
)

type StringStringSet struct {
	mx sync.Mutex
	m  map[string]string
}

func NewStringStringSet() *StringStringSet {
	return &StringStringSet{
		m: map[string]string{},
	}
}

func (set *StringStringSet) Get(key string) (string, bool) {
	set.mx.Lock()
	defer set.mx.Unlock()
	val, ok := set.m[key]
	return val, ok
}

func (set *StringStringSet) Has(key string) bool {
	set.mx.Lock()
	defer set.mx.Unlock()
	_, ok := set.m[key]
	return ok
}

func (set *StringStringSet) Add(key string, value string) {
	set.mx.Lock()
	defer set.mx.Unlock()
	set.m[key] = value
}

type StringSliceSet struct {
	mx sync.Mutex
	m  map[string][]string
}

func NewStringSliceSet() *StringSliceSet {
	return &StringSliceSet{
		m: map[string][]string{},
	}
}

func (set *StringSliceSet) Get(key string) ([]string, bool) {
	set.mx.Lock()
	defer set.mx.Unlock()
	val, ok := set.m[key]
	return val, ok
}

func (set *StringSliceSet) Has(key string) bool {
	set.mx.Lock()
	defer set.mx.Unlock()
	_, ok := set.m[key]
	return ok
}

func (set *StringSliceSet) Add(key string, value []string) {
	set.mx.Lock()
	defer set.mx.Unlock()
	set.m[key] = value
}

func (set *StringSliceSet) Append(key string, value string) {
	set.mx.Lock()
	defer set.mx.Unlock()
	set.m[key] = append(set.m[key], value)
}

func (set *StringSliceSet) Print() {
	set.mx.Lock()
	defer set.mx.Unlock()
	for key, values := range set.m {
		fmt.Printf("%s: %v\n", key, values)
	}
}
