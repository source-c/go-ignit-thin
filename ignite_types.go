package ignite

import (
	"fmt"
	"time"
)

type Time int64

type Date int64

func NewTime(val time.Time) Time {
	utcVal := val.UTC()
	return Time(utcVal.Unix()*1000 + int64(utcVal.Nanosecond())/int64(time.Millisecond))
}

func (t Time) Time() time.Time {
	return time.Unix(int64(t)/1000, (int64(t)%1000)*int64(time.Millisecond))
}

func (t Time) String() string {
	return t.Time().Format("15:04:05.000")
}

func NewDate(val time.Time) Date {
	utcVal := val.UTC()
	return Date(utcVal.Unix()*1000 + int64(utcVal.Nanosecond())/int64(time.Millisecond))
}

func (d Date) Time() time.Time {
	return time.Unix(int64(d)/1000, (int64(d)%1000)*int64(time.Millisecond))
}

func (d Date) String() string {
	return d.Time().String()
}

type CollectionKind = int8

const (
	UserSet CollectionKind = iota - 1
	UserCollection
	ArrayList
	LinkedList
	HashSet
	LinkedHashSet
	SingletonList
	HashMap       CollectionKind = 1
	LinkedHashMap CollectionKind = 2
)

type Collection struct {
	kind   CollectionKind
	values []interface{}
}

func (col *Collection) Kind() CollectionKind {
	return col.kind
}

func (col *Collection) Values() []interface{} {
	return col.values
}

func (col *Collection) Size() int {
	return len(col.values)
}

func NewUserCollection[T any](values ...T) Collection {
	return newIgniteCollection(UserCollection, values...)
}

func NewArrayList[T any](values ...T) Collection {
	return newIgniteCollection(ArrayList, values...)
}

func NewLinkedList[T any](values ...T) Collection {
	return newIgniteCollection(LinkedList, values...)
}

func NewSingletonList[T any](value T) Collection {
	return newIgniteCollection(SingletonList, value)
}

func NewHashSet[T any](values ...T) Collection {
	return newIgniteCollection(HashSet, values...)
}

func NewLinkedHashSet[T any](values ...T) Collection {
	return newIgniteCollection(LinkedHashSet, values...)
}

func newIgniteCollection[T any](kind CollectionKind, values ...T) Collection {
	values0 := make([]interface{}, 0, len(values))
	for _, value := range values {
		values0 = append(values0, value)
	}
	return Collection{
		kind:   kind,
		values: values0,
	}
}

type Map struct {
	kind    CollectionKind
	entries []KeyValue
}

func (m *Map) Kind() CollectionKind {
	return m.kind
}

func (m *Map) Entries() []KeyValue {
	return m.entries
}

func (m *Map) Size() int {
	return len(m.entries)
}

func NewUserMap(entries ...KeyValue) Map {
	return newIgniteMap(UserCollection, entries...)
}

func ToUserMap[K comparable, V any](m map[K]V) Map {
	return toIgniteMap(UserCollection, m)
}

func NewHashMap(entries ...KeyValue) Map {
	return newIgniteMap(HashMap, entries...)
}

func ToHashMap[K comparable, V any](m map[K]V) Map {
	return toIgniteMap(HashMap, m)
}

func NewLinkedHashMap(entries ...KeyValue) Map {
	return newIgniteMap(LinkedHashMap, entries...)
}

func ToLinkedHashMap[K comparable, V any](m map[K]V) Map {
	return toIgniteMap(LinkedHashMap, m)
}

func ToMap[K comparable, V any](igniteMap Map) (map[K]V, error) {
	ret := make(map[K]V, len(igniteMap.entries))
	for _, entry := range igniteMap.entries {
		var key K
		var val V
		var ok bool
		if key, ok = entry.Key.(K); !ok {
			return nil, fmt.Errorf("invalid key type: %T", entry.Key)
		}
		if entry.Value != nil {
			if val, ok = entry.Value.(V); !ok {
				return nil, fmt.Errorf("invalid value type: %T", entry.Value)
			}
		}
		ret[key] = val
	}
	return ret, nil
}

func toIgniteMap[K comparable, V any](kind CollectionKind, m map[K]V) Map {
	entries := make([]KeyValue, 0, len(m))
	for k, v := range m {
		entries = append(entries, KeyValue{Key: k, Value: v})
	}
	return newIgniteMap(kind, entries...)
}

func newIgniteMap(kind CollectionKind, entries ...KeyValue) Map {
	return Map{
		kind:    kind,
		entries: entries,
	}
}
