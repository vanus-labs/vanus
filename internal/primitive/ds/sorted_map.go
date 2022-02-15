package ds

type Entry interface {
	Previous() Entry
	Next() Entry
	Key() string
	Value() interface{}
}

type SortedMap interface {
	Put(string, Entry)
	Get(string) Entry
	Head() Entry
	Tail() Entry
	Remove(string) Entry
}

func NewSortedMap() SortedMap {
	return nil
}