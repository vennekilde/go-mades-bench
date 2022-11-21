package main

type Node[V any] struct {
	prev  *Node[V]
	next  *Node[V]
	value V
}

func (n *Node[V]) insertBefore(value V) {
	newNode := Node[V]{
		next:  n,
		prev:  n.prev,
		value: value,
	}
	if n.prev != nil {
		n.prev.next = &newNode
	}
	n.prev = &newNode
}

func (n *Node[V]) insertAfter(value V) {
	newNode := Node[V]{
		next:  n.next,
		prev:  n,
		value: value,
	}
	if n.next != nil {
		n.next.prev = &newNode
	}
	n.next = &newNode
}

type SortedMap[K Ordered, V any] struct {
	keys *Node[K]
	m    map[K]V
}

func NewSortedMap[K Ordered, V any]() SortedMap[K, V] {
	return SortedMap[K, V]{
		m: map[K]V{},
	}
}

func (s *SortedMap[K, V]) compare(t1 K, t2 K) int {
	if t1 > t2 {
		return 1
	}
	if t1 < t2 {
		return -1
	}
	return 0
}

func (s *SortedMap[K, V]) Put(key K, value V) {
	if _, ok := s.m[key]; !ok {
		s.insertKey(key)
	}
	s.m[key] = value
}

func (s *SortedMap[K, V]) insertKey(key K) {
	link := s.keys
	prev := link
	for link != nil {
		if key < link.value {
			link.insertBefore(key)
			return
		}
		prev = link
		link = link.next
	}
	if prev == nil {
		s.keys = &Node[K]{
			value: key,
		}
	} else {
		prev.insertAfter(key)
	}
}
