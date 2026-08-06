package storage

import (
	"bytes"
	"math/rand"
	"sync"
)

const maxSkipListLevel = 16

const skipListProbability = 0.5

type skipListNode struct {
	key     []byte
	value   []byte
	op      OpType
	forward []*skipListNode
}

type skipList struct {
	head   *skipListNode
	level  int
	length int
}

func newSkipList() *skipList {
	head := &skipListNode{
		forward: make([]*skipListNode, maxSkipListLevel),
	}
	return &skipList{head: head, level: 1}
}

func randomLevel() int {
	level := 1
	for level < maxSkipListLevel && rand.Float64() < skipListProbability {
		level++
	}
	return level
}

func (sl *skipList) put(key, value []byte, op OpType) {
	update := make([]*skipListNode, maxSkipListLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	existing := current.forward[0]

	if existing != nil && bytes.Equal(existing.key, key) {
		existing.value = value
		existing.op = op
		return
	}

	newLevel := randomLevel()

	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &skipListNode{
		key:     key,
		value:   value,
		op:      op,
		forward: make([]*skipListNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.length++
}

func (sl *skipList) get(key []byte) *skipListNode {
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	candidate := current.forward[0]
	if candidate != nil && bytes.Equal(candidate.key, key) {
		return candidate
	}
	return nil
}

type MemTableEntry struct {
	Key   []byte
	Value []byte
	Op    OpType
}

func (sl *skipList) iterate() []MemTableEntry {
	var entries []MemTableEntry
	current := sl.head.forward[0]
	for current != nil {
		entries = append(entries, MemTableEntry{
			Key:   current.key,
			Value: current.value,
			Op:    current.op,
		})
		current = current.forward[0]
	}
	return entries
}

type MemTable struct {
	mu        sync.RWMutex
	sl        *skipList
	sizeBytes int64
}

func NewMemTable() *MemTable {
	return &MemTable{
		sl: newSkipList(),
	}
}

func (m *MemTable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing := m.sl.get(key); existing != nil {
		m.sizeBytes -= int64(len(existing.key) + len(existing.value))
	}

	m.sl.put(key, value, OpPut)

	m.sizeBytes += int64(len(key) + len(value) + 64)
}

func (m *MemTable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing := m.sl.get(key); existing != nil {
		m.sizeBytes -= int64(len(existing.key) + len(existing.value))
	}

	m.sl.put(key, nil, OpDelete)
	m.sizeBytes += int64(len(key) + 64)
}

func (m *MemTable) Get(key []byte) (value []byte, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node := m.sl.get(key)
	if node == nil {
		return nil, false
	}

	if node.op == OpDelete {
		return nil, true
	}

	return node.value, true
}

func (m *MemTable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *MemTable) Entries() []MemTableEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.iterate()
}

func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.length
}