package bitcask

import "sync"

type KeyDirEntry struct {
	Offset    int64
	Size      int64
	Timestamp int64
}

type KeyDir struct {
	mu      sync.RWMutex
	entries map[string]*KeyDirEntry
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		mu:      sync.RWMutex{},
		entries: make(map[string]*KeyDirEntry),
	}
}

func (kd *KeyDir) Post(key string, entry *KeyDirEntry) {
	kd.mu.Lock()
	defer kd.mu.Unlock()
	kd.entries[key] = entry
}

func (kd *KeyDir) Get(key string) (*KeyDirEntry, bool) {
	kd.mu.RLock()
	defer kd.mu.RUnlock()
	entry, ok := kd.entries[key]
	return entry, ok
}

func (kd *KeyDir) Delete(key string) bool {
	kd.mu.Lock()
	defer kd.mu.Unlock()
	_, exists := kd.entries[key]
	if exists {
		delete(kd.entries, key)
	}
	return exists
}

func (kd *KeyDir) DeleteAll() {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	kd.entries = make(map[string]*KeyDirEntry)
}
