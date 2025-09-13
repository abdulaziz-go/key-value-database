package bitcask

import (
	"errors"
	"fmt"
	"key-value-storage/storage"
	"sync"
)

type BitCask struct {
	mu         sync.RWMutex
	keyDir     *KeyDir
	logManager *LogManager
	dataDir    string
	closed     bool
}

func Open(dataDir string) (*BitCask, error) {
	logManager, err := NewLogManager(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create log manager: %v", err)
	}
	keyDir := NewKeyDir()
	bc := &BitCask{
		keyDir:     keyDir,
		logManager: logManager,
		dataDir:    dataDir,
		closed:     false,
	}
	return bc, nil
}

func (bc *BitCask) Put(key string, value []byte) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.closed {
		return errors.New("closed bitcask error")
	}

	record := storage.NewRecord(key, value)
	err := bc.logManager.Write(record)
	if err != nil {
		return fmt.Errorf("failed to write record: %v", err)
	}

	entry := &KeyDirEntry{
		Offset:    record.Offset,
		Size:      record.Size,
		Timestamp: record.Timestamp,
	}

	bc.keyDir.Post(key, entry)
	return nil
}

func (bc *BitCask) Get(key string) ([]byte, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if bc.closed {
		return nil, errors.New("trying to write closed file")
	}

	entry, exists := bc.keyDir.Get(key)
	if !exists {
		return nil, errors.New("key not found")
	}

	record, err := bc.logManager.Read(entry.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %v", err)
	}
	return record.Value, nil
}

func (bc *BitCask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.closed {
		return errors.New("trying to write closed file")
	}

	_, exists := bc.keyDir.Get(key)
	if !exists {
		return errors.New("key not found")
	}

	//record := storage.NewRecord(key, []byte{})

	//err := bc.logManager.Write(record)
	//if err != nil {
	//	return fmt.Errorf("failed to write delete record: %v", err)
	//}

	bc.keyDir.Delete(key)
	return nil
}

func (bc *BitCask) Close() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.closed {
		return nil
	}

	bc.closed = true
	return bc.logManager.Close()
}
