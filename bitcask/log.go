package bitcask

import (
	"fmt"
	"key-value-storage/storage"
	"sync"
)

type LogManager struct {
	mu      sync.RWMutex
	dataDir string
	logFile *storage.LogFile
}

func NewLogManager(dataDir string) (*LogManager, error) {
	logFile, err := storage.NewLogFile(dataDir, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %v", err)
	}

	return &LogManager{
		dataDir: dataDir,
		logFile: logFile,
	}, nil
}

func (lm *LogManager) Write(record *storage.Record) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.logFile.Write(record)
}

func (lm *LogManager) Read(offset int64) (*storage.Record, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	return lm.logFile.Read(offset)
}

func (lm *LogManager) LoadAllRecords() ([]*storage.Record, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	return lm.logFile.ReadAll()
}

func (lm *LogManager) Close() error {
	return lm.logFile.Close()
}
