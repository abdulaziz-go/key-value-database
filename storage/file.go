package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type LogFile struct {
	file   *os.File
	path   string
	offset int64
}

func NewLogFile(dirPath string, fileID int) (*LogFile, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %v", err)
	}
	filepath.Join(dirPath, fmt.Sprintf("bitcask_%d.log", fileID))
	return nil, nil
}
