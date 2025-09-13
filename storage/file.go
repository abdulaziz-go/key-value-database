package storage

import (
	"fmt"
	"io"
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

	path := filepath.Join(dirPath, fmt.Sprintf("bitcask_%d.log", fileID))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %v", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %v", err)
	}

	return &LogFile{
		file:   file,
		path:   path,
		offset: stat.Size(),
	}, nil
}

func (lf *LogFile) Write(record *Record) error {
	encodedRecord := record.Encode()
	record.Offset = lf.offset
	n, err := lf.file.Write(encodedRecord)
	if err != nil {
		return err
	}

	lf.offset += int64(n)
	return nil
}

func (lf *LogFile) Read(offset int64) (*Record, error) {
	if _, err := lf.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %v", err)
	}

	headerBuf := make([]byte, 16)
	if _, err := io.ReadFull(lf.file, headerBuf); err != nil {
		return nil, fmt.Errorf("filed to readfull %v", err)
	}

	header, err := DecodeHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	dataSize := int(header.KeySize + header.ValueSize)
	keyValueBuf := make([]byte, dataSize)
	if _, err = io.ReadFull(lf.file, keyValueBuf); err != nil {
		return nil, fmt.Errorf("failed to read key-value: %v", err)
	}

	return Decoder(append(headerBuf, keyValueBuf...), offset)
}

func (lf *LogFile) ReadAll() ([]*Record, error) {
	if _, err := lf.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %v", err)
	}

	var records []*Record
	currentOffset := int64(0)
	for {
		read, err := lf.Read(currentOffset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		records = append(records, read)
		currentOffset += read.Size
	}

	return records, nil
}
