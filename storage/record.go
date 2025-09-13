package storage

import (
	"encoding/binary"
	"errors"
)

type Record struct {
	Timestamp int64 // 8 byte
	Key       string
	Value     []byte
	Offset    int64
	Size      int64
}

type RecordHeader struct {
	Timestamp int64
	KeySize   int32
	ValueSize int32
}

func (r *Record) Encode() []byte {
	keyBytes := []byte(r.Key)
	keySize := len(keyBytes)
	valueSize := len(r.Value)

	totalSize := 8 + 4 + 4 + keySize + valueSize
	buf := make([]byte, totalSize)

	pos := 0

	u64 := uint64(r.Timestamp)
	buf[pos] = byte(u64 >> 56)
	buf[pos+1] = byte(u64 >> 48)
	buf[pos+2] = byte(u64 >> 40)
	buf[pos+3] = byte(u64 >> 32)
	buf[pos+4] = byte(u64 >> 24)
	buf[pos+5] = byte(u64 >> 16)
	buf[pos+6] = byte(u64 >> 8)
	buf[pos+7] = byte(u64)
	pos += 8

	u32 := uint32(keySize)
	buf[pos] = byte(u32 >> 24)
	buf[pos+1] = byte(u32 >> 16)
	buf[pos+2] = byte(u32 >> 8)
	buf[pos+3] = byte(u32)
	pos += 4

	u32 = uint32(valueSize)
	buf[pos] = byte(u32 >> 24)
	buf[pos+1] = byte(u32 >> 16)
	buf[pos+2] = byte(u32 >> 8)
	buf[pos+3] = byte(u32)
	pos += 4

	// Key copy
	copy(buf[pos:pos+keySize], keyBytes)
	pos += keySize

	// Value copy
	copy(buf[pos:pos+valueSize], r.Value)

	r.Size = int64(totalSize)
	return buf
}

func Decoder(data []byte, offset int64) (*Record, error) {
	if len(data) < 16 {
		return nil, errors.New("timestamp , keysize and value size mismatch")
	}

	pos := 0

	timestamp := int64(uint64(data[pos])<<56 |
		uint64(data[pos+1])<<48 |
		uint64(data[pos+2])<<40 |
		uint64(data[pos+3])<<32 |
		uint64(data[pos+4])<<24 |
		uint64(data[pos+5])<<16 |
		uint64(data[pos+6])<<8 |
		uint64(data[pos+7]))
	pos += 8
	keySize := int(uint32(data[pos])<<24 |
		uint32(data[pos+1])<<16 |
		uint32(data[pos+2])<<8 |
		uint32(data[pos+3]))
	pos += 4
	valueSize := int(uint32(data[pos])<<24 |
		uint32(data[pos+1])<<16 |
		uint32(data[pos+2])<<8 |
		uint32(data[pos+3]))
	pos += 4

	if len(data) < pos+keySize+valueSize {
		return nil, errors.New("data byte mismatch with pos , key and valuesize")
	}

	key := string(data[pos : pos+keySize])
	pos += keySize

	value := make([]byte, valueSize)
	copy(value, data[pos:pos+valueSize])

	record := &Record{
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
		Offset:    offset,
		Size:      int64(16 + keySize + valueSize),
	}
	return record, nil
}

func DecodeHeader(data []byte) (*RecordHeader, error) {
	if len(data) < 16 {
		return nil, errors.New("length mismatch decode header")
	}

	header := &RecordHeader{
		Timestamp: int64(binary.BigEndian.Uint64(data[0:8])),
		KeySize:   int32(binary.BigEndian.Uint32(data[8:12])),
		ValueSize: int32(binary.BigEndian.Uint32(data[12:16])),
	}

	return header, nil
}
