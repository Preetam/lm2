package lm2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

const (
	walMagic = sentinelMagic
)

type WAL struct {
	f              *os.File
	lastGoodOffset int64
}

type walEntryHeader struct {
	Magic      uint32
	Length     int64
	NumRecords uint32
}

type walEntry struct {
	walEntryHeader
	records []walRecord
}

type walRecord struct {
	walRecordHeader
	Data []byte
}

type walRecordHeader struct {
	Offset int64
	Size   int64
}

func newWALEntry() *walEntry {
	return &walEntry{
		walEntryHeader: walEntryHeader{
			Magic:      sentinelMagic,
			NumRecords: 0,
		},
	}
}

func newWALRecord(offset int64, data []byte) walRecord {
	return walRecord{
		walRecordHeader: walRecordHeader{
			Offset: offset,
			Size:   int64(len(data)),
		},
		Data: data,
	}
}

func (rec walRecord) Bytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, rec.walRecordHeader)
	buf.Write(rec.Data)
	return buf.Bytes()
}

func (e *walEntry) Push(rec walRecord) {
	e.records = append(e.records, rec)
	e.NumRecords++
}

func openWAL(filename string) (*WAL, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return &WAL{
		f: f,
	}, nil
}

func newWAL(filename string) (*WAL, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(0)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{
		f: f,
	}, nil
}

func (w *WAL) Append(entry *walEntry) (int64, error) {
	buf := bytes.NewBuffer(nil)
	for _, rec := range entry.records {
		buf.Write(rec.Bytes())
	}
	entry.Length = int64(buf.Len())
	err := binary.Write(w.f, binary.LittleEndian, entry.walEntryHeader)
	if err != nil {
		w.Truncate()
		return 0, err
	}

	n, err := w.f.Write(buf.Bytes())
	if err != nil {
		w.Truncate()
		return 0, err
	}

	if n != buf.Len() {
		w.Truncate()
		return 0, errors.New("lm2: incomplete WAL write")
	}

	currentOffset, err := w.f.Seek(0, 2)
	if n != buf.Len() {
		w.Truncate()
		return 0, errors.New("lm2: couldn't get offset")
	}

	err = w.f.Sync()
	if err != nil {
		w.Truncate()
		return 0, err
	}

	w.lastGoodOffset = currentOffset
	return w.lastGoodOffset, nil
}

func (w *WAL) Truncate() error {
	return w.f.Truncate(w.lastGoodOffset)
}

func (w *WAL) Close() {
	w.f.Close()
}
