package lm2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

const (
	walMagic       = sentinelMagic
	walFooterMagic = ^uint32(walMagic)
)

type wal struct {
	f              *os.File
	fileSize       int64
	lastGoodOffset int64
}

type walEntryHeader struct {
	Magic      uint32
	Length     int64
	NumRecords uint32
}

type walEntryFooter struct {
	Magic       uint32
	StartOffset int64
}

type walEntry struct {
	walEntryHeader
	records []walRecord
	walEntryFooter
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
		walEntryFooter: walEntryFooter{
			Magic:       walFooterMagic,
			StartOffset: 0,
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

func openWAL(filename string) (*wal, error) {
	f, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &wal{
		f:        f,
		fileSize: stat.Size(),
	}, nil
}

func newWAL(filename string) (*wal, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(0)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &wal{
		f: f,
	}, nil
}

func (w *wal) Append(entry *walEntry) (int64, error) {
	startOffset, err := w.f.Seek(0, 2)
	if err != nil {
		w.Truncate()
		return 0, errors.New("lm2: couldn't get offset")
	}
	entry.StartOffset = startOffset

	buf := bytes.NewBuffer(nil)
	for _, rec := range entry.records {
		buf.Write(rec.Bytes())
	}
	entry.Length = int64(buf.Len())

	binary.Write(buf, binary.LittleEndian, entry.walEntryFooter)

	err = binary.Write(w.f, binary.LittleEndian, entry.walEntryHeader)
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
	if err != nil {
		w.Truncate()
		return 0, errors.New("lm2: couldn't get offset")
	}

	err = w.f.Sync()
	if err != nil {
		w.Truncate()
		return 0, err
	}

	w.lastGoodOffset = currentOffset
	return startOffset, nil
}

func (w *wal) ReadLastEntry() (*walEntry, error) {
	// Read footer
	const footerSize = 12
	_, err := w.f.Seek(-footerSize, 2)
	if err != nil {
		return nil, errors.New("lm2: error seeking to WAL footer")
	}
	footer := walEntryFooter{}
	err = binary.Read(w.f, binary.LittleEndian, &footer)
	if err != nil {
		return nil, errors.New("lm2: error reading WAL footer")
	}
	if footer.Magic != walFooterMagic {
		return nil, errors.New("lm2: invalid WAL footer magic")
	}

	// Read entry.

	_, err = w.f.Seek(footer.StartOffset, 0)
	if err != nil {
		return nil, errors.New("lm2: error seeking to WAL entry start")
	}

	return w.ReadEntry()
}

func (w *wal) SetOffset(offset int64) error {
	_, err := w.f.Seek(offset, 0)
	return err
}

func (w *wal) ReadEntry() (*walEntry, error) {
	entry := newWALEntry()

	err := binary.Read(w.f, binary.LittleEndian, &entry.walEntryHeader)
	if err != nil {
		return nil, errors.New("lm2: error reading WAL entry header")
	}
	if entry.walEntryHeader.Magic != walMagic {
		return nil, errors.New("lm2: invalid WAL header magic")
	}

	b := make([]byte, int(entry.walEntryHeader.Length))
	n, err := w.f.Read(b)
	if err != nil {
		return nil, errors.New("lm2: error reading WAL body")
	}
	if n != len(b) {
		return nil, errors.New("lm2: partial read")
	}

	r := bytes.NewReader(b)
	numRecords := int(entry.walEntryHeader.NumRecords)
	entry.walEntryHeader.NumRecords = 0
	for i := 0; i < numRecords; i++ {
		recHeader := walRecordHeader{}
		err = binary.Read(r, binary.LittleEndian, &recHeader)
		if err != nil {
			return nil, errors.New("lm2: error reading WAL record header")
		}
		walRecordBytes := make([]byte, int(recHeader.Size))
		n, err := r.Read(walRecordBytes)
		if err != nil {
			return nil, errors.New("lm2: error reading WAL record body")
		}
		if n != len(walRecordBytes) {
			return nil, errors.New("lm2: partial read")
		}

		entry.Push(newWALRecord(recHeader.Offset, walRecordBytes))
	}

	err = binary.Read(w.f, binary.LittleEndian, &entry.walEntryFooter)
	if err != nil {
		return nil, err
	}

	if entry.walEntryFooter.Magic != walFooterMagic {
		return nil, errors.New("lm2: invalid WAL footer magic")
	}

	currentOffset, err := w.f.Seek(0, 2)
	if err != nil {
		return nil, errors.New("lm2: couldn't get offset")
	}

	w.lastGoodOffset = currentOffset

	return entry, nil
}

func (w *wal) Truncate() error {
	return w.f.Truncate(w.lastGoodOffset)
}

func (w *wal) Close() {
	w.f.Close()
}
