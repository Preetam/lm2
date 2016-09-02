package lm2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"sort"
)

const sentinelMagic = 0xDEAD10CC

const cacheSize = 100000

type Collection struct {
	fileHeader
	f     *os.File
	wal   *WAL
	cache *recordCache
}

type fileHeader struct {
	Head          int64
	LastCommit    int64 // Unused for now
	LastLogCommit int64 // Unused for now
}

func (h fileHeader) Bytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, h)
	return buf.Bytes()
}

type recordHeader struct {
	Next    int64
	Deleted int64
	KeyLen  uint16
	ValLen  uint32
}

func (h recordHeader) Bytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, h)
	return buf.Bytes()
}

type sentinelRecord struct {
	Magic  uint32 // some fixed pattern
	Offset int64  // this record's offset
}

type record struct {
	recordHeader
	Offset int64
	Key    string
	Value  string
}

type recordCache struct {
	cache        map[int64]*record
	maxKeyRecord *record
	size         int
	preventPurge bool

	c *Collection
}

func newCache(size int) *recordCache {
	return &recordCache{
		cache:        map[int64]*record{},
		maxKeyRecord: nil,
		size:         size,
	}
}

func (rc *recordCache) findLastLessThan(key string) int64 {
	if rc.maxKeyRecord != nil {
		if rc.maxKeyRecord.Key < key {
			return rc.maxKeyRecord.Offset
		}
	}
	max := ""
	maxOffset := int64(0)

	for offset, record := range rc.cache {
		if record.Key >= key {
			continue
		}
		if record.Key > max {
			max = record.Key
			maxOffset = offset
		}
	}
	return maxOffset
}

func (rc *recordCache) push(rec *record) {
	if rc.maxKeyRecord == nil || rc.maxKeyRecord.Key < rec.Key {
		rc.maxKeyRecord = rec
	} else if rand.Float32() >= 0.04 {
		return
	}
	rc.cache[rec.Offset] = rec
	if len(rc.cache) > rc.size && !rc.preventPurge {
		deletedKey := int64(0)
		for k := range rc.cache {
			if k == rc.maxKeyRecord.Offset {
				continue
			}
			deletedKey = k
			return
		}
		delete(rc.cache, deletedKey)
	}
}

func (rc *recordCache) forcePush(rec *record) {
	if rc.maxKeyRecord == nil || rc.maxKeyRecord.Key < rec.Key {
		rc.maxKeyRecord = rec
	}
	rc.cache[rec.Offset] = rec
}

func (c *Collection) readRecord(offset int64) (*record, error) {
	if offset == 0 {
		return nil, errors.New("lm2: invalid record offset 0")
	}

	if rec := c.cache.cache[offset]; rec != nil {
		return rec, nil
	}

	_, err := c.f.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	header := recordHeader{}
	err = binary.Read(c.f, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	keyValBuf := make([]byte, int(header.KeyLen)+int(header.ValLen))
	_, err = c.f.Read(keyValBuf)
	if err != nil {
		return nil, err
	}

	key := string(keyValBuf[:int(header.KeyLen)])
	value := string(keyValBuf[int(header.KeyLen):])

	rec := &record{
		recordHeader: header,
		Offset:       offset,
		Key:          key,
		Value:        value,
	}

	c.cache.push(rec)

	return rec, nil
}

func (c *Collection) setRecordNext(offset int64, next int64) error {
	if rec := c.cache.cache[offset]; rec != nil {
		rec.recordHeader.Next = next
		return nil
	}
	_, err := c.f.Seek(offset, 0)
	if err != nil {
		return err
	}
	return binary.Write(c.f, binary.LittleEndian, next)
}

func (c *Collection) nextRecord(rec *record) *record {
	if rec == nil {
		return nil
	}
	nextRec, err := c.readRecord(rec.Next)
	if err != nil {
		return nil
	}
	return nextRec
}

func (c *Collection) writeRecord(rec *record) (int64, error) {
	offset, err := c.f.Seek(0, 2)
	if err != nil {
		return 0, err
	}

	rec.KeyLen = uint16(len(rec.Key))
	rec.ValLen = uint32(len(rec.Value))

	err = binary.Write(c.f, binary.LittleEndian, rec.recordHeader)
	if err != nil {
		return 0, err
	}

	_, err = c.f.WriteString(rec.Key)
	if err != nil {
		return 0, err
	}
	_, err = c.f.WriteString(rec.Value)
	if err != nil {
		return 0, err
	}

	rec.Offset = offset
	return offset, nil
}

func (c *Collection) writeSentinel() (int64, error) {
	offset, err := c.f.Seek(0, 2)
	if err != nil {
		return 0, err
	}
	sentinel := sentinelRecord{
		Magic:  sentinelMagic,
		Offset: offset,
	}
	err = binary.Write(c.f, binary.LittleEndian, sentinel)
	if err != nil {
		return 0, err
	}
	return offset + 12, nil
}

func (c *Collection) updateRecordHeader(offset int64, header recordHeader) error {
	if rec := c.cache.cache[offset]; rec != nil {
		rec.recordHeader = header
		return nil
	}
	_, err := c.f.Seek(offset, 0)
	if err != nil {
		return err
	}
	return binary.Write(c.f, binary.LittleEndian, header)
}

func (c *Collection) findLastLessThanOrEqual(key string) (int64, error) {
	offset := int64(0)

	if c.Head == 0 {
		// Empty collection.
		return 0, nil
	}

	// read the head
	rec, err := c.readRecord(c.Head)
	if err != nil {
		return 0, err
	}
	if rec.Key > key { // we have a new head
		return 0, nil
	}

	cacheResult := c.cache.findLastLessThan(key)
	if cacheResult != 0 {
		rec, err = c.readRecord(cacheResult)
		if err != nil {
			return 0, err
		}
	}

	offset = rec.Offset

	for rec != nil {
		if rec.Key > key {
			break
		}
		offset = rec.Offset
		rec = c.nextRecord(rec)
	}

	return offset, nil
}

// Update atomically and durably applies a WriteBatch (a set of updates) to the collection.
func (c *Collection) Update(wb *WriteBatch) error {
	// Clean up WriteBatch.
	wb.cleanup()

	// Find and load records that will be modified into the cache.
	recordsToLoad := []int64{}

	keys := []string{}
	for key := range wb.sets {
		keys = append(keys, key)
		offset, err := c.findLastLessThanOrEqual(key)
		if err != nil {
			return err
		}
		if offset > 0 {
			recordsToLoad = append(recordsToLoad, offset)
		}
	}

	// Prevent cache purges.
	c.cache.preventPurge = true
	defer func() { c.cache.preventPurge = false }()

	for _, offset := range recordsToLoad {
		rec, err := c.readRecord(offset)
		if err != nil {
			return err
		}
		c.cache.forcePush(rec)
	}

	// Sort keys to be inserted.
	sort.Strings(keys)

	// NOTE: we shouldn't be reading any more records after this point.
	// TODO: assert it.

	walEntry := newWALEntry()
	walEntry.Push(newWALRecord(0, c.fileHeader.Bytes()))

	// Append new records with the appropriate "next" pointers.

	overwrittenRecords := []int64{}
	for _, key := range keys {
		value := wb.sets[key]

		// Find last less than.
		offset, err := c.findLastLessThanOrEqual(key)
		if err != nil {
			return err
		}
		if offset == 0 {
			// Head.
			rec := &record{
				recordHeader: recordHeader{
					Next: c.Head,
				},
				Key:   key,
				Value: value,
			}
			newRecordOffset, err := c.writeRecord(rec)
			if err != nil {
				return err
			}
			c.Head = newRecordOffset
			walEntry.Push(newWALRecord(0, c.fileHeader.Bytes()))
			continue
		}
		prevRec, err := c.readRecord(offset)
		if err != nil {
			return err
		}
		rec := &record{
			recordHeader: recordHeader{
				Next: prevRec.Next,
			},
			Key:   key,
			Value: value,
		}
		newRecordOffset, err := c.writeRecord(rec)
		prevRec.Next = newRecordOffset
		walEntry.Push(newWALRecord(prevRec.Offset, prevRec.recordHeader.Bytes()))
		if prevRec.Key == key {
			overwrittenRecords = append(overwrittenRecords, prevRec.Offset)
		}
		c.cache.forcePush(rec)
		c.cache.forcePush(prevRec)
	}

	// Write sentinel record.

	currentOffset, err := c.writeSentinel()
	if err != nil {
		return err
	}

	// fsync data file.
	err = c.f.Sync()
	if err != nil {
		return err
	}

	// Mark deleted and overwritten records as "deleted" at sentinel offset.
	// (This happens in memory.)

	for key := range wb.deletes {
		offset, err := c.findLastLessThanOrEqual(key)
		if err != nil {
			return err
		}
		rec, err := c.readRecord(offset)
		if err != nil {
			return err
		}
		if rec.Deleted == 0 {
			rec.Deleted = currentOffset
			walEntry.Push(newWALRecord(rec.Offset, rec.recordHeader.Bytes()))
		}
	}

	for _, offset := range overwrittenRecords {
		rec, err := c.readRecord(offset)
		if err != nil {
			return err
		}
		rec.Deleted = currentOffset
		walEntry.Push(newWALRecord(rec.Offset, rec.recordHeader.Bytes()))
	}

	// ^ record changes should have been serialized + buffered. Write those entries
	// out to the WAL.
	c.LastCommit = currentOffset
	walEntry.Push(newWALRecord(0, c.fileHeader.Bytes()))
	logCommit, err := c.wal.Append(walEntry)
	if err != nil {
		return err
	}

	c.LastLogCommit = logCommit

	// Update + fsync data file header.

	for _, walRec := range walEntry.records {
		n, err := c.f.WriteAt(walRec.Data, walRec.Offset)
		if err != nil {
			return err
		}
		if int64(n) != walRec.Size {
			return errors.New("lm2: incomplete data write")
		}
	}

	return c.f.Sync()
}

func NewCollection(file string) (*Collection, error) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(0)
	if err != nil {
		f.Close()
		return nil, err
	}

	wal, err := newWAL(file + ".wal")
	if err != nil {
		f.Close()
		return nil, err
	}

	c := &Collection{
		f:     f,
		wal:   wal,
		cache: newCache(cacheSize),
	}
	c.cache.c = c

	// write file header
	c.fileHeader.Head = 0
	c.f.Seek(0, 0)
	err = binary.Write(c.f, binary.LittleEndian, c.fileHeader)
	if err != nil {
		c.f.Close()
		c.wal.Close()
		return nil, err
	}
	return c, nil
}

func OpenCollection(file string) (*Collection, error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	wal, err := newWAL(file + ".wal")
	if err != nil {
		f.Close()
		return nil, err
	}

	c := &Collection{
		f:     f,
		wal:   wal,
		cache: newCache(cacheSize),
	}
	c.cache.c = c

	// read file header
	c.f.Seek(0, 0)
	err = binary.Read(c.f, binary.LittleEndian, &c.fileHeader)
	if err != nil {
		c.f.Close()
		c.wal.Close()
		return nil, err
	}
	return c, nil
}

func (c *Collection) Close() {
	c.f.Close()
	c.wal.Close()
}
