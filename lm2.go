package lm2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
)

const sentinelMagic = 0xDEAD10CC

var (
	// ErrDoesNotExist is returned when a collection's data file
	// doesn't exist.
	ErrDoesNotExist = errors.New("lm2: does not exist")
)

// Collection represents an ordered linked list map.
type Collection struct {
	fileHeader
	f     *os.File
	wal   *wal
	cache *recordCache

	metaLock sync.RWMutex
}

type fileHeader struct {
	Head              int64
	LastCommit        int64
	LastValidLogEntry int64
}

func (h fileHeader) bytes() []byte {
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

const recordHeaderSize = 8 + 8 + 2 + 4

func (h recordHeader) bytes() []byte {
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

	lock sync.RWMutex
}

type recordCache struct {
	cache        map[int64]*record
	maxKeyRecord *record
	size         int
	preventPurge bool
	lock         sync.RWMutex
	lastSave     time.Time

	f *os.File

	c *Collection
}

func newCache(size int, file string) (*recordCache, error) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	err = f.Truncate(0)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &recordCache{
		cache:        map[int64]*record{},
		maxKeyRecord: nil,
		size:         size,
		f:            f,
	}, nil
}

func openCache(size int, file string) (*recordCache, error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &recordCache{
		cache:        map[int64]*record{},
		maxKeyRecord: nil,
		size:         size,
		f:            f,
	}, nil
}

func (rc *recordCache) reload() {
	numRecords := 0
	b, err := ioutil.ReadAll(rc.f)
	maxNumRecords := len(b) / 8
	if err != nil {
		rc.f.Truncate(int64(maxNumRecords * 8))
		return
	}
	buf := bytes.NewReader(b)
	for i := 0; i < maxNumRecords; i++ {
		offset := int64(0)
		err = binary.Read(buf, binary.LittleEndian, &offset)
		if err != nil {
			break
		}

		rec, err := rc.c.readRecord(offset)
		if err != nil {
			break
		}

		rc.push(rec)

		numRecords++
	}

	rc.f.Truncate(int64(numRecords * 8))
}

func (rc *recordCache) close() {
	rc.f.Close()
}

func (rc *recordCache) findLastLessThan(key string) int64 {
	rc.lock.RLock()
	defer rc.lock.RUnlock()

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
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if rc.maxKeyRecord == nil || rc.maxKeyRecord.Key < rec.Key {
		rc.maxKeyRecord = rec
	} else if len(rc.cache) == rc.size && rand.Float32() >= 0.01 {
		return
	}
	rc.cache[rec.Offset] = rec
	if !rc.preventPurge {
		rc.purge()
	}
}

func (rc *recordCache) save() {
	_, err := rc.f.Seek(0, 0)
	if err != nil {
		return
	}
	b := bytes.NewBuffer(make([]byte, 0, rc.size))
	for offset := range rc.cache {
		binary.Write(b, binary.LittleEndian, offset)
	}
	rc.f.Write(b.Bytes())
	rc.f.Sync()
	rc.lastSave = time.Now()
}

func (rc *recordCache) purge() {
	purged := 0
	for len(rc.cache) > rc.size {
		deletedKey := int64(0)
		for k := range rc.cache {
			if k == rc.maxKeyRecord.Offset {
				continue
			}
			deletedKey = k
			break
		}
		delete(rc.cache, deletedKey)
		purged++
	}
	if time.Now().Sub(rc.lastSave) > time.Second {
		rc.save()
	}
}

func (rc *recordCache) forcePush(rec *record) {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if rc.maxKeyRecord == nil || rc.maxKeyRecord.Key < rec.Key {
		rc.maxKeyRecord = rec
	}
	rc.cache[rec.Offset] = rec
}

func (c *Collection) readRecord(offset int64) (*record, error) {
	if offset == 0 {
		return nil, errors.New("lm2: invalid record offset 0")
	}

	c.cache.lock.RLock()
	if rec := c.cache.cache[offset]; rec != nil {
		c.cache.lock.RUnlock()
		return rec, nil
	}
	c.cache.lock.RUnlock()

	recordHeaderBytes := [recordHeaderSize]byte{}
	n, err := c.f.ReadAt(recordHeaderBytes[:], offset)
	if err != nil {
		return nil, err
	}
	if n != recordHeaderSize {
		return nil, errors.New("lm2: partial read")
	}

	header := recordHeader{}
	err = binary.Read(bytes.NewReader(recordHeaderBytes[:]), binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	keyValBuf := make([]byte, int(header.KeyLen)+int(header.ValLen))
	n, err = c.f.ReadAt(keyValBuf, offset+recordHeaderSize)
	if err != nil {
		return nil, err
	}
	if n != len(keyValBuf) {
		return nil, errors.New("lm2: partial read")
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

	buf := bytes.NewBuffer(nil)

	err = binary.Write(buf, binary.LittleEndian, rec.recordHeader)
	if err != nil {
		return 0, err
	}

	_, err = buf.WriteString(rec.Key)
	if err != nil {
		return 0, err
	}
	_, err = buf.WriteString(rec.Value)
	if err != nil {
		return 0, err
	}

	n, err := c.f.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("lm2: error writing record: %v", err)
	}
	if n != buf.Len() {
		return 0, errors.New("lm2: partial write")
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

func (c *Collection) findLastLessThanOrEqual(key string, startingOffset int64) (int64, error) {
	offset := startingOffset

	if c.Head == 0 {
		// Empty collection.
		return 0, nil
	}

	var rec *record
	var err error
	if offset == 0 {
		// read the head
		rec, err = c.readRecord(c.Head)
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
	} else {
		rec, err = c.readRecord(offset)
		if err != nil {
			return 0, err
		}
	}

	for rec != nil {
		c.cache.push(rec)
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
	c.metaLock.Lock()
	defer c.metaLock.Unlock()

	// Clean up WriteBatch.
	wb.cleanup()

	// Find and load records that will be modified into the cache.
	recordsToLoad := map[int64]struct{}{}
	keys := []string{}
	lastLessThanOrEqualCache := map[string]int64{}

	for key := range wb.sets {
		keys = append(keys, key)
	}

	// Sort keys to be inserted.
	sort.Strings(keys)

	startingOffset := int64(0)
	for _, key := range keys {
		offset, err := c.findLastLessThanOrEqual(key, startingOffset)
		if err != nil {
			return err
		}
		if offset > 0 {
			recordsToLoad[offset] = struct{}{}
			lastLessThanOrEqualCache[key] = offset
			startingOffset = offset
		}
	}

	// Prevent cache purges.
	c.cache.lock.Lock()
	c.cache.preventPurge = true
	c.cache.lock.Unlock()
	defer func() {
		c.cache.lock.Lock()
		c.cache.preventPurge = false
		c.cache.purge()
		c.cache.lock.Unlock()
	}()

	recordsToUnlock := []*record{}
	for offset := range recordsToLoad {
		rec, err := c.readRecord(offset)
		if err != nil {
			return err
		}
		rec.lock.Lock()
		recordsToUnlock = append(recordsToUnlock, rec)
		c.cache.forcePush(rec)
	}

	defer func() {
		for _, rec := range recordsToUnlock {
			rec.lock.Unlock()
		}
	}()

	// NOTE: we shouldn't be reading any more records after this point.
	// TODO: assert it.

	walEntry := newWALEntry()

	// Append new records with the appropriate "next" pointers.
	overwrittenRecords := []int64{}
	startingOffset = int64(0)
	for _, key := range keys {
		value := wb.sets[key]

		// Find last less than.
		offset, err := c.findLastLessThanOrEqual(key, startingOffset)
		if err != nil {
			return err
		}
		startingOffset = offset
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
		walEntry.Push(newWALRecord(prevRec.Offset, prevRec.recordHeader.bytes()))
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
		offset, err := c.findLastLessThanOrEqual(key, 0)
		if err != nil {
			return err
		}
		rec, err := c.readRecord(offset)
		if err != nil {
			return err
		}
		if rec.Deleted == 0 {
			rec.Deleted = currentOffset
			walEntry.Push(newWALRecord(rec.Offset, rec.recordHeader.bytes()))
		}
	}

	for _, offset := range overwrittenRecords {
		rec, err := c.readRecord(offset)
		if err != nil {
			return err
		}
		rec.Deleted = currentOffset
		walEntry.Push(newWALRecord(rec.Offset, rec.recordHeader.bytes()))
	}

	// ^ record changes should have been serialized + buffered. Write those entries
	// out to the WAL.
	c.LastCommit = currentOffset
	walEntry.Push(newWALRecord(0, c.fileHeader.bytes()))
	logCommit, err := c.wal.Append(walEntry)
	if err != nil {
		return err
	}

	c.LastValidLogEntry = logCommit

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

// NewCollection creates a new collection with a data file at file.
// cacheSize represents the size of the collection cache.
func NewCollection(file string, cacheSize int) (*Collection, error) {
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

	cache, err := newCache(cacheSize, file+".cache")
	if err != nil {
		f.Close()
		wal.Close()
		return nil, err
	}
	c := &Collection{
		f:     f,
		wal:   wal,
		cache: cache,
	}
	c.cache.c = c

	// write file header
	c.fileHeader.Head = 0
	c.fileHeader.LastCommit = int64(8 * 3)
	c.f.Seek(0, 0)
	err = binary.Write(c.f, binary.LittleEndian, c.fileHeader)
	if err != nil {
		c.f.Close()
		c.wal.Close()
		return nil, err
	}
	return c, nil
}

// OpenCollection opens a collection with a data file at file.
// cacheSize represents the size of the collection cache.
// ErrDoesNotExist is returned if file does not exist.
func OpenCollection(file string, cacheSize int) (*Collection, error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrDoesNotExist
		}
		return nil, fmt.Errorf("lm2: error opening data file: %v", err)
	}

	wal, err := openWAL(file + ".wal")
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("lm2: error WAL: %v", err)
	}

	cache, err := openCache(cacheSize, file+".cache")
	if err != nil {
		f.Close()
		wal.Close()
		return nil, err
	}
	c := &Collection{
		f:     f,
		wal:   wal,
		cache: cache,
	}
	c.cache.c = c

	// read file header
	c.f.Seek(0, 0)
	err = binary.Read(c.f, binary.LittleEndian, &c.fileHeader)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("lm2: error reading file header: %v", err)
	}

	// Read last WAL entry.
	lastEntry, err := c.wal.ReadLastEntry()
	if err != nil {
		// Maybe latest WAL write didn't succeed.
		// Read the last known good one.
		err = c.wal.SetOffset(c.LastValidLogEntry)
		if err != nil {
			// Nothing else to do. Bail out.
			c.Close()
			return nil, err
		}
		lastEntry, err = c.wal.ReadEntry()
		if err != nil {
			if c.wal.fileSize > 0 {
				// Nothing else to do. Bail out.
				c.Close()
				return nil, err
			}
		} else {
			c.wal.Truncate()
		}
	}

	if lastEntry != nil {
		// Apply last WAL entry again.
		for _, walRec := range lastEntry.records {
			n, err := c.f.WriteAt(walRec.Data, walRec.Offset)
			if err != nil {
				c.Close()
				return nil, err
			}
			if int64(n) != walRec.Size {
				c.Close()
				return nil, errors.New("lm2: incomplete data write")
			}
		}
	}

	c.f.Truncate(c.LastCommit)

	err = c.sync()
	if err != nil {
		c.Close()
		return nil, err
	}

	// Reload cached entries.
	c.cache.reload()

	return c, nil
}

func (c *Collection) sync() error {
	if err := c.wal.f.Sync(); err != nil {
		return errors.New("lm2: error syncing WAL")
	}
	if err := c.f.Sync(); err != nil {
		return errors.New("lm2: error syncing data file")
	}
	return nil
}

// Close closes a collection and all of its resources.
func (c *Collection) Close() {
	c.f.Close()
	c.wal.Close()
	c.cache.close()
}
