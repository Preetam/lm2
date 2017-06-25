package lm2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

const sentinelMagic = 0xDEAD10CC
const maxLevels = 20

var (
	// ErrDoesNotExist is returned when a collection's data file
	// doesn't exist.
	ErrDoesNotExist = errors.New("lm2: does not exist")
	// ErrInternal is returned when the internal state of the collection
	// is invalid. The collection should be closed and reopened.
	ErrInternal = errors.New("lm2: internal error")
)

// Collection represents an ordered linked list map.
type Collection struct {
	fileHeader
	f         *os.File
	wal       *wal
	stats     Stats
	dirty     map[int64]*record
	dirtyLock sync.Mutex

	// internalState is 0 if OK, 1 if inconsistent.
	internalState uint32

	metaLock sync.RWMutex
}

type fileHeader struct {
	Next       [maxLevels]int64
	LastCommit int64
}

func (h fileHeader) bytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, h)
	return buf.Bytes()
}

type recordHeader struct {
	Next    [maxLevels]int64
	Deleted int64
	KeyLen  uint16
	ValLen  uint32
}

const recordHeaderSize = (maxLevels * 8) + 8 + 2 + 4

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

func generateLevel() int {
	level := 0
	for i := 0; i < maxLevels-1; i++ {
		if rand.Float32() <= 0.5 {
			level++
		} else {
			break
		}
	}
	return level
}

func (c *Collection) getDirty(offset int64) *record {
	c.dirtyLock.Lock()
	defer c.dirtyLock.Unlock()
	if c.dirty == nil {
		return nil
	}
	return c.dirty[offset]
}

func (c *Collection) setDirty(offset int64, rec *record) {
	c.dirtyLock.Lock()
	defer c.dirtyLock.Unlock()
	c.dirty[offset] = rec
}

func (c *Collection) readRecord(offset int64) (*record, error) {
	//time.Sleep(time.Second / 10)
	//log.Println("readRecord", offset)
	if offset == 0 {
		return nil, errors.New("lm2: invalid record offset 0")
	}

	if rec := c.getDirty(offset); rec != nil {
		return rec, nil
	}

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
	c.stats.incRecordsRead(1)
	c.stats.incCacheMisses(1)

	return rec, nil
}

func (c *Collection) nextRecord(rec *record, level int) (*record, error) {
	//log.Println("nextRecord", rec)
	if rec == nil {
		return nil, errors.New("lm2: invalid record")
	}
	if atomic.LoadInt64(&rec.Next[level]) == 0 {
		// There's no next record.
		return nil, nil
	}
	nextRec, err := c.readRecord(atomic.LoadInt64(&rec.Next[level]))
	if err != nil {
		return nil, err
	}
	return nextRec, nil
}

func writeRecord(rec *record, currentOffset int64, buf *bytes.Buffer) error {
	rec.KeyLen = uint16(len(rec.Key))
	rec.ValLen = uint32(len(rec.Value))

	err := binary.Write(buf, binary.LittleEndian, rec.recordHeader)
	if err != nil {
		return err
	}

	_, err = buf.WriteString(rec.Key)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(rec.Value)
	if err != nil {
		return err
	}

	rec.Offset = currentOffset
	return nil
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

func (c *Collection) findLastLessThanOrEqual(key string, startingOffset int64, level int) (int64, error) {
	//log.Println("findLastLessThanOrEqual", key, startingOffset, level)
	offset := startingOffset

	if c.Next[level] == 0 {
		// Empty collection.
		return 0, nil
	}

	var rec *record
	var err error
	if offset == 0 {
		// read the head
		rec, err = c.readRecord(c.Next[level])
		if err != nil {
			//log.Println("!")
			return 0, err
		}
		if rec.Key > key { // we have a new head
			return 0, nil
		}
	} else {
		rec, err = c.readRecord(offset)
		if err != nil {
			//log.Println("!!", offset)
			return 0, err
		}
	}

	for rec != nil {
		//log.Println(rec.Offset, rec.Key, rec.Next)
		//time.Sleep(time.Second / 10)
		rec.lock.RLock()
		if rec.Key > key {
			rec.lock.RUnlock()
			break
		}
		offset = rec.Offset
		oldRec := rec
		rec, err = c.nextRecord(oldRec, level)
		if err != nil {
			return 0, err
		}
		oldRec.lock.RUnlock()
	}

	return offset, nil
}

// Update atomically and durably applies a WriteBatch (a set of updates) to the collection.
// It returns the new version (on success) and an error.
func (c *Collection) Update(wb *WriteBatch) (int64, error) {
	if atomic.LoadUint32(&c.internalState) != 0 {
		return 0, ErrInternal
	}

	c.metaLock.Lock()
	defer c.metaLock.Unlock()

	c.dirtyLock.Lock()
	c.dirty = map[int64]*record{}
	c.dirtyLock.Unlock()
	defer func() {
		c.dirtyLock.Lock()
		c.dirty = nil
		c.dirtyLock.Unlock()
	}()

	// Clean up WriteBatch.
	wb.cleanup()

	// Find and load records that will be modified into the cache.

	mergedSetDeleteKeys := map[string]struct{}{}
	for key := range wb.sets {
		mergedSetDeleteKeys[key] = struct{}{}
	}
	keys := []string{}
	for key := range mergedSetDeleteKeys {
		keys = append(keys, key)
	}

	// Sort keys to be inserted or deleted.
	sort.Strings(keys)

	walEntry := newWALEntry()
	appendBuf := bytes.NewBuffer(nil)
	currentOffset, err := c.f.Seek(0, 2)
	if err != nil {
		atomic.StoreUint32(&c.internalState, 1)
		return 0, errors.New("lm2: couldn't get current file offset")
	}

	overwrittenRecords := []int64{}
	startingOffsets := [maxLevels]int64{}
	for _, key := range keys {
		value := wb.sets[key]
		level := generateLevel()
		rec := &record{
			recordHeader: recordHeader{
				Next: [maxLevels]int64{},
			},
			Key:   key,
			Value: value,
		}
		newRecordOffset := currentOffset + int64(appendBuf.Len())

		for i := maxLevels - 1; i > level; i-- {
			offset, err := c.findLastLessThanOrEqual(key, startingOffsets[i], i)
			if err != nil {
				//log.Println("asdf")
				return 0, err
			}
			if offset > 0 {
				startingOffsets[i] = offset
				if i > 0 {
					startingOffsets[i-1] = offset
				}
			}
		}

		for ; level >= 0; level-- {
			offset, err := c.findLastLessThanOrEqual(key, startingOffsets[level], level)
			if err != nil {
				//log.Println("asdf")
				return 0, err
			}
			if offset == 0 {
				// Insert at head
				//log.Println("inserting at head:", key)
				//log.Println("head was", c.fileHeader.Next[level])
				atomic.StoreInt64(&rec.Next[level], c.fileHeader.Next[level])
				atomic.StoreInt64(&c.fileHeader.Next[level], newRecordOffset)
			} else {
				// Have a previous record
				var prevRec *record
				if prev := c.getDirty(offset); prev != nil {
					prevRec = prev
				} else {
					//log.Println("asdfasdfasdf")
					prevRec, err = c.readRecord(offset)
					if err != nil {
						atomic.StoreUint32(&c.internalState, 1)
						return 0, err
					}
				}
				//log.Println(prevRec.Key, "next was", prevRec.Next)
				atomic.StoreInt64(&rec.Next[level], prevRec.Next[level])
				//log.Println(prevRec.Key, "next is", rec.Key)
				atomic.StoreInt64(&prevRec.Next[level], newRecordOffset)
				c.setDirty(prevRec.Offset, prevRec)
				walEntry.Push(newWALRecord(prevRec.Offset, prevRec.recordHeader.bytes()))

				if prevRec.Key == key {
					//log.Println("overwrote", key)
					overwrittenRecords = append(overwrittenRecords, prevRec.Offset)
				}

				if level > 0 {
					startingOffsets[level-1] = prevRec.Offset
				}
			}

			startingOffsets[level] = newRecordOffset

			err = writeRecord(rec, newRecordOffset, appendBuf)
			if err != nil {
				atomic.StoreUint32(&c.internalState, 1)
				return 0, err
			}
			c.setDirty(newRecordOffset, rec)
			//log.Println("new record at level ", level, rec.Key, rec.Value, "at", newRecordOffset, rec.Next)
		}
	}

	n, err := c.f.Write(appendBuf.Bytes())
	if err != nil {
		atomic.StoreUint32(&c.internalState, 1)
		return 0, errors.New("lm2: appending records failed")
	}
	if n != appendBuf.Len() {
		atomic.StoreUint32(&c.internalState, 1)
		return 0, errors.New("lm2: partial write")
	}

	// Write sentinel record.

	currentOffset, err = c.writeSentinel()
	if err != nil {
		atomic.StoreUint32(&c.internalState, 1)
		return 0, err
	}

	// fsync data file.
	err = c.f.Sync()
	if err != nil {
		atomic.StoreUint32(&c.internalState, 1)
		return 0, err
	}

	// Mark deleted and overwritten records as "deleted" at sentinel offset.
	// (This happens in memory.)
	//
	// for key := range wb.deletes {
	// 	offset := lastLessThanOrEqualCache[key]
	// 	if offset == 0 {
	// 		continue
	// 	}
	// 	rec, err := c.readRecord(offset)
	// 	if err != nil {
	// 		atomic.StoreUint32(&c.internalState, 1)
	// 		return 0, err
	// 	}
	// 	if rec.Deleted == 0 {
	// 		rec.Deleted = currentOffset
	// 		walEntry.Push(newWALRecord(rec.Offset, rec.recordHeader.bytes()))
	// 	}
	// }

	c.dirtyLock.Lock()
	for _, dirtyRec := range c.dirty {
		walEntry.Push(newWALRecord(dirtyRec.Offset, dirtyRec.recordHeader.bytes()))
	}
	c.dirtyLock.Unlock()

	for key := range wb.deletes {
		offset := int64(0)
		for level := maxLevels - 1; level >= 0; level-- {
			offset, err = c.findLastLessThanOrEqual(key, offset, level)
			if err != nil {
				atomic.StoreUint32(&c.internalState, 1)
				return 0, err
			}
		}
		if offset == 0 {
			continue
		}
		rec, err := c.readRecord(offset)
		if err != nil {
			atomic.StoreUint32(&c.internalState, 1)
			return 0, err
		}
		rec.Deleted = currentOffset
		walEntry.Push(newWALRecord(rec.Offset, rec.recordHeader.bytes()))
	}

	for _, offset := range overwrittenRecords {
		var rec *record
		if dirtyRec := c.getDirty(offset); dirtyRec != nil {
			rec = dirtyRec
		} else {
			//log.Println("overwritten readRecord")
			rec, err = c.readRecord(offset)
			if err != nil {
				atomic.StoreUint32(&c.internalState, 1)
				return 0, err
			}
		}
		rec.Deleted = currentOffset
		walEntry.Push(newWALRecord(rec.Offset, rec.recordHeader.bytes()))
	}

	// ^ record changes should have been serialized + buffered. Write those entries
	// out to the WAL.
	c.LastCommit = currentOffset
	walEntry.Push(newWALRecord(0, c.fileHeader.bytes()))
	_, err = c.wal.Append(walEntry)
	if err != nil {
		atomic.StoreUint32(&c.internalState, 1)
		return 0, err
	}

	// Update + fsync data file header.
	for _, walRec := range walEntry.records {
		n, err := c.f.WriteAt(walRec.Data, walRec.Offset)
		if err != nil {
			atomic.StoreUint32(&c.internalState, 1)
			return 0, err
		}
		if int64(n) != walRec.Size {
			atomic.StoreUint32(&c.internalState, 1)
			return 0, errors.New("lm2: incomplete data write")
		}
	}

	err = c.f.Sync()
	if err != nil {
		atomic.StoreUint32(&c.internalState, 1)
		return 0, err
	}

	return c.LastCommit, nil
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

	c := &Collection{
		f:   f,
		wal: wal,
	}

	// write file header
	c.fileHeader.Next[0] = 0
	c.fileHeader.LastCommit = int64(8 * 2)
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

	c := &Collection{
		f:   f,
		wal: wal,
	}

	// Read file header.
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
		// Truncate.
		c.wal.Truncate()
	} else {
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

		// Reread file header because it could have been updated
		c.f.Seek(0, 0)
		err = binary.Read(c.f, binary.LittleEndian, &c.fileHeader)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("lm2: error reading file header: %v", err)
		}
	}

	c.f.Truncate(c.LastCommit)

	err = c.sync()
	if err != nil {
		c.Close()
		return nil, err
	}

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
	c.metaLock.Lock()
	defer c.metaLock.Unlock()
	c.f.Close()
	c.wal.Close()
}

// Version returns the last committed version.
func (c *Collection) Version() int64 {
	c.metaLock.RLock()
	defer c.metaLock.RUnlock()
	return c.LastCommit
}

// Stats returns collection statistics.
func (c *Collection) Stats() Stats {
	return c.stats.clone()
}

// Destroy closes the collection and removes its associated data files.
func (c *Collection) Destroy() error {
	c.Close()
	var err error
	err = os.Remove(c.f.Name())
	if err != nil {
		return err
	}
	err = c.wal.Destroy()
	if err != nil {
		return err
	}
	return nil
}
