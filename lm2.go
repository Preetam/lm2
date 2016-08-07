package lm2

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
)

const cacheSize = 100000

type Collection struct {
	fileHeader
	f     *os.File
	cache *recordCache
}

type fileHeader struct {
	Head int64
}

type recordHeader struct {
	Next    int64
	Deleted byte
	KeyLen  uint16
	ValLen  uint32
}

type record struct {
	recordHeader
	Offset int64
	Key    string
	Value  string
}

type recordCache struct {
	cache        map[int64]*record
	dirty        map[int64]bool
	maxKeyRecord *record
	size         int

	c *Collection
}

func newCache(size int) *recordCache {
	return &recordCache{
		cache:        map[int64]*record{},
		dirty:        map[int64]bool{},
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
	//start := time.Now()
	for offset, record := range rc.cache {
		if record.Key >= key {
			continue
		}
		if record.Key > max {
			max = record.Key
			maxOffset = offset
		}
	}
	if len(rc.cache) == 10000 {
		//log.Println("cache lookup time:", time.Now().Sub(start))
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
	if len(rc.cache) > rc.size {
		deletedKey := int64(0)
		for k := range rc.cache {
			if k == rc.maxKeyRecord.Offset {
				continue
			}
			deletedKey = k
			return
		}
		if rc.dirty[deletedKey] {
			// This is a dirty record. Flush changes to disk.
			rc.c.updateRecordHeader(deletedKey, rc.cache[deletedKey].recordHeader)
			delete(rc.dirty, deletedKey)
		}
		delete(rc.cache, deletedKey)
	}
}

func (c *Collection) readRecord(offset int64) (*record, error) {
	if offset == 0 {
		return nil, errors.New("lm2: invalid record offset")
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
		c.cache.dirty[offset] = true
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
	c.cache.push(rec)

	return offset, nil
}

func (c *Collection) updateRecordHeader(offset int64, header recordHeader) error {
	if rec := c.cache.cache[offset]; rec != nil {
		rec.recordHeader = header
		c.cache.dirty[offset] = true
		return nil
	}
	_, err := c.f.Seek(offset, 0)
	if err != nil {
		return err
	}
	return binary.Write(c.f, binary.LittleEndian, header)
}

func (c *Collection) Set(key, value string) error {
	// find last less than key

	if c.Head == 0 { // first record
		// write new record
		newRecordOffset, err := c.writeRecord(&record{
			Key:   key,
			Value: value,
		})
		if err != nil {
			return err
		}

		// set head to the new record offset
		c.fileHeader.Head = newRecordOffset
		c.f.Seek(0, 0)
		return binary.Write(c.f, binary.LittleEndian, c.fileHeader)
	}

	// read the head
	rec, err := c.readRecord(c.Head)
	if err != nil {
		return err
	}
	if rec.Key > key { // we have a new head
		// write new record
		newRecordOffset, err := c.writeRecord(&record{
			recordHeader: recordHeader{
				Next: rec.Offset,
			},
			Key:   key,
			Value: value,
		})
		if err != nil {
			return err
		}

		// set head to the new record offset
		c.fileHeader.Head = newRecordOffset
		c.f.Seek(0, 0)
		return binary.Write(c.f, binary.LittleEndian, c.fileHeader)
	}

	cacheResult := c.cache.findLastLessThan(key)
	if cacheResult != 0 {
		rec, err = c.readRecord(cacheResult)
		if err != nil {
			return err
		}
	}

	prevRec := rec
	for rec != nil {
		if rec.Key > key {
			break
		}
		if rec.Key == key {
			// Equal key. Delete because we're overwriting.
			rec.Deleted = 1
			err = c.updateRecordHeader(rec.Offset, rec.recordHeader)
			if err != nil {
				return err
			}
		}
		prevRec = rec
		rec = c.nextRecord(rec)
	}

	// write the new record
	newRecordOffset, err := c.writeRecord(&record{
		recordHeader: recordHeader{
			Next: prevRec.Next,
		},
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	prevRec.Next = newRecordOffset
	return c.updateRecordHeader(prevRec.Offset, prevRec.recordHeader)
}

func (c *Collection) Delete(key string) error {
	// find last less than key

	if c.Head == 0 { // first record
		return nil
	}

	// read the head
	rec, err := c.readRecord(c.Head)
	if err != nil {
		return err
	}

	for rec != nil {
		if rec.Key > key {
			break
		}
		if rec.Key == key && rec.Deleted == 0 {
			rec.Deleted = 1
			err = c.updateRecordHeader(rec.Offset, rec.recordHeader)
			if err != nil {
				return err
			}
		}
		rec = c.nextRecord(rec)
	}
	return nil
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

	c := &Collection{
		f:     f,
		cache: newCache(cacheSize),
	}
	c.cache.c = c

	// write file header
	c.fileHeader.Head = 0
	c.f.Seek(0, 0)
	err = binary.Write(c.f, binary.LittleEndian, c.fileHeader)
	if err != nil {
		c.f.Close()
		return nil, err
	}
	return c, nil
}

func OpenCollection(file string) (*Collection, error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	c := &Collection{
		f:     f,
		cache: newCache(cacheSize),
	}
	c.cache.c = c

	// read file header
	c.f.Seek(0, 0)
	err = binary.Read(c.f, binary.LittleEndian, &c.fileHeader)
	if err != nil {
		c.f.Close()
		return nil, err
	}
	return c, nil
}

func (c *Collection) Close() {
	c.f.Close()
}
