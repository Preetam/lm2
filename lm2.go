package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

type Collection struct {
	fileHeader
	f     *os.File
	cache *keyCache
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

type keyCache struct {
	cache map[string]int64
	size  int
}

func (kc *keyCache) findLastLessThan(key string) int64 {
	max := ""
	for k := range kc.cache {
		if k >= key {
			continue
		}
		if k > max {
			max = k
		}
	}
	return kc.cache[max]
}

func (kc *keyCache) push(key string, offset int64) {
	if rand.Float32() >= 0.04 {
		return
	}
	if kc.cache[key] < offset {
		kc.cache[key] = offset
	}
	if len(kc.cache) > kc.size {
		for k := range kc.cache {
			delete(kc.cache, k)
			return
		}
	}
}

func (c *Collection) readRecord(offset int64) (*record, error) {
	if offset == 0 {
		return nil, errors.New("lm2: invalid record offset")
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

	c.cache.push(key, offset)

	return &record{
		recordHeader: header,
		Offset:       offset,
		Key:          key,
		Value:        value,
	}, nil
}

func (c *Collection) setRecordNext(offset int64, next int64) error {
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

	c.cache.push(rec.Key, offset)

	return offset, nil
}

func (c *Collection) updateRecordHeader(offset int64, header recordHeader) error {
	_, err := c.f.Seek(offset, 0)
	if err != nil {
		return err
	}
	return binary.Write(c.f, binary.LittleEndian, header)
}

func (c *Collection) set(key, value string) error {
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

func NewCollection(file string) (*Collection, error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	c := &Collection{
		f: f,
		cache: &keyCache{
			cache: map[string]int64{},
			size:  8192,
		},
	}

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
		f: f,
		cache: &keyCache{
			cache: map[string]int64{},
			size:  8192,
		},
	}

	// read file header
	c.f.Seek(0, 0)
	err = binary.Read(c.f, binary.LittleEndian, &c.fileHeader)
	if err != nil {
		c.f.Close()
		return nil, err
	}
	return c, nil
}

func (c *Collection) close() {
	c.f.Close()
}

func main() {
	c, err := NewCollection("/tmp/test.lm2")
	if err != nil {
		log.Fatal(err)
	}
	defer c.close()

	const N = 5000
	insertStart := time.Now()
	for i := 0; i < N; i++ {
		key := fmt.Sprint(rand.Intn(N * 4))
		val := fmt.Sprint(i)
		if err := c.set(key, val); err != nil {
			log.Fatal(err)
		}
	}
	insertEnd := time.Now()

	cur, err := c.NewCursor()
	if err != nil {
		log.Fatal(err)
	}
	for cur.Next() {
		log.Println(cur.Key(), "=>", cur.Value())
	}

	iterationEnd := time.Now()

	log.Println(insertEnd.Sub(insertStart), iterationEnd.Sub(insertEnd))
	log.Println(len(c.cache.cache))
}
