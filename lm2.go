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

type collection struct {
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
	if rand.Float32() > 0.1 {
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

func (c *collection) readRecord(offset int64) (*record, error) {
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

func (c *collection) setRecordNext(offset int64, next int64) error {
	_, err := c.f.Seek(offset, 0)
	if err != nil {
		return err
	}
	return binary.Write(c.f, binary.LittleEndian, next)
}

func (c *collection) nextRecord(rec *record) *record {
	if rec == nil {
		return nil
	}
	nextRec, err := c.readRecord(rec.Next)
	if err != nil {
		return nil
	}
	return nextRec
}

func (c *collection) writeRecord(rec *record) (int64, error) {
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

	return offset, nil
}

func (c *collection) updateRecordHeader(offset int64, header recordHeader) error {
	_, err := c.f.Seek(offset, 0)
	if err != nil {
		return err
	}
	return binary.Write(c.f, binary.LittleEndian, header)
}

func (c *collection) set(key, value string) error {
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

func newCollection(file string) (*collection, error) {
	f, err := os.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &collection{
		f: f,
		cache: &keyCache{
			cache: map[string]int64{},
			size:  1024,
		},
	}, nil
}

func (c *collection) close() {
	c.f.Close()
}

func main() {
	c, err := newCollection("/tmp/test.lm2")
	if err != nil {
		log.Fatal(err)
	}
	defer c.close()

	const N = 5000
	vals := map[string]string{}
	randStart := time.Now()
	for i := 0; i < N*8; i++ {
		vals[fmt.Sprint(rand.Intn(N*4))] = fmt.Sprint(i)
	}
	randEnd := time.Now()
	for key, val := range vals {
		if err := c.set(key, val); err != nil {
			log.Fatal(err)
		}
	}
	insertEnd := time.Now()

	rec, err := c.readRecord(c.Head)
	if err != nil {
		log.Fatal(err)
	}

	for ; rec != nil; rec = c.nextRecord(rec) {
		if rec.Deleted > 0 {
			continue
		}
		log.Println(rec.Key, "=>", rec.Value)
	}

	log.Println(randEnd.Sub(randStart), insertEnd.Sub(randEnd))
}
