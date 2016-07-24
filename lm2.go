package lm2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"syscall"
)

const (
	BlockSize        = 16 * 1024 // 16 kB
	HeaderSize       = 8 + 4 + 4 + 4 + 4
	BlockHeaderSize  = 4 + 4 + 1 + 1
	RecordHeaderSize = 8 + 8 + 4 + 4 + 2 + 2
)

// Block represents a set of key-value records.
type Block struct {
	id         uint32
	nextBlock  uint32
	flags      uint8
	numRecords uint8

	records []Record
}

// HeaderBlock represents the first block, which
// contains global metadata.
type HeaderBlock struct {
	// Last committed transaction
	lastTx int64
	// ID of the first block in the free list
	freeBlock uint32
	// Current block accepting writes
	currentDataBlock uint32
	// Last allocated block ID
	lastBlock uint32
	// Block with the first key
	firstBlock uint32
}

type Record struct {
	// Created transaction ID
	created int64
	// Deleted transaction ID
	deleted int64
	// Block ID of the previous record
	prev uint32
	// Block ID of the next record
	next uint32
	// Key
	key string
	// Value
	value string

	// Offset within the block
	offset int64
}

type DB struct {
	file     *os.File
	fileSize int64
	// Memory-mapped data file
	mapped []byte

	lock sync.Mutex

	// Header block
	header HeaderBlock
}

// NewDB opens the file located at filepath and initializes
// a new lm2 data file.
func NewDB(filepath string) (*DB, error) {
	f, err := os.OpenFile(filepath, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if stat.Size() < 2*BlockSize {
		f.Close()
		return nil, errors.New("lm2: file too small")
	}

	mapped, err := syscall.Mmap(int(f.Fd()), 0, int(stat.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}

	db := &DB{
		file:     f,
		fileSize: stat.Size(),
		mapped:   mapped,
		lock:     sync.Mutex{},

		header: HeaderBlock{
			lastTx:           0,
			freeBlock:        0,
			currentDataBlock: 1, // block 0 is the header block
			lastBlock:        0,
			firstBlock:       1,
		},
	}

	firstBlock := &Block{
		id: 1,
	}

	err = db.writeBlock(firstBlock)
	if err != nil {
		db.Close()
		return nil, err
	}

	err = db.writeHeader()
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() {
	syscall.Munmap(db.mapped)
	db.file.Close()
}

func (db *DB) writeHeader() error {
	buf := bytes.NewBuffer(make([]byte, 0, HeaderSize))
	err := binary.Write(buf, binary.LittleEndian, db.header)
	if err != nil {
		return err
	}
	copy(db.mapped, buf.Bytes())
	return nil
}

func (db *DB) readBlock(blockID uint32) (*Block, error) {
	r := bytes.NewReader(db.mapped)
	blockOffset := int64(blockID) * BlockSize
	r.Seek(blockOffset, 0)
	block := &Block{}
	var err error
	err = binary.Read(r, binary.LittleEndian, &block.id)
	if err != nil {
		return nil, err
	}
	if blockID != block.id {
		// Inconsistent block ID.
		return nil, errors.New("lm2: inconsistent block ID")
	}
	err = binary.Read(r, binary.LittleEndian, &block.nextBlock)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.LittleEndian, &block.flags)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.LittleEndian, &block.numRecords)
	if err != nil {
		return nil, err
	}

	for i := uint8(0); i < block.numRecords; i++ {
		offset, err := r.Seek(0, 1)
		if err != nil {
			return nil, err
		}
		rec := Record{
			offset: offset - blockOffset,
		}
		err = binary.Read(r, binary.LittleEndian, &rec.created)
		if err != nil {
			return nil, err
		}
		err = binary.Read(r, binary.LittleEndian, &rec.deleted)
		if err != nil {
			return nil, err
		}
		err = binary.Read(r, binary.LittleEndian, &rec.prev)
		if err != nil {
			return nil, err
		}
		err = binary.Read(r, binary.LittleEndian, &rec.next)
		if err != nil {
			return nil, err
		}

		keySize, valueSize := uint16(0), uint16(0)

		err = binary.Read(r, binary.LittleEndian, &keySize)
		if err != nil {
			return nil, err
		}
		err = binary.Read(r, binary.LittleEndian, &valueSize)
		if err != nil {
			return nil, err
		}

		kvSize := int(keySize + valueSize)
		kvBuf := make([]byte, kvSize)
		_, err = r.Read(kvBuf)
		if err != nil {
			return nil, err
		}

		rec.key = string(kvBuf[:int(keySize)])
		rec.value = string(kvBuf[int(keySize):])

		block.records = append(block.records, rec)
	}
	return block, nil
}

func (db *DB) writeBlock(block *Block) error {
	_, err := db.file.Seek(int64(block.id)*BlockSize, 0)
	if err != nil {
		return err
	}

	err = binary.Write(db.file, binary.LittleEndian, block.id)
	if err != nil {
		return err
	}

	err = binary.Write(db.file, binary.LittleEndian, block.nextBlock)
	if err != nil {
		return err
	}

	err = binary.Write(db.file, binary.LittleEndian, block.flags)
	if err != nil {
		return err
	}

	err = binary.Write(db.file, binary.LittleEndian, uint8(len(block.records)))
	if err != nil {
		return err
	}

	for _, rec := range block.records {
		err = binary.Write(db.file, binary.LittleEndian, rec.created)
		if err != nil {
			return err
		}
		err = binary.Write(db.file, binary.LittleEndian, rec.deleted)
		if err != nil {
			return err
		}
		err = binary.Write(db.file, binary.LittleEndian, rec.prev)
		if err != nil {
			return err
		}
		err = binary.Write(db.file, binary.LittleEndian, rec.next)
		if err != nil {
			return err
		}
		err = binary.Write(db.file, binary.LittleEndian, uint16(len(rec.key)))
		if err != nil {
			return err
		}
		err = binary.Write(db.file, binary.LittleEndian, uint16(len(rec.value)))
		if err != nil {
			return err
		}
		_, err = db.file.WriteString(rec.key)
		if err != nil {
			return err
		}
		_, err = db.file.WriteString(rec.value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (block *Block) size() int {
	size := BlockHeaderSize
	for _, rec := range block.records {
		size += RecordHeaderSize
		size += len(rec.key) + len(rec.value)
	}
	return size
}

func (db *DB) allocateBlock() (*Block, error) {
	if int64(db.header.lastBlock+1)*BlockSize <= db.fileSize {
		return nil, errors.New("lm2: out of space")
	}

	db.header.lastBlock++
	block := &Block{
		id: db.header.lastBlock,
	}

	err := db.writeBlock(block)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (db *DB) getNewBlock() (*Block, error) {
	freeBlockID := db.header.freeBlock
	if freeBlockID != 0 {
		block, err := db.readBlock(freeBlockID)
		if err != nil {
			return nil, err
		}

		db.header.freeBlock = block.nextBlock
		return block, nil
	}

	// No free block. Allocate another.
	return db.allocateBlock()
}

func (db *DB) Set(key, value string) error {
	currentBlock, err := db.readBlock(db.header.firstBlock)
	if err != nil {
		return err
	}

	lastLessThanBlock := currentBlock.id
	lastLessThanBlockRecordIndex := 0
	for {
		maxValidKey := ""
		for i, rec := range currentBlock.records {
			if rec.key >= maxValidKey && rec.key <= key {
				maxValidKey = rec.key
				lastLessThanBlock = currentBlock.id
				lastLessThanBlockRecordIndex = i
			}
		}
		if maxValidKey == "" {
			break
		}

		nextBlockID := currentBlock.records[lastLessThanBlockRecordIndex].next
		if nextBlockID == 0 {
			break
		}
		currentBlock, err = db.readBlock(nextBlockID)
		if err != nil {
			return err
		}
	}

	return errors.New("lm2: TODO")

	if lastLessThanBlock == 0 {
		// First key

	}

	_ = lastLessThanBlock
	_ = lastLessThanBlockRecordIndex

	return nil
}
