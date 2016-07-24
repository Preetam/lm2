Design
===

Block size: 16 kb

Everything is organized into blocks.

0 block doesn't exist. Its space is reserved for
file headers.

First block starts at `1 * 16 kb`.
Blocks are identified by a number.
`Block num * 16 kb` tells you where it's located.

```go
type block struct {
	id uint32
	// Potential flags
	// - Is free
	// - Compressed
	// - Keys first
	// - Is overflow
	flags uint8
	nextBlock uint32 // blockID

	numRecords uint8 // max 255 records
	[]records
}
```

`nextBlock` is used for the next free block.

```
record info:
- created (int64), 8 bytes
- deleted (int64), 8 bytes
- prev (int32), 4 bytes // block ID
- next (int32), 4 bytes // block ID
- key size (uint16), 2 bytes
- value size (uint16), 2 bytes
- key
- value
```

28 bytes of overhead per k-v pair.

The combined key+value size must be less than 16,000 bytes.

---

**File header**

- Last allocated block ID
- Current block accepting writes
- Free list block start
- Last committed transaction ID
