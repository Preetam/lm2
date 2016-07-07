Design
===

Block size: 16 kb

Everything is organized into blocks.

0 block doesn't exist. Its space is reserved for
file headers.

First block starts at `1 * 16 kb`.
Blocks are identified by a number.
`Block num * 16 kb` tells you where it's located.

```
struct block
{
	flags (compressed, keys first, etc)
    next_block
}
```

`next_block` is used for the next free block or overflow
blocks.

```
record info:
- created (int64), 8 bytes
- deleted (int64), 8 bytes
- prev (int32), 4 bytes // block ID
- next (int32), 4 bytes // block ID
- key size (uint16), 2 bytes
- value size (uint16), 4 bytes
- key
- value
```

30 bytes of overhead per k-v pair.

---

**File header**

- Last block ID
- Free list block start
- Data block start
- Last committed transaction ID
