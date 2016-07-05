Design
===

Block size: 16 kb

Everything is organized into blocks.

0 block doesn't exist.

First block starts at 1 * 16 kb
Blocks are identified by a number.
Block num * 16 kb tells you where it's located.

	struct block
	{
	    next block
	    num_records
	}


record info:
- created (int64), 8 bytes
- deleted (int64), 8 bytes
- prev (int32), 4 bytes
- next (int32), 4 bytes
- key
- value

