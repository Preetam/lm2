package lm2

// Cursor represents a snapshot cursor.
type Cursor struct {
	collection *Collection
	current    *record
	first      bool
	snapshot   int64
}

// NewCursor returns a new cursor with a snapshot view of the
// current collection state.
func (c *Collection) NewCursor() (*Cursor, error) {
	c.metaLock.RLock()
	defer c.metaLock.RUnlock()
	if c.Head == 0 {
		return &Cursor{
			collection: c,
			current:    nil,
			first:      false,
			snapshot:   c.LastCommit,
		}, nil
	}

	head, err := c.readRecord(c.Head)
	if err != nil {
		return nil, err
	}
	return &Cursor{
		collection: c,
		current:    head,
		first:      true,
		snapshot:   c.LastCommit,
	}, nil
}

// Valid returns true if the cursor's Key() and Value()
// methods can be called. It returns false if the cursor
// isn't at a valid record position.
func (c *Cursor) Valid() bool {
	return c.current != nil
}

// Next moves the cursor to the next record. It returns true
// if it lands on a valid record.
func (c *Cursor) Next() bool {
	if !c.Valid() {
		return false
	}

	if c.first {
		c.first = false
		return true
	}

	c.current.lock.RLock()
	rec, err := c.collection.readRecord(c.current.Next)
	if err != nil {
		c.current.lock.RUnlock()
		c.current = nil
		return false
	}
	c.current.lock.RUnlock()
	c.current = rec

	c.current.lock.RLock()
	for (c.current.Deleted != 0 && c.current.Deleted <= c.snapshot) ||
		(c.current.Offset >= c.snapshot) {
		rec, err = c.collection.readRecord(c.current.Next)
		if err != nil {
			c.current.lock.RUnlock()
			c.current = nil
			return false
		}
		c.current.lock.RUnlock()
		c.current = rec
		c.current.lock.RLock()
	}
	c.current.lock.RUnlock()

	return true
}

// Key returns the key of the current record. It returns an empty
// string if the cursor is not valid.
func (c *Cursor) Key() string {
	if c.Valid() {
		return c.current.Key
	}
	return ""
}

// Value returns the value of the current record. It returns an
// empty string if the cursor is not valid.
func (c *Cursor) Value() string {
	if c.Valid() {
		return c.current.Value
	}
	return ""
}

// Seek positions the cursor at the last key less than
// or equal to the provided key.
func (c *Cursor) Seek(key string) {
	var rec *record
	var err error
	offset := c.collection.cache.findLastLessThan(key)
	if offset == 0 {
		c.collection.metaLock.RLock()
		rec, err = c.collection.readRecord(c.collection.Head)
		c.collection.metaLock.RUnlock()
		if err != nil {
			c.current = nil
			return
		}
	} else {
		rec, err = c.collection.readRecord(offset)
		if err != nil {
			c.current = nil
			return
		}
	}
	c.current = rec
	c.first = true
	for rec != nil {
		rec.lock.RLock()
		if rec.Key > key {
			rec.lock.RUnlock()
			break
		}
		if (rec.Deleted > 0 && rec.Deleted <= c.snapshot) || (rec.Offset >= c.snapshot) {
			oldRec := rec
			rec = c.collection.nextRecord(rec)
			oldRec.lock.RUnlock()
			continue
		}
		if rec.Key <= key {
			c.current = rec
		}
		oldRec := rec
		rec = c.collection.nextRecord(rec)
		oldRec.lock.RUnlock()
	}
}
