package lm2

type Cursor struct {
	collection *Collection
	current    *record
	first      bool
}

func (c *Collection) NewCursor() (*Cursor, error) {
	head, err := c.readRecord(c.Head)
	if err != nil {
		return nil, err
	}
	return &Cursor{
		collection: c,
		current:    head,
		first:      false,
	}, nil
}

func (c *Cursor) Valid() bool {
	return c.current != nil
}

func (c *Cursor) Next() bool {
	if !c.Valid() {
		return false
	}

	if c.first {
		return true
	}

	rec, err := c.collection.readRecord(c.current.Next)
	if err != nil {
		c.current = nil
		return false
	}

	c.current = rec
	return true
}

func (c *Cursor) Key() string {
	return c.current.Key
}

func (c *Cursor) Value() string {
	return c.current.Value
}

func (c *Cursor) Seek(key string) {
	offset := c.collection.cache.findLastLessThan(key)
	if offset == 0 {
		head, err := c.collection.readRecord(c.collection.Head)
		if err != nil {
			c.current = nil
			return
		}
		c.current = head
		c.first = true
		return
	}
	rec, err := c.collection.readRecord(offset)
	if err != nil {
		c.current = nil
		return
	}
	c.current = rec
	c.first = true
	for ; rec != nil; rec = c.collection.nextRecord(rec) {
		if rec.Key > key {
			break
		}
		if rec.Deleted > 0 {
			continue
		}
		if rec.Key <= key {
			c.current = rec
		}
	}
}
