package lm2

import "testing"

func Test1(t *testing.T) {
	db, err := NewDB("/tmp/test.lm2")
	if err != nil {
		t.Fatal(err)
	}
	block, err := db.readBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	block.records = append(block.records, Record{
		created: 1,
		deleted: 0,
		prev:    0,
		next:    0,
		key:     "foo",
		value:   "bar",
	})
	t.Log(block)
	t.Log(db.writeBlock(block))
	t.Log(db.readBlock(1))
	db.Close()
}
