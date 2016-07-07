package lm2

import "testing"

func Test1(t *testing.T) {
	db, err := NewDB("/tmp/test.lm2")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}
