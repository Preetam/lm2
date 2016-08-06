package lm2

import (
	"fmt"
	"math/rand"
	"testing"
)

func verifyOrder(t *testing.T, c *Collection) {
	prev := ""
	cur, err := c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}
	for cur.Next() {
		if cur.Key() < prev {
			t.Errorf("key %v greater than previous key %v", cur.Key(), prev)
		}
	}
}

func Test1(t *testing.T) {
	c, err := NewCollection("/tmp/test1.lm2")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const N = 5000
	for i := 0; i < N; i++ {
		key := fmt.Sprint(rand.Intn(N * 4))
		val := fmt.Sprint(i)
		if err := c.Set(key, val); err != nil {
			t.Fatal(err)
		}
	}
	verifyOrder(t, c)
}

func Test2(t *testing.T) {
	expected := [][2]string{
		{"key1", "1"},
		{"key2", "2"},
		{"key3", "1"},
	}

	c, err := NewCollection("/tmp/test2.lm2")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err = c.Set("key1", "1")
	if err != nil {
		t.Fatal(err)
	}

	err = c.Set("key2", "1")
	if err != nil {
		t.Fatal(err)
	}

	err = c.Set("key3", "1")
	if err != nil {
		t.Fatal(err)
	}

	err = c.Set("key2", "2")
	if err != nil {
		t.Fatal(err)
	}

	err = c.Set("key4", "1")
	if err != nil {
		t.Fatal(err)
	}

	err = c.Delete("key4")
	if err != nil {
		t.Fatal(err)
	}

	verifyOrder(t, c)

	cur, err := c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for cur.Next() {
		if i == len(expected) {
			t.Fatal("unexpected key", cur.Key())
		}
		if cur.Key() != expected[i][0] || cur.Value() != expected[i][1] {
			t.Errorf("expected %v => %v, got %v => %v",
				expected[i][0], expected[i][1], cur.Key(), cur.Value())
		}
		i++
	}
}
