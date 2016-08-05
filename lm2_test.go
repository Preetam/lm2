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
	c, err := NewCollection("/tmp/test.lm2")
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
