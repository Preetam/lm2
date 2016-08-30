package lm2

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func verifyOrder(t *testing.T, c *Collection) int {
	count := 0
	prev := ""
	cur, err := c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}
	for cur.Next() {
		count++
		if cur.Key() < prev {
			t.Errorf("key %v greater than previous key %v", cur.Key(), prev)
		}
	}
	return count
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

func TestCopy(t *testing.T) {
	c, err := NewCollection("/tmp/test_copy.lm2")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const N = 10000
	firstWriteStart := time.Now()
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("%019d-%019d-%019d-%019d-%019d-%019d-%019d-%019d",
			rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63(),
			rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63())
		val := fmt.Sprint(i)
		if err := c.Set(key, val); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("First write pass time:", time.Now().Sub(firstWriteStart))
	verifyOrder(t, c)

	c2, err := NewCollection("/tmp/test_copy_copy.lm2")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	cur, err := c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}

	work := make(chan [2]string, 100)
	go func() {
		for cur.Next() {
			work <- [2]string{cur.Key(), cur.Value()}
		}
		close(work)
	}()

	secondWriteStart := time.Now()
	for pair := range work {
		err = c2.Set(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Log("Second write pass time:", time.Now().Sub(secondWriteStart))

	firstStart := time.Now()
	count1 := verifyOrder(t, c)
	firstEnd := time.Now()
	secondStart := firstEnd
	count2 := verifyOrder(t, c2)
	secondEnd := time.Now()
	t.Log("Time to iterate through first list:", firstEnd.Sub(firstStart), "with", count1, "elements")
	t.Log("Time to iterate through second list:", secondEnd.Sub(secondStart), "with", count2, "elements")

	if count1 != count2 || count1 != N {
		t.Errorf("incorrect count. N = %d, count1 = %d, count2 = %d", N, count1, count2)
	}
}

func TestWriteBatch(t *testing.T) {
	expected := [][2]string{
		{"key1", "1"},
		{"key2", "2"},
		{"key3", "3"},
		{"key4", "4"},
	}

	c, err := NewCollection("/tmp/test_writebatch.lm2")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wb := NewWriteBatch()
	wb.Set("key1", "1")
	wb.Set("key2", "2")
	wb.Set("key3", "3")
	wb.Set("key4", "4")

	err = c.Update(wb)
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

	expected = [][2]string{
		{"key1", "5"},
		{"key2", "6"},
		{"key3", "7"},
		{"key4", "8"},
	}

	wb = NewWriteBatch()
	wb.Set("key1", "5")
	wb.Set("key2", "6")
	wb.Set("key3", "7")
	wb.Set("key4", "8")

	err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	verifyOrder(t, c)

	cur, err = c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}

	i = 0
	for cur.Next() {
		if cur.current.Deleted > 0 {
			continue
		}
		t.Log(cur.Key(), "=>", cur.Value())
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

func TestWriteBatch2(t *testing.T) {
	expected := [][2]string{
		{"key1", "1"},
		{"key2", "2"},
		{"key3", "1"},
	}

	c, err := NewCollection("/tmp/test_writebatch2.lm2")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wb := NewWriteBatch()

	wb.Set("key1", "1")
	err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key2", "1")
	err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key3", "1")
	err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key2", "2")
	err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key4", "1")
	err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Delete("key4")
	err = c.Update(wb)
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
