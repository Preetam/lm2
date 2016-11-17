package lm2

import (
	"fmt"
	"math/rand"
	"sync"
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

func TestCopy(t *testing.T) {
	c, err := NewCollection("/tmp/test_copy.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	const N = 1000
	firstWriteStart := time.Now()
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("%019d-%019d-%019d-%019d-%019d-%019d-%019d-%019d",
			rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63(),
			rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63())
		val := fmt.Sprint(i)
		wb := NewWriteBatch()
		wb.Set(key, val)
		if _, err := c.Update(wb); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("First write pass time:", time.Now().Sub(firstWriteStart))
	verifyOrder(t, c)

	c2, err := NewCollection("/tmp/test_copy_copy.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Destroy()

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
	const batchSize = 100
	remaining := batchSize
	wb := NewWriteBatch()
	for pair := range work {
		wb.Set(pair[0], pair[1])
		remaining--

		if remaining == 0 {
			_, err := c2.Update(wb)
			if err != nil {
				t.Fatal(err)
			}
			remaining = batchSize
			wb = NewWriteBatch()
		}
	}

	if remaining < batchSize {
		_, err := c2.Update(wb)
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
	t.Logf("%+v", c.Stats())
	t.Logf("%+v", c2.Stats())
}

func TestWriteBatch(t *testing.T) {
	expected := [][2]string{
		{"key1", "1"},
		{"key2", "2"},
		{"key3", "3"},
		{"key4", "4"},
	}

	c, err := NewCollection("/tmp/test_writebatch.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	wb := NewWriteBatch()
	wb.Set("key1", "1")
	wb.Set("key2", "2")
	wb.Set("key3", "3")
	wb.Set("key4", "4")

	_, err = c.Update(wb)
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

	_, err = c.Update(wb)
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
		if i == len(expected) {
			t.Fatal("unexpected key", cur.Key())
		}
		if cur.Key() != expected[i][0] || cur.Value() != expected[i][1] {
			t.Errorf("expected %v => %v, got %v => %v",
				expected[i][0], expected[i][1], cur.Key(), cur.Value())
			t.Logf("%+#v", cur.current)
		}
		i++
	}

	// Check if cursor can be reset
	cur.Seek("")
	i = 0
	for cur.Next() {
		if cur.current.Deleted > 0 {
			continue
		}
		if i == len(expected) {
			t.Fatal("unexpected key", cur.Key())
		}
		if cur.Key() != expected[i][0] || cur.Value() != expected[i][1] {
			t.Errorf("expected %v => %v, got %v => %v",
				expected[i][0], expected[i][1], cur.Key(), cur.Value())
			t.Logf("%+#v", cur.current)
		}
		i++
	}
}

func TestWriteBatch1(t *testing.T) {
	c, err := NewCollection("/tmp/test_writebatch1.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	const N = 500
	for i := 0; i < N; i++ {
		wb := NewWriteBatch()
		key := fmt.Sprint(rand.Intn(N * 4))
		val := fmt.Sprint(i)
		wb.Set(key, val)
		if _, err := c.Update(wb); err != nil {
			t.Fatal(err)
		}
	}
	verifyOrder(t, c)
}

func TestWriteBatch1Concurrent(t *testing.T) {
	c, err := NewCollection("/tmp/test_writebatch1concurrent.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	const N = 50
	const NumGoroutines = 8

	startWG := sync.WaitGroup{}
	endWG := sync.WaitGroup{}
	startWG.Add(NumGoroutines)
	endWG.Add(NumGoroutines)

	for i := 0; i < NumGoroutines; i++ {
		go func() {
			for j := 0; j < N; j++ {
				wb := NewWriteBatch()
				key := fmt.Sprint(rand.Intn(N * 4))
				val := fmt.Sprint(j)
				wb.Set(key, val)
				if _, err := c.Update(wb); err != nil {
					t.Fatal(err)
				}
				if j == 0 {
					startWG.Done()
				}
			}
			endWG.Done()
		}()
	}

	// Wait for them to start working.
	startWG.Wait()

	verifyOrder(t, c)
	verifyOrder(t, c)

	// Wait for them to end.
	endWG.Wait()

	verifyOrder(t, c)
}

func TestWriteBatch2(t *testing.T) {
	expected := [][2]string{
		{"key1", "1"},
		{"key2", "2"},
		{"key3", "1"},
	}

	c, err := NewCollection("/tmp/test_writebatch2.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	wb := NewWriteBatch()

	wb.Set("key1", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key2", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key3", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key2", "2")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key4", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Delete("key4")
	_, err = c.Update(wb)
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
	t.Logf("%+v", c.Stats())
}

func TestWriteCloseOpen(t *testing.T) {
	expected := [][2]string{
		{"key1", "1"},
		{"key2", "2"},
		{"key3", "1"},
	}

	c, err := NewCollection("/tmp/test_writecloseopen.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}

	wb := NewWriteBatch()
	wb.Set("key1", "1")
	wb.Set("key2", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key3", "1")
	wb.Set("key2", "2")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("key4", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Delete("key4")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	c.Close()

	c, err = OpenCollection("/tmp/test_writecloseopen.lm2", 100)
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
	t.Logf("%+v", c.Stats())

	err = c.Destroy()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadLastEntry(t *testing.T) {
	c, err := NewCollection("/tmp/test_readlastentry.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}

	wb := NewWriteBatch()
	wb.Set("key1", "1")
	wb.Set("key2", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	wal, err := openWAL("/tmp/test_readlastentry.lm2.wal")
	if err != nil {
		t.Fatal(err)
	}

	entry, err := wal.ReadLastEntry()
	if err != nil {
		t.Fatal(err)
	}

	if entry.NumRecords != 2 {
		t.Errorf("expected %d records, got %d", 2, entry.NumRecords)
	}
	t.Logf("%+v", c.Stats())

	c.Destroy()
}

func TestSeekToFirstKey(t *testing.T) {
	c, err := NewCollection("/tmp/test_seektofirstkey.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	wb := NewWriteBatch()
	wb.Set("a", "1")
	wb.Set("b", "1")
	wb.Set("c", "1")
	wb.Set("d", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	cur, err := c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}

	cur.Seek("a")
	if !cur.Valid() {
		t.Fatal("expected cursor to be valid")
	}

	if !cur.Next() {
		t.Fatal("expected Next() to return true")
	}

	if cur.Key() != "a" {
		t.Fatalf("expected cursor key to be 'a', got %v", cur.Key())
	}
}

func TestOverwriteFirstKey(t *testing.T) {
	c, err := NewCollection("/tmp/test_overwritefirstkey.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	wb := NewWriteBatch()
	wb.Set("a", "1")
	wb.Set("b", "1")
	wb.Set("c", "1")
	wb.Set("d", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("a", "2")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	cur, err := c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}

	cur.Seek("a")
	if !cur.Valid() {
		t.Fatal("expected cursor to be valid")
	}

	if !cur.Next() {
		t.Fatal("expected Next() to return true")
	}

	if cur.Key() != "a" {
		t.Fatalf("expected cursor key to be 'a', got %v", cur.Key())
	}

	if !cur.Next() {
		t.Fatal("expected Next() to return true")
	}

	if cur.Key() != "b" {
		t.Fatalf("expected cursor key to be 'b', got %v", cur.Key())
	}
}

func TestOverwriteFirstKeyOnly(t *testing.T) {
	c, err := NewCollection("/tmp/test_overwritefirstkeyonly.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	wb := NewWriteBatch()
	wb.Set("a", "1")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	wb = NewWriteBatch()
	wb.Set("a", "2")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}

	cur, err := c.NewCursor()
	if err != nil {
		t.Fatal(err)
	}

	cur.Seek("")
	if !cur.Valid() {
		t.Fatal("expected cursor to be valid")
	}

	if !cur.Next() {
		t.Fatal("expected Next() to return true")
	}

	if cur.Key() != "a" {
		t.Fatalf("expected cursor key to be 'a', got %v", cur.Key())
	}
	t.Log(cur.Key(), "=>", cur.Value())

	if cur.Next() {
		t.Error("expected Next() to return false")
		t.Log(cur.Key(), "=>", cur.Value())
	}
}

func TestDeleteInFirstUpdate(t *testing.T) {
	c, err := NewCollection("/tmp/test_deleteinfirstupdate.lm2", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Destroy()

	wb := NewWriteBatch()
	wb.Delete("a")
	_, err = c.Update(wb)
	if err != nil {
		t.Fatal(err)
	}
}
