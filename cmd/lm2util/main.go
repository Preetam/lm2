package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Preetam/lm2"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
	filename := flag.String("file", "", "data file to open")
	cmd := flag.String("cmd", "", "command to run")
	key := flag.String("key", "", "key to get or set")
	value := flag.String("value", "", "value of key to set")
	endKey := flag.String("end-key", "", "end range of scan")
	flag.Parse()

	if *cmd == "create" {
		c, err := lm2.NewCollection(*filename)
		if err != nil {
			log.Fatal(err)
		}
		c.Close()
		return
	}

	c, err := lm2.OpenCollection(*filename)
	if err != nil {
		log.Fatal(err)
	}

	switch *cmd {
	case "get":
		cur, err := c.NewCursor()
		if err != nil {
			log.Fatal(err)
		}
		cur.Seek(*key)
		if cur.Valid() {
			if cur.Key() == *key {
				fmt.Println(cur.Key(), "=>", cur.Value())
				return
			}
		}
		log.Fatal("key not found")
	case "scan":
		cur, err := c.NewCursor()
		if err != nil {
			log.Fatal(err)
		}
		cur.Seek(*key)
		for cur.Next() {
			if cur.Key() > *endKey {
				break
			}
			fmt.Println(cur.Key(), "=>", cur.Value())
		}
	case "set":
		err = c.Set(*key, *value)
		if err != nil {
			log.Fatal(err)
		}
	case "delete"
		err = c.Delete(*key)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("unknown command", *cmd)
	}
}
