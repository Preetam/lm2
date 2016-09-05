lm2 [![CircleCI](https://circleci.com/gh/Preetam/lm2.svg?style=svg&circle-token=bcee812ab7d8cd6d1fc8582644214fd06201908b)](https://circleci.com/gh/Preetam/lm2) [![GoDoc](https://godoc.org/github.com/Preetam/lm2?status.svg)](https://godoc.org/github.com/Preetam/lm2)
===
lm2 (listmap2) is an ordered key-value storage library.

It provides

* Ordered key-value data model
* Append-only modifications
* Fully durable, atomic writes
* Cursors with snapshot reads

Because it is append-only, records are never actually deleted.
You will have to rewrite a collection to reclaim space.

License
---
BSD (see LICENSE)
