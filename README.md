listmap2
===
listmap2 is an ordered key-value storage library written in C++ (with a C interface).

It is still a work in progress.

Design
---

I'm currently taking a brute-force approach to this library. I'm starting with a linked list
and building up from there. That said, it's quite different from a traditional linked list.

Some interesting links:

* [Unrolled linked list][unrolled linked list]
* [Skip list][skip list]
* [listmap][listmap]
* [vlmap][vlmap]

[unrolled linked list]: https://en.wikipedia.org/wiki/Unrolled_linked_list
[skip list]: https://en.wikipedia.org/wiki/Skip_list
[listmap]: https://github.com/Preetam/listmap
[vlmap]: https://github.com/Preetam/vlmap


License
---
BSD (see LICENSE)
