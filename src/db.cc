#include <stdio.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <string>
#include <iostream>

#include "db.hpp"

lm2 :: DB :: DB(const char* path)
: path(path) {
	file = fopen(path, "r");
	if (!file) {
		throw "invalid path";
	}

	struct stat st;
	if (fstat(fileno(file), &st)) {
		fclose(file);
		throw "couldn't get file stats";
	}

	if (!S_ISDIR(st.st_mode)) {
		fclose(file);
		throw "not a directory";
	}

	file_size = st.st_size;

	mapped = mmap(nullptr, (size_t)file_size, PROT_READ|PROT_WRITE,
		MAP_SHARED |
		#ifdef __APPLE__
		MAP_ANON
		#else
		MAP_ANONYMOUS
		#endif
		, -1, 0);
	if ((int64_t)mapped == -1) {
		fclose(file);
		throw "couldn't mmap";
	}

	pthread_rwlock_init(&lock, NULL);
}

lm2 :: DB :: ~DB() throw() {
	std::cout << "closing lm2::DB at " << path << std::endl;
	munmap(mapped, file_size);
	fclose(file);
	pthread_rwlock_destroy(&lock);
}
