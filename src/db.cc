#include <iostream> // std::cout
#include <stdexcept> // std::runtime_error

#include <sys/stat.h> // fstat
#include <sys/mman.h> // mmap

#include "db.hpp"     // lm2::DB
#include "platform.h" // MAP_ANONYMOUS

lm2 :: DB :: DB(const char* path)
: path(path) {
	file = fopen(path, "r");
	if (!file) {
		throw std::runtime_error("invalid path");
	}

	struct stat st;
	if (fstat(fileno(file), &st)) {
		fclose(file);
		throw std::runtime_error("couldn't get file stats");
	}

	if (!S_ISDIR(st.st_mode)) {
		fclose(file);
		throw std::runtime_error("not a directory");
	}

	file_size = st.st_size;

	mapped = mmap(nullptr, (size_t)file_size, PROT_READ|PROT_WRITE,
		MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if ((int64_t)mapped == -1) {
		fclose(file);
		throw std::runtime_error("couldn't mmap");
	}

	pthread_rwlock_init(&lock, nullptr);
}

lm2 :: DB :: ~DB() throw() {
	std::cout << "closing lm2::DB at " << path << std::endl;
	if (mapped) {
		munmap(mapped, file_size);
	}
	if (file) {
		fclose(file);
	}
	pthread_rwlock_destroy(&lock);
}
