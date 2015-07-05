#pragma once

#include <stdio.h>
#include <pthread.h>

#include <string>

namespace lm2
{
class DB
{
public:
	DB(const char* path);
	~DB() throw();

private:
	std::string path;
	void* mapped;
	pthread_rwlock_t lock;
	FILE* file;
	uint file_size;
}; // DB
} // lm2
