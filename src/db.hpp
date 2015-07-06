#pragma once

#include <string> // std::string

#include <stdio.h>   // FILE
#include <pthread.h> // pthread_rwlock_t

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
	size_t file_size;
}; // DB
} // lm2
