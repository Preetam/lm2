#pragma once

#include <stdexcept> // std::runtime_error

#include <pthread.h> // pthread_rwlock_t

namespace lm2
{
class Block
{
public:
	Block(void* block_ptr)
	: block_ptr(block_ptr)
	{
		if (pthread_rwlock_init(&lock, nullptr)) {
			throw std::runtime_error("couldn't initialize lock");
		}
	}

	~Block()
	{
		pthread_rwlock_destroy(&lock);
	}

private:
	void* block_ptr;
	pthread_rwlock_t lock; // TODO: use locks
	int num_keys;
}; // Block
} // lm2
