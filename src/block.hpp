#pragma once

#include <stdexcept> // std::runtime_error
#include <memory>    // std::shared_ptr

#include <pthread.h> // pthread_rwlock_t

namespace lm2
{
class Block
{
public:
	Block(uint64_t id, void* data_ptr)
	: id(id), data_ptr(data_ptr)
	{
		if (pthread_rwlock_init(&lock, nullptr)) {
			throw std::runtime_error("couldn't initialize lock");
		}
	}

	~Block()
	{
		pthread_rwlock_destroy(&lock);
	}

	void set_next(std::shared_ptr<Block> new_next) {
		next = new_next;
	}

	void set_prev(std::shared_ptr<Block> new_prev) {
		prev = new_prev;
	}

	uint64_t get_id() {
		return id;
	}

	std::shared_ptr<Block> get_next() {
		return next;
	}

	std::shared_ptr<Block> get_prev() {
		return prev;
	}

	void* data() {
		return data_ptr;
	}

private:
	pthread_rwlock_t lock; // TODO: use locks

	uint64_t id;
	void* data_ptr;
	std::shared_ptr<Block> next;
	std::shared_ptr<Block> prev;
	int num_keys;
}; // Block
} // lm2
