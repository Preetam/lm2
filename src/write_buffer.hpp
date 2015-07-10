#pragma once

#include <string>    // std::string
#include <stdexcept> // std::invalid_argument

#include <pthread.h> // pthread_rwlock_t

namespace lm2
{

enum OPERATION {
	typeSet,
	typeDelete
};

struct BufferOperation
{
	uint8_t     type;
	std::string key;
	std::string value;

	BufferOperation(uint8_t type, std::string key)
	: type(type), key(key) {
		if (type != typeDelete) {
			throw std::invalid_argument("expected value for non-delete operation");
		}
	}

	BufferOperation(uint8_t type, std::string key, std::string value)
	: type(type), key(key), value(value)
	{
	}
};

class WriteBuffer
{
	friend class Buffer;

public:
	WriteBuffer(void* data_buffer, ssize_t data_buffer_len)
	: data_buffer(data_buffer), data_buffer_len(data_buffer_len)
	{
		if (pthread_rwlock_init(&lock, nullptr)) {
			throw std::runtime_error("couldn't initialize lock");
		}
	}

	~WriteBuffer()
	{
		pthread_rwlock_destroy(&lock);
	}

	int append(BufferOperation);

private:
	pthread_rwlock_t lock;

	void* data_buffer;
	int current_offset;
	ssize_t data_buffer_len;

}; // WriteBuffer
} // lm2
