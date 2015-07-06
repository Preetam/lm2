// c.cc provides the definitions required for the C API.
#include <iostream>  // std::cout
#include <stdexcept> // std::exception

#include "lm2.h"
#include "db.hpp"

struct lm2_db_t { lm2::DB* rep; };

lm2_db_t*
lm2_db_open(const char* path) {
	auto result = new lm2_db_t;
	try {
		auto db = new lm2::DB(path);
		result->rep = db;
		std::cout << "opened lm2::DB at " << path << std::endl;
	} catch (std::exception& e) {
		delete result;
		result = nullptr;
		std::cout << "error: " << e.what() << std::endl;
	}
	return result;
}

void
lm2_db_close(lm2_db_t* db) {
	if (!db) {
		return;
	}
	delete db->rep;
	delete db;
}
