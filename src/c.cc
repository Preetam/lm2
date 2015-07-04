#include "listmap2.h"
#include "db.hpp"

#include <iostream>

int listmap2_init() {
	lm2::DB db();
	std::cout << "initialized listmap2" << std::endl;
	return 0;
}
