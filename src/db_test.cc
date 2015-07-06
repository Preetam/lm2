#include "catch.hpp"

#include <lm2.h>

TEST_CASE("lm2::DB handle is created", "[lm2::DB]") {
	lm2_db_t* db = lm2_db_open("/tmp/lm2/test");
	REQUIRE(db != nullptr);
	lm2_db_close(db);
}
