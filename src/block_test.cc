#include "catch.hpp"

#include <memory>
#include <iostream>

#include "block.hpp"

TEST_CASE("tests that a valid block is initialized", "[block]") {
	uint8_t block_data[32768];
	REQUIRE(block_data != nullptr);

	auto a = std::make_shared<lm2::Block>(1, block_data);
	REQUIRE(a->get_id() == 1);

	auto b = std::make_shared<lm2::Block>(2, block_data);
	REQUIRE(b->get_id() == 2);

	b->set_prev(a);
	a->set_next(b);

	std::shared_ptr<lm2::Block> cur;
	cur = a;
	REQUIRE(cur->get_id() == 1);

	cur = cur->get_next();
	REQUIRE(cur->get_id() == 2);

	cur = cur->get_next();
	REQUIRE(cur == nullptr);
}
