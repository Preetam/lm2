# Sources
BUILD_SOURCES := ${shell find ./src -name *.cc -not -name *_test.cc}
TEST_SOURCES := ${shell find ./src -name *_test.cc}

# Includes
INCLUDE_DIRS := ./src
INCLUDE_DIRS_FLAGS := $(foreach d, $(INCLUDE_DIRS), -I$d)

# Build output
BUILD_DIR := ./build
BUILD_FLAGS = -std=c++14 -Wall -shared -fPIC
BUILD_LINK_FLAGS = -lpthread
BUILD_BINARY = liblm2.so

# Test output
TEST_DIR := ./test
TEST_FLAGS = -std=c++14 -Wall
TEST_LINK_FLAGS = -llm2 -lpthread
TEST_BINARY = test
TEST_DATA_DIR = /tmp/lm2/test/

all: build build_test

clean:
	rm -r ./build
	rm $(TEST_DIR)/$(TEST_BINARY)
	rm -rf $(TEST_DATA_DIR)

build:
	mkdir -p ./build/
	$(CXX) $(BUILD_FLAGS) $(BUILD_SOURCES) \
		$(INCLUDE_DIRS_FLAGS) $(BUILD_LINK_FLAGS) -o $(BUILD_DIR)/$(BUILD_BINARY)

build_test:
	$(CXX) $(TEST_FLAGS) $(TEST_SOURCES) $(INCLUDE_DIRS_FLAGS) -L$(BUILD_DIR) $(TEST_LINK_FLAGS) \
		-o $(TEST_DIR)/$(TEST_BINARY)

test:
	mkdir -p $(TEST_DATA_DIR)
	LD_LIBRARY_PATH=$(BUILD_DIR)/:$(LD_LIBRARY_PATH) $(TEST_DIR)/$(TEST_BINARY)

test_leaks:
	mkdir -p $(TEST_DATA_DIR)
	LD_LIBRARY_PATH=$(BUILD_DIR)/:$(LD_LIBRARY_PATH) valgrind $(TEST_DIR)/$(TEST_BINARY)

.PHONY: clean build build_test test test_leaks
