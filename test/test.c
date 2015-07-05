#include <listmap2.h>

int
main() {
	lm2_db_t* db = lm2_db_open("/tmp/listmap2/test");
	lm2_db_close(db);
	return 0;
}
