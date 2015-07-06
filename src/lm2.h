// lm2.h 
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lm2_db_t lm2_db_t;

// lm2_db_open creates a handle to an lm2 database
// located at path.
lm2_db_t*
lm2_db_open(const char* path);

// lm2_db_close closes the database represented by an lm2_db_t*.
void
lm2_db_close(lm2_db_t* db);

#ifdef __cplusplus
} // extern
#endif
