#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lm2_db_t lm2_db_t;

lm2_db_t*
lm2_db_open(const char*);

void
lm2_db_close(lm2_db_t*);

#ifdef __cplusplus
} // extern
#endif
