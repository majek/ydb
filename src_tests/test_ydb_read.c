#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ydb.h"
#include "test_common.h"

int callback(void *ud,
	     const char *key, unsigned key_sz,
	     const char *value, unsigned value_sz)
{
	ud = ud;
	printf("hash=%s key=%.*s value=%.*s\n",
	       md5_str(key, key_sz),
	       key_sz, key,
	       value_sz, value);
	return 0;
}


int main(int argc, char **argv)
{
	struct ydb *ydb = test_ydb_open(argc, argv,
					(struct ydb_options){4 << 20,0,0});

	/* int i; */
	/* for (i=0; i< 3; i++) { */
	/* 	printf("ratio=%.3f\n", ydb_ratio(ydb)); */
	/* 	int r = ydb_roll(ydb, 128 << 10); */
	/* 	assert(r >= 0); */
	/* } */
	ydb_iterate(ydb, 512 << 10, callback, NULL);

	ydb_close(ydb);
	return 0;
}
