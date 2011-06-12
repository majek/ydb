#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ydb.h"
#include "test_common.h"


struct ydb *ydb = NULL;
struct ydb_batch *batch = NULL;

int do_line(char *action, int tokc, char **tokv)
{
	if (streq(action, "set") && tokc == 2) {
		ydb_set(batch,
			(const char*)tokv[0], strlen(tokv[0]),
			(const char*)tokv[1], strlen(tokv[1]));
		return 0;
	} else if (streq(action, "del") && tokc == 1) {
		ydb_del(batch, tokv[0], strlen(tokv[0]));
		return 0;
	} else if (streq(action, "write")) {
		int do_fsync = 0;
		if (tokc == 1) {
			do_fsync = atoi(tokv[0]);
		}
		int r = ydb_write(ydb, batch, do_fsync);
		assert(r >= 0);
		ydb_batch_free(batch);
		batch = ydb_batch();

		while (ydb_ratio(ydb) > 4.0) {
			int j = ydb_roll(ydb, 1 << 20);
			assert(j >= 0);
		}
		return 0;
	}
	return 1;
}

int main(int argc, char **argv)
{
	ydb = test_ydb_open(argc, argv,
			    (struct ydb_options){0,0,0});
	batch = ydb_batch();

	int ret = readlines(stdin, do_line);

	int r = ydb_write(ydb, batch, 0);
	assert(r >= 0);
	ydb_batch_free(batch);
	ydb_close(ydb);
	return ret;
}
