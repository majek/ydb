#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ydb.h"
#include "test_common.h"


char *database_path = NULL;
struct ydb *ydb = NULL;
struct ydb_batch *batch = NULL;

float gc_ratio = 4.0;
struct ydb_options opt = {16 << 20,0,0};

unsigned gc_sz = 1 << 20;

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
		if (tokc >= 1) {
			do_fsync = atoi(tokv[0]);
		}
		int r = ydb_write(ydb, batch, do_fsync);
		assert(r >= 0);
		ydb_batch_free(batch);
		batch = ydb_batch();

		while (ydb_ratio(ydb) > gc_ratio && gc_sz > 4) {
			int j = ydb_roll(ydb, gc_sz);
			if (j < 0) {
				fprintf(stderr, "gc failed\n");
				gc_sz /= 2;
				break;
			}
		}
		return 0;
	} else if (streq(action, "reopen")) {
		if (tokc >= 1) {
			opt.log_file_size_limit = atoi(tokv[0]) * 1024; // KiB
		}
		if (tokc >= 2) {
			opt.max_open_logs = atoi(tokv[1]);
		}
		if (tokc >= 3) {
			opt.index_size_limit = atoi(tokv[2]) * 1024; // KiB
		}
		ydb_close(ydb);
		ydb = ydb_open(database_path, &opt);
		assert(ydb);
		return 0;
	} else if (streq(action, "gc") && tokc == 1) {
		gc_ratio = atof(tokv[0]);
		assert(gc_ratio > 1.0);
		return 0;
	}
	return 1;
}

int main(int argc, char **argv)
{
	database_path = argv[1];
	ydb = test_ydb_open(argc, argv, opt);
	batch = ydb_batch();

	int ret = readlines(stdin, do_line);

	int r = ydb_write(ydb, batch, 0);
	assert(r >= 0);
	ydb_batch_free(batch);
	ydb_close(ydb);
	return ret;
}
