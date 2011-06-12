#include <assert.h>
#include <fnmatch.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <unistd.h>

#include "stddev.h"
#include "bitmap.h"

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_logs.h"
#include "ydb_log.h"
#include "ydb_writer.h"
#include "ydb_state.h"
#include "ydb_itree.h"
#include "ydb_record.h"
#include "ydb_hashdir.h"
#include "ydb_batch.h"
#include "ydb_db.h"

#include "ydb.h"
#include "ydb_base.h"

struct ydb {
	struct db *db;
	struct base *base;
	struct dir *log_dir;
	struct dir *index_dir;
};


struct ydb *ydb_open(const char *directory, struct ydb_options *options)
{
	struct db *db = db_new(directory);
	if (db == NULL) {
		return NULL;
	}

	struct base *base = base_new(db, db_log_dir(db), db_index_dir(db),
				     options);
	if (base == NULL) {
		db_free(db);
		return NULL;
	}
	log_info(db, "Opening YDB database \"%s\" by pid=%i.", directory, getpid());

	struct ydb *ydb = malloc(sizeof(struct ydb));
	ydb->db = db;
	ydb->base = base;

	int r = base_load(ydb->base);
	if (r != 0) {
		ydb_close(ydb);
		return NULL;
	}
	base_print_stats(ydb->base);
	return ydb;
}

void ydb_close(struct ydb *ydb)
{
	base_print_stats(ydb->base);
	log_info(ydb->db, "Closing YDB database. %s", "");
	base_free(ydb->base);
	db_free(ydb->db);
	free(ydb);
}

float ydb_ratio(struct ydb *ydb)
{
	return base_ratio(ydb->base);
}

void ydb_prefetch(struct ydb *ydb,
		  struct ydb_vec *keysv, unsigned keysv_cnt)
{
	base_prefetch(ydb->base, keysv, keysv_cnt);
}

int ydb_get(struct ydb *ydb,
	    const char *key, unsigned key_sz,
	    char *buf, unsigned buf_sz)
{
	return base_get(ydb->base, key, key_sz, buf, buf_sz);
}


struct ydb_batch;

struct ydb_batch *ydb_batch()
{
	return (struct ydb_batch*)batch_new();
}

void ydb_batch_free(struct ydb_batch *ybatch)
{
	struct batch *batch = (struct batch *)ybatch;
	batch_free(batch);
}

void ydb_set(struct ydb_batch *ybatch,
	     const char *key, unsigned key_sz,
	     const char *value, unsigned value_sz)
{
	struct batch *batch = (struct batch *)ybatch;
	batch_set(batch, (char*)key, key_sz, (char*)value, value_sz);
}

void ydb_del(struct ydb_batch *ybatch,
	     const char *key, unsigned key_sz)
{
	struct batch *batch = (struct batch *)ybatch;
	batch_del(batch, (char*)key, key_sz);
}

int ydb_write(struct ydb *ydb, struct ydb_batch *ybatch, int do_fsync)
{
	struct batch *batch = (struct batch *)ybatch;
	return base_write(ydb->base, batch, do_fsync);
}


int ydb_iterate(struct ydb *ydb, unsigned prefetch_size,
		ydb_iter_callback callback, void *userdata)
{
	return base_iterate(ydb->base, prefetch_size, callback, userdata);
}


int ydb_roll(struct ydb *ydb, unsigned gc_size)
{
	return base_gc(ydb->base, gc_size);
}
