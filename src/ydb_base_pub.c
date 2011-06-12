#include <assert.h>
#include <fnmatch.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <sys/time.h>

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

#include "ydb.h"
#include "ydb_base.h"


void base_prefetch(struct base *base, struct ydb_vec *keysv, unsigned keysv_cnt)
{
	unsigned i;
	for (i=0; i < keysv_cnt; i++) {
		struct ydb_vec *vec = &keysv[i];
		uint128_t key_hash = md5(vec->key, vec->key_sz);

		uint64_t log_remno;
		int hpos;
		int r = itree_get2(base->itree, key_hash, &log_remno, &hpos);
		if (r < 0) {
			vec->value_sz = -1;
		} else {
			struct log *log = log_by_remno(base->logs, log_remno);
			vec->value_sz = log_prefetch(log, hpos);
		}
	}
}

int base_get(struct base *base,
	     const char *key, unsigned key_sz,
	     char *buf, unsigned buf_sz)
{
	uint128_t key_hash = md5(key, key_sz);

	uint64_t log_remno;
	int hpos;
	int r = itree_get2(base->itree, key_hash, &log_remno, &hpos);
	if (r < 0) {
		return -1;
	}
	struct log *log = log_by_remno(base->logs, log_remno);
	/* TODO: get rid of the awful malloc */
	unsigned data_sz = log_buffer_size(log, hpos);
	char *data = malloc(data_sz);
	struct keyvalue kv;
	r = log_read(log, hpos, data, data_sz, &kv);
	if (r < 0) {
		free(data);
		return -2;
	}
	if (kv.value_sz > buf_sz) {
		free(data);
		return -3;
	}
	if (key_sz != kv.key_sz || memcmp(key, kv.key, key_sz) != 0) {
		log_error(base->db, "Congratulations! You just found a "
			  "collision! Apparently key %*s has the same md5 hash as %*s!",
			  key_sz, key,
			  kv.key_sz, kv.key);
		free(data);
		return -1;
	}
	memcpy(buf, kv.value, kv.value_sz);
	free(data);
	return kv.value_sz;
}


void base_write_callback(void *base_p, uint32_t magic,
			 const char *key, unsigned key_sz,
			 uint64_t offset, uint64_t size)
{
	struct base *base = (struct base *)base_p;
	uint128_t key_hash = md5(key, key_sz);
	if (magic == YDB_LOG_SET) {
		itree_add(base->itree,
			  (struct hashdir_item){key_hash, offset, size, 0});
	} else {
		itree_del(base->itree, key_hash);
	}
}

int base_write(struct base *base, struct batch *batch, int do_fsync)
{
	int do_snapshot = 0;
	if (batch_size(batch) > base->log_file_size_limit ||
	    batch_sets(batch) >= base->index_slots_limit) {
		log_error(base->db, "Sorry, unable to write so big batch. %s",
			  "");
		return -3;
	}
	if (base->writer == NULL) {
		return -2;
	}
	if (writer_filesize(base->writer) + batch_size(batch) > base->log_file_size_limit ||
	    log_sets_count(logs_newest(base->logs)) + batch_sets(batch) >= base->index_slots_limit) {
		/* Roll on to new log */
		int r = base_roll(base);
		if (r < 0) {
			return -2;
		}
		do_snapshot = 1;
	}
	assert(base->writer);

	int r = batch_write(batch, base->writer, base_write_callback, base);
	if (r < 0) {
		return -2;
	}
	/* square will go out, but at least sum and counter will match */
	stddev_modify(&base->disk_size, 0, r);

	if (do_fsync) {
		writer_sync(base->writer);
	}

	if (base_maybe_free_oldest(base)) {
		do_snapshot = 1;
	}
	if (do_snapshot) {
		int j = base_schedule_snapshot(base);
		if (j < 0) {
			log_warn(base->db, "Unable to save snapshot. %s", "");
		}
	}
	return r;
}

float base_ratio(struct base *base)
{
	return (float)base->disk_size.sum / (float)base->used_size.sum;
}

struct _gc_ctx {
	struct base *base;
	struct batch *batch;
	int count;
	uint64_t written;
};

static int _base_gc_callback(void *ctx_p, const char *key, unsigned key_sz,
			     const char *value, unsigned value_sz)
{
	struct _gc_ctx *ctx = (struct _gc_ctx *)ctx_p;
	batch_set(ctx->batch, key, key_sz, value, value_sz);
	ctx->count -= 1;
	if (ctx->count > 0) {
		int r = base_write(ctx->base, ctx->batch, 0);
		batch_free(ctx->batch);
		ctx->batch = batch_new();
		ctx->count = 1024;
		if (r >= 0) {
			ctx->written += r;
			return 0;
		} else {
			return r;
		}
	}
	return 1;
}

int base_gc(struct base *base, unsigned gc_size)
{
	struct timeval tv0, tv1;

	gettimeofday(&tv0, NULL);
	struct _gc_ctx ctx = {base, batch_new(), 1024, 0};
	struct log *log = logs_oldest(base->logs);
	int r = log_iterate_sorted(log, gc_size,
				   _base_gc_callback, &ctx);
	if (r < 0) {
		goto error;
	}
	r = base_write(base, ctx.batch, 0);
	batch_free(ctx.batch);
	if (r < 0) {
		goto error;
	}
	r = ctx.written + r;
error:;
	gettimeofday(&tv1, NULL);
	log_info(base->db, "Gc round of size %6.1f MB took %lu ms. (r=%i)",
		 (float)gc_size / (1024*1024.),
		 TIMEVAL_MSEC_SUBTRACT(tv1, tv0),
		 r);
	return r;
}
