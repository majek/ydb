#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_writer.h"
#include "ydb_batch.h"
#include "ydb_record.h"

#define BATCH_MIN_SLOTS 32

struct batch {
	struct iovec *iov;
	int iov_cnt;
	int iov_sz;
	uint64_t total_size;
	unsigned total_sets;
};

struct batch *batch_new()
{
	struct batch *batch = malloc(sizeof(struct batch));
	memset(batch, 0, sizeof(struct batch));
	batch->iov = malloc(sizeof(struct iovec) * BATCH_MIN_SLOTS);
	batch->iov_sz = BATCH_MIN_SLOTS;
	return batch;
}

void batch_free(struct batch *batch)
{
	int i;
	for (i=0; i < batch->iov_cnt; i++) {
		free(batch->iov[i].iov_base);
	}
	free(batch->iov);
	free(batch);
}

static int _batch_alloc_iov(struct batch *batch)
{
	if (batch->iov_cnt == batch->iov_sz) {
		int new_sz = batch->iov_sz * 2;
		batch->iov = realloc(batch->iov, sizeof(struct iovec) * new_sz);
		batch->iov_sz = new_sz;
	}
	return batch->iov_cnt++;
}

void batch_set(struct batch *batch,
	       const char *key, unsigned key_sz,
	       const char *value, unsigned value_sz)
{
	int slot_no = _batch_alloc_iov(batch);
	batch->iov[slot_no] = record_pack((struct record){YDB_LOG_SET,
				key, key_sz, value, value_sz});
	batch->total_size += batch->iov[slot_no].iov_len;
	batch->total_sets += 1;
}

void batch_del(struct batch *batch,
	       char *key, unsigned key_sz)
{
	int slot_no = _batch_alloc_iov(batch);
	batch->iov[slot_no] = record_pack((struct record) {YDB_LOG_DEL,
				key, key_sz, NULL, 0});
	batch->total_size += batch->iov[slot_no].iov_len;
}

int batch_write(struct batch *batch, struct writer *writer,
		batch_write_cb callback, void *context)
{
	uint64_t offset;
	int r = writer_write(writer, batch->iov, batch->iov_cnt, &offset);
	if (r == -1) {
		return -1;
	}
	int i;
	for (i=0; i < batch->iov_cnt; i++) {
		struct iovec *slot = &batch->iov[i];
		struct record rec = record_unpack_force(*slot);
		callback(context, rec.magic, rec.key, rec.key_sz,
			 offset, slot->iov_len);
		offset += slot->iov_len;
	}
	return r;
}

uint64_t batch_size(struct batch *batch)
{
	return batch->total_size;
}

unsigned batch_sets(struct batch *batch)
{
	return batch->total_sets;
}
