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
#include "ydb_sys.h"
#include "ydb_batch.h"
#include "ydb_itree.h"

#include "ydb.h"
#include "ydb_base.h"


static void _dummy_reply_callback(void *ctx, uint32_t magic,
				  const char *key, unsigned key_sz,
				  uint64_t offset, uint64_t size) {
	ctx = ctx; magic = magic;
	key = key; key_sz = key_sz;
	offset = offset; size = size;
	assert(0);
}

int base_roll(struct base *base)
{
	uint64_t log_number = logs_new_number(base->logs);
	if (log_number == 0) {
		log_error(base->db, "I have to open a new log, but it's "
			  "impossible - too many log files are already opened. "
			  "%s", "");
		return -2;
	}

	writer_free(base->writer);
	base->writer = writer_new(base->log_dir, log_filename(log_number), 1);
	if (base->writer == NULL) {
		log_error(base->db, "Unable to create a new log, no %llu",
			  (unsigned long long)log_number);
		return -1;
	}
	struct log *log = log_new_replay(base->db, log_number, base->log_dir,
					 base->index_dir,
					 itree_move_callback, base->itree);
	if (log == NULL) {
		writer_free(base->writer);
		base->writer = NULL;
		log_error(base->db, "Can open new log file %llu for writing "
			  "bug not for reading. Weird.",
			  (unsigned long long)log_number);
		return -1;
	}
	log_do_replay(log, _dummy_reply_callback, NULL);
	struct log *newest = logs_newest(base->logs);
	int r = log_freeze(newest);
	if (r != 0) {
		log_error(base->db, "log=%llx can't freeze log",
			 (unsigned long long)log_get_number(newest));
		log_free(log);
		return -1;
	}
	log_info(base->db, "log=%llx %6.1f MB committed, %6.1f MB used, "
		 "%10u items (freezing)",
		 (unsigned long long)log_get_number(newest),
		 (float)log_disk_size(newest) / (1024*1024.),
		 (float)log_used_size(newest) / (1024*1024.),
		 log_sets_count(newest));
	/* TODO: */
	/* int r = log_save(newest); */
	/* if (r != 0) { */
	/* 	log_warn(base->db, "Can't save index for log %llu.", */
	/* 		 log_get_number(newest)); */
	/* } */
	logs_add(base->logs, log);
	return 0;
}

struct _save_ctx {
	struct swriter *swriter;
	struct log *newest;
};

static int _save_callback(void *ctx_p, struct log *log)
{
	struct _save_ctx *ctx = (struct _save_ctx *)ctx_p;
	if (log == ctx->newest) {
		return 0;
	}

	return swriter_write(ctx->swriter,
			     log_get_number(log),
			     log_get_bitmap(log));
}

static int _base_save_state(struct base *base)
{
	struct timeval tv0, tv1;
	gettimeofday(&tv0, NULL);

	struct swriter *swriter = swriter_new(base->index_dir, STATE_FILENAME);
	if (swriter == NULL) {
		log_error(base->db, "Unable to write snapshot. %s", "");
		return -1;
	}

	struct _save_ctx ctx = {swriter, logs_newest(base->logs)};
	int r = logs_iterate(base->logs, _save_callback, &ctx);
	if (r != 0) {
		swriter_free(swriter, 0);
		log_error(base->db, "Unable to write snapshot. %s", "");
		return -1;
	}

	r = swriter_free(swriter, 1);
	if (r == 0) {
		gettimeofday(&tv1, NULL);
		log_info(base->db, "Snapshot saved in %lu ms.",
			 TIMEVAL_MSEC_SUBTRACT(tv1, tv0));
	} else {
		log_error(base->db, "Unable to save snapshot.%s", "");
	}
	return r;
}

static void _do_snapshot(void *base_p) {
	struct base *base = (struct base *)base_p;
	_base_save_state(base);
}

int base_schedule_snapshot(struct base *base)
{
	if (base->snapshot_child_pid == -1) {
		return _base_save_state(base);
	}
	if (base->snapshot_child_pid == 0) {
		base->snapshot_child_pid = sys_fork(_do_snapshot, base);
		return base->snapshot_child_pid == -1 ? -1 : 0;
	}

	if (sys_pid_exist(base->snapshot_child_pid) == 0) {
		base->snapshot_child_pid = sys_fork(_do_snapshot, base);
		return base->snapshot_child_pid == -1 ? -1 : 0;
	}
	return -2;
}

int base_maybe_free_oldest(struct base *base)
{
	int c = 0;
	while (log_is_unused(logs_oldest(base->logs)) &&
	       logs_oldest(base->logs) != logs_newest(base->logs)) {
		/* Free the old log */
		struct log *log = logs_oldest(base->logs);
		log_info(base->db, "Deleting unused log %llx.",
			 (unsigned long long)log_get_number(log));

		stddev_remove(&base->disk_size, log_disk_size(log));
		logs_del(base->logs, log);
		log_free_remove(log);
		c += 1;
	}
	return c;
}

static uint64_t _filename_log_number(const char *filename) {
	char *e;
	long long log_number = strtoll(filename, &e, 16);
	if (*e == '.') {
		return log_number;
	}
	return 0;
}

static int _filter(void *ud, const char *filename)
{
	uint64_t log_number = *(uint64_t*)ud;
	if (fnmatch("[0-9a-f]*.ydb", filename, FNM_PATHNAME) == 0) {
		if (_filename_log_number(filename) > log_number) {
			return 1;
		}
	}
	return 0;
}

void logs_enumerate(struct dir *log_dir, uint64_t log_number,
		    uint64_t **logno_list_ptr, int *logno_list_sz_ptr)
{
	int pos = 0;
	int sz = 1024;
	uint64_t *logno_list = malloc(sizeof(uint64_t)*sz);
	char **files_org = dir_list(log_dir, _filter, &log_number);
	char **files;
	for (files = files_org; *files != NULL; files++) {
		logno_list[pos++] = _filename_log_number(*files);
		free(*files);
		if (pos == sz) {
			sz *= 2;
			logno_list = realloc(logno_list, sizeof(uint64_t)*sz);
		}
	}
	free(files_org);
	*logno_list_ptr = logno_list;
	*logno_list_sz_ptr = pos;
}

struct _iter_context {
	struct base *base;
	uint64_t prefetch_size;
	ydb_iter_callback callback;
	void *userdata;
};

static int _base_iter(void *ic_p, struct log *log)
{
	struct _iter_context *ic = (struct _iter_context *)ic_p;
	return log_iterate_sorted(log, ic->prefetch_size,
				  ic->callback, ic->userdata);
}

int base_iterate(struct base *base, uint64_t prefetch_size,
		 ydb_iter_callback callback, void *userdata)
{
	struct _iter_context ic = {base, prefetch_size, callback, userdata};
	return logs_iterate(base->logs, _base_iter, &ic);
}


void base_print_stats(struct base *base)
{
	log_info(base->db, "Stats: %s", "");
	log_info(base->db, "%u/%u logs in use",
		 (unsigned)(log_get_number(logs_newest(base->logs)) -
			    log_get_number(logs_oldest(base->logs)) + 1),
		 base->max_open_logs);
	uint64_t counter;
	double avg, dev;
	stddev_get(&base->used_size, &counter, &avg, &dev);
	log_info(base->db, "Item stats: %9.1f bytes average, %7.1f bytes deviation, "
		 "%llu items", avg, dev, (unsigned long long)counter);
	unsigned long allocated, wasted;
	itree_mem_stats(base->itree, &allocated, &wasted);
	log_info(base->db, "Tree memory: %8.1f MB committed, "
		 "%8.1f MB in use, committed/used ratio of %.3f",
		 (float)(allocated) / (1024*1024.),
		 (float)(allocated - wasted) / (1024*1024.),
		 (float)allocated / (float)(allocated - wasted));
	log_info(base->db, "Disk space: %9.1f MB committed, %8.1f MB in use, "
		 "committed/used ratio of %.3f",
		 (float)base->disk_size.sum / (1024*1024.),
		 (float)base->used_size.sum / (1024*1024.),
		 (float)base->disk_size.sum / (float)base->used_size.sum);
}
