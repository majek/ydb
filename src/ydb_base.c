#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <sys/time.h>

#include "config.h"
#include "list.h"
#include "stddev.h"
#include "bitmap.h"

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_logs.h"
#include "ydb_hashdir.h"
#include "ydb_frozen_list.h"
#include "ydb_log.h"
#include "ydb_itree.h"
#include "ydb_writer.h"
#include "ydb_state.h"
#include "ydb_batch.h"

#include "ydb.h"
#include "ydb_base.h"


static struct hashdir_item _get(void *base_p, uint64_t log_remno, int hpos) {
	struct base *base = (struct base *)base_p;
	struct log *log = log_by_remno(base->logs, log_remno);
	return log_get(log, hpos);
}

static void _add(void *base_p, struct hashdir_item hdi,
		 uint64_t *log_remno_ptr, int *hpos_ptr) {
	struct base *base = (struct base *)base_p;
	struct log *log = logs_newest(base->logs);
	stddev_add(&base->used_size, hdi.size);

	*hpos_ptr = log_add(log, hdi);
	*log_remno_ptr = log_to_remno(base->logs, log);
}

static void _del(void *base_p, uint64_t log_remno, int hpos) {
	struct base *base = (struct base *)base_p;
	struct log *log = log_by_remno(base->logs, log_remno);

	struct hashdir_item hdi = log_del(log, hpos);
	stddev_remove(&base->used_size, hdi.size);
}

void base_move_callback(void *base_p, struct log *log,
			int new_hpos, int old_hpos)
{
	struct base *base = (struct base *)base_p;
	itree_move_callback(base->itree, log_to_remno(base->logs, log),
			    new_hpos, old_hpos);
}

static uint64_t _between(uint64_t min, uint64_t user, uint64_t max)
{
	if (user >= min && user <= max) {
		return user;
	}
	return max;
}

struct base *base_new(struct db *db, struct dir *log_dir, struct dir *index_dir,
		      struct ydb_options *options)
{
	struct base *base = malloc(sizeof(struct base));
	memset(base, 0, sizeof(struct base));
	base->log_dir = log_dir;
	base->index_dir = index_dir;

	base->log_file_size_limit = _between(
		4096,
		options ? options->log_file_size_limit : 0,
		1ULL << 32);

	base->max_open_logs = _between(
		2,
		options ? options->max_open_logs : 0,
		1 << 16);

	base->index_slots_limit = _between(
		2,
		options ? options->index_size_limit / 25 : 0,
		1 << 23);

	base->writer = NULL;

	base->db = db;
	base->itree = itree_new(_get, _add, _del, base);
	base->logs = logs_new(base->db, base->max_open_logs);
	base->frozen_list = frozen_list_new(db);
	return base;
}

static int _save_log(void *base_p, struct log *log)
{
	struct base *base = (struct base *)base_p;
	if (log != logs_newest(base->logs)) {
		log_index_save(log);
	}
	return 0;
}

void base_free(struct base *base)
{
	logs_iterate(base->logs, _save_log, base);

	while (logs_oldest(base->logs)) {
		struct log *log = logs_oldest(base->logs);
		logs_del(base->logs, log);
		log_free(log);
	}
	frozen_list_free(base->frozen_list);

	if (base->writer) {
		writer_free(base->writer);
	}
	itree_free(base->itree);
	logs_free(base->logs);
	free(base);
}

static int _replay_add_callback(void *base_p, uint128_t key_hash, int hpos)
{
	struct base *base = (struct base*)base_p;
	assert(hpos > 0);

	struct log *log = logs_newest(base->logs);
	struct hashdir_item hdi = log_get(log, hpos);
	stddev_add(&base->used_size, hdi.size);
	itree_add_noidx(base->itree,
			key_hash,
			log_to_remno(base->logs, log),
			hpos);
	return 0;
}

int base_load(struct base *base)
{
	struct timeval tv0, tv1;

	int r;
	uint64_t log_number = 0;
	struct sreader *sreader = sreader_new_load(base->db, base->index_dir,
						   STATE_FILENAME);
	if (sreader == NULL) {
		log_info(base->db, "No snapshot found. %s", "");
	} else {
		log_info(base->db, "Reading snapshot \"%s\".",
			 STATE_FILENAME);
		while (1) {
			gettimeofday(&tv0, NULL);
			struct sreader_item rec;
			r = sreader_read(sreader, &rec);
			if (r != 1) {
				if (r != 0) { /* ok? */
					log_warn(base->db, "Error on reading snapshot. I'll "
						 "slow-read all the remaining logs from %llx.",
						 (unsigned long long)log_number+1);
				}
				break;
			}
			assert(rec.log_number > log_number);
			log_number = rec.log_number;
			struct log *log = log_new_fast(base->db, log_number,
						       base->log_dir,
						       base->index_dir,
						       rec.bitmap,
						       base_move_callback,
						       base,
						       base->frozen_list);
			if (log == NULL) {
				/* It's possible that we removed the
				 * oldest log, and the snapshot is not
				 * yet saved. */
				if (logs_oldest(base->logs) != NULL ||
				    dir_file_exists(base->log_dir, log_filename(log_number))) {
					log_number -= 1;
					/* Try to read this log lazily. */
					log_warn(base->db, "Error on reading log %llx. I'll "
						 "slow-read from this log.",
						 (unsigned long long)log_number+1);
					break;
				}
				/* If oldest == NULL, it's
				 * indistinguishable from a correct
				 * situation of delayed snapshot. */
			} else {
				logs_add(base->logs, log);
				log_iterate(log, _replay_add_callback, base);
				stddev_add(&base->disk_size, log_disk_size(log));
				gettimeofday(&tv1, NULL);
				log_info(base->db, "log=%llx %6.1f MB committed, "
					 "%6.1f MB used, %10u items "
					 "(from snapshot in %5li ms)",
					 (unsigned long long)log_number,
					 (float)log_disk_size(log) / (1024*1024.),
					 (float)log_used_size(log) / (1024*1024.),
					 log_sets_count(log),
					 TIMEVAL_MSEC_SUBTRACT(tv1, tv0));
			}
		}
		sreader_free(sreader);
	}

	uint64_t *logno_list;
	int logno_list_sz;
	logs_enumerate(base->log_dir, log_number,
		       &logno_list, &logno_list_sz);
	int i;
	for(i = 0; i < logno_list_sz-1; i++) {
		gettimeofday(&tv0, NULL);
		assert(logno_list[i] > log_number);
		log_number = logno_list[i];
		struct log *log = log_new_replay(base->db, log_number,
						 base->log_dir, base->index_dir,
						 base_move_callback,
						 base,
						 base->frozen_list);
		if (log == NULL) {
			log_error(base->db, "Can't load log %llx.",
				  (unsigned long long)log_number);
			return -1;
		}
		logs_add(base->logs, log);
		stddev_add(&base->disk_size, log_disk_size(log));
		int r = log_do_replay(log, base_write_callback, base);
		if (r != 0) {
			log_error(base->db, "Can't load log %llx.",
				  (unsigned long long)log_number);
			return -1;
		}
		r = log_freeze(log);
		if (r != 0) {
			log_error(base->db, "Can't load log %llx.",
				  (unsigned long long)log_number);
			return -1;
		}

		gettimeofday(&tv1, NULL);
		log_info(base->db, "log=%llx %6.1f MB committed, %6.1f MB used, "
			 "%10u items (replayed in %5li ms)",
			 (unsigned long long)log_number,
			 (float)log_disk_size(log) / (1024*1024.),
			 (float)log_used_size(log) / (1024*1024.),
			 log_sets_count(log),
			 TIMEVAL_MSEC_SUBTRACT(tv1, tv0));
	}
	uint64_t logno;
	if (logno_list_sz == 0) {
		logno = logs_new_number(base->logs);
		base->writer = writer_new(base->log_dir, log_filename(logno), 1);
	} else {
		logno = logno_list[logno_list_sz-1];
		base->writer = writer_new(base->log_dir, log_filename(logno), 0);
	}
	free(logno_list);

	gettimeofday(&tv0, NULL);
	assert(logno > log_number);
	log_number = logno;

	struct log *log = log_new_replay(base->db, log_number,
					 base->log_dir, base->index_dir,
					 base_move_callback,
					 base,
					 base->frozen_list);
	if (log == NULL) {
		log_error(base->db, "Can't load log %llx.",
			  (unsigned long long)log_number);
		return -1;
	}
	logs_add(base->logs, log);
	stddev_add(&base->disk_size, log_disk_size(log));
	r = log_do_replay(log, base_write_callback, base);
	if (r != 0) {
		log_error(base->db, "Can't load log %llx.",
			  (unsigned long long)log_number);
		return -1;
	}

	gettimeofday(&tv1, NULL);
	log_info(base->db, "log=%llx %6.1f MB committed, %6.1f MB used, "
		 "%10u items (writer replayed in %5li ms)",
		 (unsigned long long)log_number,
		 (float)log_disk_size(log) / (1024*1024.),
		 (float)log_used_size(log) / (1024*1024.),
		 log_sets_count(log),
		 TIMEVAL_MSEC_SUBTRACT(tv1, tv0));

	if (logno_list_sz > 1) {
		int r = base_schedule_snapshot(base);
		if (r < 0) {
			log_warn(base->db, "Unable to save snapshot. %s", "");
		}
	}
	return 0;
}
