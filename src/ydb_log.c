#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <sys/time.h>

#include "config.h"
#include "list.h"
#include "bitmap.h"
#include "stddev.h"

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_hashdir.h"
#include "ydb_frozen_list.h"
#include "ydb_log.h"
#include "ydb_reader.h"
#include "ydb_record.h"



struct log {
	struct db *db;
	struct dir *log_dir;
	struct dir *index_dir;
	uint64_t log_number;
	struct reader *reader;

	struct stddev used_size;

	struct hashdir *hashdir;
	log_move_callback move_callback;
	void *move_userdata;

	struct frozen_list *frozen_list;
};


static void _log_move(void *log_p, int new_hpos, int old_hpos)
{
	struct log *log = (struct log *)log_p;
	log->move_callback(log->move_userdata, log, new_hpos, old_hpos);
}

static char *_filename(uint64_t log_number, char *suffix)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "%012llx.%s",
		 (unsigned long long)log_number, suffix);
	return buf;
}

char *log_filename(uint64_t log_number) {
	return _filename(log_number, "ydb");
}

static char *idx_filename(uint64_t log_number) {
	return _filename(log_number, "idx");
}

static char *_dirty_idx_filename(uint64_t log_number) {
	return _filename(log_number, "idx.dirty");
}

static struct log *_log_new(struct db *db, uint64_t log_number,
			    struct dir *log_dir, struct dir *index_dir,
			    log_move_callback move_callback,
			    void *move_userdata,
			    struct frozen_list *frozen_list)
{
	struct reader *reader = reader_new(db, log_dir, log_filename(log_number));
	if (reader == NULL) {
		return NULL;
	}

	struct log *log = malloc(sizeof(struct log));
	memset(log, 0, sizeof(struct log));
	log->db = db;
	log->log_dir = log_dir;
	log->index_dir = index_dir;
	log->log_number = log_number;
	log->reader = reader;

	log->move_callback = move_callback;
	log->move_userdata = move_userdata;

	log->frozen_list = frozen_list;
	return log;
}

struct log *log_new_replay(struct db *db, uint64_t log_number,
			   struct dir *log_dir, struct dir *index_dir,
			   log_move_callback move_callback,
			   void *move_context,
			   struct frozen_list *frozen_list)
{
	struct log *log = _log_new(db, log_number, log_dir, index_dir,
				   move_callback, move_context, frozen_list);
	if (log == NULL) {
		return NULL;
	}
	log->hashdir = hashdir_new_active(db, _log_move, log);
	return log;
}

int log_do_replay(struct log *log, log_replay_cb callback, void *userdata)
{
	return reader_replay(log->reader, callback, userdata);
}

struct log *log_new_fast(struct db *db, uint64_t log_number,
			 struct dir *log_dir, struct dir *index_dir,
			 struct bitmap *bitmap,
			 log_move_callback move_callback,
			 void *move_context,
			 struct frozen_list *frozen_list)
{
	struct log *log = _log_new(db, log_number, log_dir, index_dir,
				   move_callback, move_context, frozen_list);
	if (log == NULL) {
		return NULL;
	}
	char *idx_file = idx_filename(log_number);
	struct hashdir *hdsets = hashdir_new_load(db,
						  _log_move, log,
						  index_dir, idx_file,
						  bitmap,
						  frozen_list);
	if (hdsets == NULL) {
		log_warn(db, "Can't find index file \"%s\".", idx_file);
		log_free(log);
		return NULL;
		/* log_warn(db, "Can't find index file \"%s\". Trying rebuild.", */
		/* 	 idx_file); */
		/* hdsets = hashdir_new(db); */
		/* struct hashdir *hdall = hashdir_new(db); */
		/* _log_replay(log->reader, hdall, hdsets); */
		/* hashdir_free(hdall); /\* not interested in deletions *\/ */
		/* hashdir_freeze(hdsets); */
		/* int r = hashdir_save(hdsets, index_dir, idx_file); */
		/* if (r == -1) { */
		/* 	log_error(db, "Can't save index file \"%s\".", idx_file); */
		/* } */
	}

	void *ptr;
	struct hashdir_item hdi;
	hashdir_for_each(hdsets, ptr, hdi) {
		stddev_add(&log->used_size, hdi.size);
	}
	log->hashdir = hdsets;
	return log;
}

int log_iterate(struct log *log, log_callback callback, void *userdata)
{
	int hdpos;
	void *ptr;
	struct hashdir_item hdi;
	hashdir_for_each_hdpos(log->hashdir, ptr, hdi, hdpos) {
		int r = callback(userdata, hdi.key_hash, hdpos);
		if (r) {
			return r;
		}
	}
	return 0;
}

#define DIV_ROUND_UP(n,d) (((n) + (d) - 1) / (d))
#define PREFETCH_PAGE 4096

static int _iterate_prefetch(struct log *log, struct hashdir *shd,
			     int last_hpos, uint64_t prefetch_size)
{
	int hpos_max = hashdir_size2(shd);
	if (prefetch_size < 2) {
		return hpos_max;
	}

	uint64_t a = 0;
	uint64_t b = 0;

	uint64_t prefetched = 0;
	int i;
	for (i=last_hpos; i < hpos_max && prefetched < prefetch_size; i++) {
		struct hashdir_item hi = hashdir_get(shd, i);

		uint64_t c = hi.offset / PREFETCH_PAGE;
		uint64_t d = DIV_ROUND_UP(hi.offset + hi.size, PREFETCH_PAGE);

		if (c > b) { 	/* TODO: Concatenate if small number
				 * of pages in the middle are
				 * unused? */
			if (a && b) {
				reader_prefetch(log->reader,
						a*PREFETCH_PAGE,
						(b-a)*PREFETCH_PAGE);
			}
			a = c;
			b = d;
		} else {
			b = d;
		}
		prefetched += hi.size;
	}

	if (a && b) {
		reader_prefetch(log->reader,
				a*PREFETCH_PAGE,
				(b-a)*PREFETCH_PAGE);
	}

	return i;
}

int log_iterate_sorted(struct log *log, uint64_t prefetch_size,
		       log_iterate_callback callback, void *userdata)
{
	struct timeval tv0, tv1;
	gettimeofday(&tv0, NULL);
	struct hashdir *shd = hashdir_dup_sorted(log->hashdir);
	gettimeofday(&tv1, NULL);
	log_info(log->db, "Sorting index in log %llx took %5li ms.",
		 (unsigned long long)log->log_number,
		 TIMEVAL_MSEC_SUBTRACT(tv1, tv0));
	int hpos_max = hashdir_size2(shd);

	int last_hpos = 1;
	int r = 0;
	int i;
	for (i=1; i < hpos_max; i++) {
		if (i == last_hpos) {
			last_hpos = _iterate_prefetch(log, shd,
						      last_hpos, prefetch_size);
		}

		struct hashdir_item hi = hashdir_get(shd, i);
		struct keyvalue kv;
		char *buf = malloc(hi.size);
		r = reader_read(log->reader, hi.offset, buf, hi.size, &kv);
		if (r) {
			free(buf);
			break;
		}
		r = callback(userdata,
			     kv.key, kv.key_sz,
			     kv.value, kv.value_sz);
		free(buf);
		if (r) {
			break;
		}
	}

	hashdir_free(shd);
	return r;
}

int log_read(struct log *log, int hpos,
	     char *buffer, unsigned int buffer_sz,
	     struct keyvalue *kv)
{
	struct hashdir_item hi = hashdir_get(log->hashdir, hpos);
	assert(hi.size <= buffer_sz);
	return reader_read(log->reader, hi.offset, buffer, hi.size, kv);
}

unsigned log_prefetch(struct log *log, int hpos)
{
	struct hashdir_item hi = hashdir_get(log->hashdir, hpos);
	reader_prefetch(log->reader, hi.offset, hi.size);
	return hi.size;
}

unsigned log_buffer_size(struct log *log, int hpos)
{
	return hashdir_get(log->hashdir, hpos).size;
}

struct hashdir_item log_get(struct log *log, int hpos)
{
	return hashdir_get(log->hashdir, hpos);
}

int log_add(struct log *log, struct hashdir_item hdi)
{
	stddev_add(&log->used_size, hdi.size);
	return hashdir_add(log->hashdir, hdi);
}

struct hashdir_item log_del(struct log *log, int hpos)
{
	struct hashdir_item hdi = hashdir_del(log->hashdir, hpos);
	stddev_remove(&log->used_size, hdi.size);
	return hdi;
}

int log_freeze(struct log *log)
{
	int r = hashdir_freeze(log->hashdir, log->index_dir,
			       idx_filename(log->log_number));
	if (r == -1) {
		log_error(log->db, "Unable to save index for log %llx. This is "
			  "pretty bad.", (unsigned long long)log->log_number);
		return -1;
	}
	struct hashdir *hd = hashdir_new_load(log->db,
					      _log_move, log,
					      log->index_dir,
					      idx_filename(log->log_number),
					      bitmap_new(hashdir_size2(log->hashdir), 0),
					      log->frozen_list);
	if (hd == NULL) {
		log_error(log->db, "Can't load saved index %llx.",
			  (unsigned long long)log->log_number);
		return -1;
	}
	hashdir_free(log->hashdir);
	log->hashdir = hd;
	return 0;
}

void log_free_remove(struct log *log)
{
	reader_free(log->reader);
	int r = dir_unlink(log->log_dir, log_filename(log->log_number));
	if (r == -1) {
		log_warn(log->db, "Can't unlink unused log file %s.",
			 log_filename(log->log_number));
	}
	r = dir_unlink(log->index_dir, idx_filename(log->log_number));
	if (r == -1) {
		log_warn(log->db, "Can't unlink unused index file %s.",
			 idx_filename(log->log_number));
	}
	r = dir_unlink(log->index_dir, _dirty_idx_filename(log->log_number));
	if (r == -1) {
		log_warn(log->db, "Can't unlink unused dirty index file %s.",
			 _dirty_idx_filename(log->log_number));
	}
	if (log->hashdir) {
		hashdir_free(log->hashdir);
	}
	free(log);
}

void log_free(struct log *log)
{
	reader_free(log->reader);
	if (log->hashdir) {
		hashdir_free(log->hashdir);
	}
	free(log);
}

uint64_t log_get_number(struct log *log)
{
	return log->log_number;
}

struct bitmap *log_get_bitmap(struct log *log)
{
	return hashdir_get_bitmap(log->hashdir);
}

int log_is_unused(struct log *log)
{
	uint64_t count;
	stddev_get(&log->used_size, &count, NULL, NULL);
	return count == 0;
}

unsigned log_sets_count(struct log *log)
{
	return hashdir_size2(log->hashdir);
}

uint64_t log_disk_size(struct log *log)
{
	return reader_size(log->reader);
}


uint64_t log_used_size(struct log *log)
{
	return log->used_size.sum;
}

void log_index_save(struct log *log)
{
	int r = hashdir_save(log->hashdir);
	if (r == -1) {
		log_warn(log->db, "Unable to save index for "
			 "log %llx.",
			 (unsigned long long)log->log_number);
	}
}
