#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "hamt.h"
#include "bitmap.h"

#include "ydb_common.h"
#include "ydb_db.h"
#include "ydb_file.h"
#include "ydb_log.h"

#include "ydb_logs.h"


struct logs {
	struct db *db;
	struct hamt_root logs;
	struct log *oldest;
	struct log *newest;
	unsigned slots;
};

static uint64_t _hash_log(void *ud, void *log_p) {
	ud = ud;
	struct log *log = (struct log*)log_p;
	return log_get_number(log);
}

static void *_wrap_malloc(void *ctx, unsigned size) {
	ctx = ctx;
	return malloc(size);
}

static void _wrap_free(void *ctx, void *ptr, unsigned size) {
	ctx = ctx; size = size;
	free(ptr);
}


struct logs *logs_new(struct db *db, unsigned slots)
{
	struct logs *logs = malloc(sizeof(struct logs));
	memset(logs, 0, sizeof(struct logs));
	logs->db = db;
	logs->logs = HAMT_ROOT(NULL, _wrap_malloc, _wrap_free, _hash_log, NULL);
	logs->slots = slots;
	return logs;
}

void logs_free(struct logs *logs)
{
	/* TODO: better... */
	while (1) {
		struct hamt_state state;
		struct log *log = hamt_first(&logs->logs, &state);
		if (log == NULL) {
			break;
		}
		hamt_delete(&logs->logs, log_get_number(log));
	}
	free(logs);
}

static struct log *log_by_number(struct logs *logs, uint64_t log_no)
{
	struct log *found = hamt_search(&logs->logs, log_no);
	assert(found);
	return found;
}

struct log *log_by_remno(struct logs *logs, uint64_t log_remno)
{
	uint64_t log_no;
	uint64_t rem = log_get_number(logs->oldest) % logs->slots;
	uint64_t base = log_get_number(logs->oldest) - rem;
	if (log_remno >= rem) {
		log_no = base + log_remno;
	} else {
		log_no = base + log_remno + logs->slots;
	}
	return log_by_number(logs, log_no);
}

uint64_t log_to_remno(struct logs *logs, struct log *log)
{
	return log_get_number(log) % logs->slots;
}

uint64_t logs_new_number(struct logs *logs)
{
	if (logs->oldest == NULL) {
		return 1;
	}
	uint64_t old = log_get_number(logs->oldest);
	uint64_t log_no = log_get_number(logs->newest) + 1;
	if (log_no >= old + logs->slots) {
		return 0;
	}
	return log_no;
}

void logs_add(struct logs *logs, struct log *log)
{
	assert(log);
	struct log *found = hamt_insert(&logs->logs, log);
	assert(found == log);
	if (logs->oldest == NULL) {
		logs->oldest = log;
	}
	if (logs->newest) {
		assert(log_get_number(logs->newest) < log_get_number(log));
	}
	logs->newest = log;
}

void logs_del(struct logs *logs, struct log *log)
{
	assert(logs->oldest == log);
	struct log *found = hamt_delete(&logs->logs, log_get_number(log));
	assert(found);

        struct hamt_state state;
	logs->oldest = hamt_first(&logs->logs, &state);
	if (logs->newest == log) {
		logs->newest = NULL;
	}
}

int logs_iterate(struct logs *logs, logs_callback callback, void *context)
{
        struct hamt_state state;
        struct log *log;
        hamt_for_each(log, &logs->logs, &state) {
		int r = callback(context, log);
		if (r != 0) {
			return r;
		}
	}
	return 0;
}

struct log *logs_oldest(struct logs *logs)
{
	return logs->oldest;
}

struct log *logs_newest(struct logs *logs)
{
	return logs->newest;
}

