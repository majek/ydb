#define _POSIX_C_SOURCE 200809L	/* openat(2) */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "config.h"
#include "list.h"
#include "bitmap.h"
#include "ydb_common.h"
#include "ydb_db.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_hashdir.h"
#include "ydb_frozen_list.h"

#include "ydb_hashdir_internal.h"



struct frozen_list {
	struct db *db;
	int busy;
	struct list_head list;
};

struct frozen_list *frozen_list_new(struct db *db)
{
	struct frozen_list *fl = malloc(sizeof(struct frozen_list));
	memset(fl, 0, sizeof(struct frozen_list));
	fl->db = db;
	INIT_LIST_HEAD(&fl->list);

	return fl;
}

void frozen_list_free(struct frozen_list *fl)
{
	assert(list_empty(&fl->list));
	free(fl);
}

void frozen_list_add(struct frozen_list *fl, struct hashdir *hd)
{
	list_add(&hd->in_frozen_list, &fl->list);
}

void frozen_list_del(struct frozen_list *fl, struct hashdir *hd)
{
	fl = fl;
	list_del(&hd->in_frozen_list);
}

void frozen_list_incr(struct frozen_list *fl, struct hashdir *hd)
{
	while (!list_is_last(&hd->in_frozen_list, &fl->list)) {
		struct list_head *next = hd->in_frozen_list.next;
		struct hashdir *hdn =
			container_of(next, struct hashdir, in_frozen_list);
		if (hdn->deleted_cnt >= hd->deleted_cnt) {
			break;
		}
		list_del(&hd->in_frozen_list);
		__list_add(&hd->in_frozen_list, next, next->next);
	}
	frozen_list_maybe_marshall(fl);
}

int frozen_list_maybe_marshall(struct frozen_list *fl)
{
	db_do_answers(fl->db);
	if (fl->busy) {
		return 0;
	}
	return frozen_list_marshall(fl);
}


struct _task_save_ctx {
	struct db *db;
	void *ptr;
	uint64_t size;
	struct frozen_list *fl;
};

static void _answer_unbusy(void *fl_p)
{
	struct frozen_list *fl = (struct frozen_list*)fl_p;
	fl->busy = 0;
	frozen_list_maybe_marshall(fl);
}

static void _task_save(void *ctx_p)
{
	struct _task_save_ctx *ctx = (struct _task_save_ctx*)ctx_p;
	/* If that fails, I guess not much will break. */
	file_msync(ctx->db, ctx->ptr, ctx->size, 1);
	db_answer(ctx->db, _answer_unbusy, ctx->fl);
	free(ctx);
}

int frozen_list_marshall(struct frozen_list *fl)
{
	if (list_empty(&fl->list)) {
		return 0;
	}
	struct list_head *last = fl->list.prev;
	struct hashdir *hd =
		container_of(last, struct hashdir, in_frozen_list);

	if (hd->deleted_cnt <= 1024) {
		return 0;
	}
	frozen_list_del(fl, hd);
	frozen_list_add(fl, hd);

	/* TODO: how to react on error? */
	hashdir_save(hd);

	fl->busy = 1;
	struct _task_save_ctx *ctx = malloc(sizeof(struct _task_save_ctx));
	*ctx = (struct _task_save_ctx){fl->db, hd->items, hd->mmap_sz, fl};
	db_task(fl->db, _task_save, ctx);

	return 1;
}


/* int frozen_list_marshall(struct db *db, struct list_head *frozen_list) */
/* { */
/* 	if (list_empty(frozen_list)) { */
/* 		return 0; */
/* 	} */
/* 	struct list_head *last = frozen_list->prev; */
/* 	struct hashdir *hd = */
/* 		container_of(last, struct hashdir, in_frozen_list); */

/* 	if (hd->deleted_cnt > 1024) { */
/* 		/\* TODO: how to react on error? *\/ */
/* 		hashdir_save(hd); */

/* 		db_task(db, _task_save, ); */
		
/* 		worker_do_answers(struct worker *worker); */

/* 		return 1; */
/* 	} */
/* } */
