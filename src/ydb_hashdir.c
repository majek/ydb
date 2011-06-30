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
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_hashdir.h"

#include "ydb_hashdir_internal.h"



struct hashdir *_hashdir_new(struct db *db,
			    hashdir_move_cb callback, void *userdata)
{
	struct hashdir *hd = malloc(sizeof(struct hashdir));
	memset(hd, 0, sizeof(struct hashdir));
	hd->db = db;
	hd->move_callback = callback;
	hd->move_userdata = userdata;
	return hd;
}

void hashdir_free(struct hashdir *hd)
{
	if (IS_ACTIVE(hd)) {
		active_free(hd);
	} else {
		frozen_free(hd);
	}
	free(hd);
}

struct hashdir_item hashdir_del(struct hashdir *hd, int hdpos)
{
	if (IS_ACTIVE(hd)) {
		return active_del(hd, hdpos);
	}
	return frozen_del(hd, hdpos, 1, 1);
}

static int _hashdir_offset_sort(const void *a_p, const void *b_p)
{
	const struct item *a = a_p;
	const struct item *b = b_p;
	if (a->a_offset < b->a_offset) {
		return -1;
	}
	if (a->a_offset > b->a_offset) {
		return 1;
	}
	return 0;
}

/* TODO: This function should conserve memory. */
struct hashdir *hashdir_dup_sorted(struct hashdir *hdo)
{
	if (IS_FROZEN(hdo)) {
		/* Make sure there are no gaps. */
		hashdir_save(hdo);
	}

	struct hashdir *hd = _hashdir_new(hdo->db, NULL, NULL);
	hd->items_cnt = hdo->items_cnt;
	hd->items_sz = hdo->items_cnt;
	hd->items = malloc(sizeof(struct item) * hd->items_sz);
	memcpy(hd->items, hdo->items, sizeof(struct item) * hd->items_sz);
	qsort(hd->items, hd->items_cnt, sizeof(struct item),
	      _hashdir_offset_sort);
	return hd;
}

struct hashdir_item hashdir_get(struct hashdir *hd, int hdpos)
{
	assert(hdpos > 0  && hdpos < hd->items_cnt);
	return _unpack(hd->items[hdpos]);
}

void *_hashdir_next(struct hashdir *hd,
		    void *item_ptr,
		    struct hashdir_item *hdi,
		    int *hdpos_ptr)
{
	struct item *item = (struct item*)item_ptr;
	if (IS_FROZEN(hd)) {
		item = frozen_next(hd, item);
	} else {
		item = active_next(hd, item);
	}
	if (item != NULL) {
		*hdi = _unpack(*item);
		if (hdpos_ptr) {
			*hdpos_ptr = item - hd->items;
		}
	}
	return item;
}


int hashdir_size2(struct hashdir *hd)
{
	if (IS_ACTIVE(hd)) {
		return hd->items_cnt;
	}
	if (hd->deleted_cnt) {
		log_warn(hd->db, "hashdir_size on frozen. slow. %s", "");
		hashdir_save(hd);
	}
	return hd->items_cnt;
}
