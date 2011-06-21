#define _POSIX_C_SOURCE 200809L	/* openat(2) */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "bitmap.h"
#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_hashdir.h"

#include "ydb_hashdir_internal.h"


static int _verify_buf(struct hashdir *hd, char *buf, uint64_t size,
		       const char *filename)
{
	uint32_t *checksum_ptr = (uint32_t *)(buf + size - 4);
	if (adler32(buf, size - 4) != *checksum_ptr) {
		log_warn(hd->db, "Can't load %s: broken checksum.", filename);
		return -1;
	}

	if (size < 4 || (size - 4) % sizeof(struct item) != 0) {
		log_warn(hd->db, "Can't load %s: broken size.", filename);
		return -1;
	}
	return 0;
}

static int _hashdir_try_open_dirty(struct hashdir *hd)
{
	struct file *dirty = file_open_rw(hd->dir, hd->dirtyname);
	if (dirty == NULL) {
		return -1;
	}

	uint64_t size;
	int r = file_size(dirty, &size);
	if (r != 0) {
		file_close(dirty);
		return -1;
	}

	char *buf = file_mmap_share(dirty, size);
	file_close(dirty);
	if (buf == NULL) {
		return -1;
	}

	if (_verify_buf(hd, buf, size, hd->dirtyname) != 0) {
		file_munmap(hd->db, buf, size);
		return -1;
	}

	hd->mmap_sz = size;
	hd->items = (struct item*)buf;
	hd->items_cnt = (size - 4) / sizeof(struct item);
	return 0;
}

static int _hashdir_new_dirty(struct hashdir *hd, const char *filename)
{
	struct file *file = file_open_read(hd->dir, filename);
	if (file == NULL) {
		return -1;
	}
	uint64_t size;
	char *buf = file_mmap_ro(file, &size);
	file_close(file);
	if (buf == NULL) {
		return -1;
	}
	if (_verify_buf(hd, buf, size, filename) != 0) {
		file_munmap(hd->db, buf, size);
		return -1;
	}

	struct file *dirty = file_open_new_rw(hd->dir, hd->dirtyname);
	if (dirty == NULL) {
		file_munmap(hd->db, buf, size);
		return -1;
	}

	int r = file_truncate(dirty, size);
	if (r < 0) {
		file_munmap(hd->db, buf, size);
		file_close(dirty);
	}

	char *dbuf = file_mmap_share(dirty, size);
	file_close(dirty);
	if (dbuf == NULL) {
		file_munmap(hd->db, buf, size);
		return -1;
	}
	memcpy(dbuf, buf, size);
	file_munmap(hd->db, buf, size);

	file_msync(hd->db, dbuf, size, 0);

	hd->mmap_sz = size;
	hd->items = (struct item*)dbuf;
	hd->items_cnt = (size - 4) / sizeof(struct item);
	return 0;
}

static char *_dirty_filename(const char *pathname)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "%s.dirty", pathname);
	return buf;
}

struct hashdir *hashdir_new_load(struct db *db,
				 hashdir_move_cb callback, void *userdata,
				 struct dir *dir, const char *filename,
				 struct bitmap *mask)
{
	struct hashdir *hd = _hashdir_new(db, callback, userdata);

	hd->dirtyname = strdup(_dirty_filename(filename));
	hd->dir = dir;

	int r = 0;
	if (dir_file_exists(dir, hd->dirtyname)) {
		r = _hashdir_try_open_dirty(hd);
		if (r != 0) {
			r = _hashdir_new_dirty(hd, filename);
		}
	} else {
		r = _hashdir_new_dirty(hd, filename);
	}

	if (r != 0) {
		hashdir_free(hd);
		return NULL;
	}

	hd->deleted_sz = hd->items_cnt >= 1024 ? hd->items_cnt/4 : hd->items_cnt;
	hd->deleted = malloc(sizeof(int) * hd->deleted_sz);
	hd->deleted_cnt = 0;

	hd->bitmap = mask;
	int i;
	for (i=1; i < hd->items_cnt; i++) {
		int bpos = hd->items[i].bitmap_pos;
		if (bitmap_get(mask, bpos) == 1) {
			frozen_del(hd, i, 0, 0);
		}
	}
	return hd;
}

void frozen_free(struct hashdir *hd)
{
	free(hd->dirtyname);
	if (hd->bitmap) {
		bitmap_free(hd->bitmap);
	}
	free(hd->deleted);
}

static int _rev_int_cmp(const void *p1, const void *p2)
{
	const int a = abs(*(int*)p1), b = abs(*(int*)p2);
	if (a < b) {
		return 1;
	} else if (a > b) {
		return -1;
	}
	return 0;
}

int hashdir_save(struct hashdir *hd)
{
	log_info(hd->db, "SAVE %i %i", hd->deleted_cnt, hd->items_cnt);
	assert(IS_FROZEN(hd));
	qsort(hd->deleted, hd->deleted_cnt, sizeof(int), _rev_int_cmp);

	/* TODO: I'm sure we can do better than just moving last item
	 * a lot of times.  */
	int i;
	for (i=0; i < hd->deleted_cnt; i++) {
		active_del(hd, hd->deleted[i]);
	}
	free(hd->deleted);
	hd->deleted_sz = hd->items_cnt >= 1024 ? hd->items_cnt/4 : hd->items_cnt;
	hd->deleted = malloc(sizeof(int) * hd->deleted_sz);
	hd->deleted_cnt = 0;

	int size = sizeof(struct item) * hd->items_cnt + 4;
	hd->items = file_remap(hd->db, hd->items, hd->mmap_sz, size);
	hd->mmap_sz = size;
	assert(hd->items);

	uint32_t *checksum = (uint32_t*)((char*)hd->items + size - 4);
	*checksum = adler32((char*)hd->items, size - 4);

	int r = dir_truncateat(hd->dir, hd->dirtyname, size);
	int p = file_msync(hd->db, hd->items, size, 0);
	return r || p;
}


struct hashdir_item frozen_del(struct hashdir *hd, int hpos, int in_index,
			       int may_save)
{
	assert(hpos > 0  && hpos < hd->items_cnt);
	struct hashdir_item hdi = _unpack(hd->items[hpos]);

	if (in_index) {
		assert(bitmap_get(hd->bitmap, hdi.bitmap_pos) == 0);
		bitmap_set(hd->bitmap, hdi.bitmap_pos);
	}
	hd->deleted[hd->deleted_cnt] = hpos;
	hd->deleted_cnt += 1;

	if (may_save && hd->deleted_cnt >= 1024 && hd->deleted_cnt > hd->items_cnt / 8) {
		hashdir_save(hd);
	}

	if (hd->deleted_cnt == hd->deleted_sz) {
		int sz = hd->deleted_sz * 2;
		hd->deleted = realloc(hd->deleted, sizeof(int) * sz);
		hd->deleted_sz = sz;
	}
	return hdi;
}

struct bitmap *hashdir_get_bitmap(struct hashdir *hd)
{
	assert(IS_FROZEN(hd));
	return hd->bitmap;
}

struct item *frozen_next(struct hashdir *hd, struct item *item)
{
	if (item == NULL) {
		item = &hd->items[1];
	} else {
		item ++;
	}

	struct item *last = &hd->items[hd->items_cnt];
	for (;item < last; item++) {
		if (bitmap_get(hd->bitmap, (unsigned)item->bitmap_pos) == 0) {
			return item;
		}
	}
	return NULL;
}
