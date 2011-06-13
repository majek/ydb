#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_hashdir.h"
#include "bitmap.h"


struct item {
	uint128_t key_hash;
	uint32_t a_offset:27;	// align: DATA_ALIGN
	uint32_t a_size:22;	// align: DATA_ALIGN
	uint32_t bitmap_pos:23;
}  __attribute__ ((packed));

#define DATA_ALIGN 5
#define HASHDIR_INITIAL_SIZE (128)
#define HASHDIR_SHRINK_THRESHOLD (((4096*4) / (int)sizeof(struct item)) + 1)

static struct hashdir_item _unpack(struct item item)
{
	return (struct hashdir_item) {item.key_hash,
			(uint64_t)item.a_offset << DATA_ALIGN,
			(uint64_t)item.a_size << DATA_ALIGN,
			item.bitmap_pos
			};
}
static struct item _pack(struct hashdir_item hdi) {
	assert(hdi.offset % (1 << DATA_ALIGN) == 0);
	assert(hdi.size % (1 << DATA_ALIGN) == 0);
	return (struct item) {hdi.key_hash,
			(uint64_t)hdi.offset >> DATA_ALIGN,
			(uint64_t)hdi.size >> DATA_ALIGN,
			hdi.bitmap_pos
			};
}

struct hashdir {
	struct db *db;
	struct item *items;
	int items_cnt;
	int items_sz;
	uint64_t mmap_sz;
	struct bitmap *bitmap;
};

static struct hashdir *_hashdir_new(struct db *db)
{
	struct hashdir *hd = malloc(sizeof(struct hashdir));
	memset(hd, 0, sizeof(struct hashdir));
	hd->db = db;
	return hd;
}


struct hashdir *hashdir_new(struct db *db)
{
	struct hashdir *hd = _hashdir_new(db);
	hd->items = malloc(sizeof(struct item)*HASHDIR_INITIAL_SIZE);
	hd->items[0] = (struct item){0,0,0,0};
	hd->items_cnt = 1;
	hd->items_sz = HASHDIR_INITIAL_SIZE;
	return hd;
}


static char *_dirty_filename(const char *pathname)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "%s.dirty", pathname);
	return buf;
}


static char *_hashdir_create_dirty(struct dir *dir, const char *filename,
				   uint64_t size)
{
	struct file *dirty = file_open_new_rw(dir, _dirty_filename(filename));
	if (dirty == NULL) {
		return NULL;
	}

	int r = file_truncate(dirty, size);
	if (r < 0) {
		file_close(dirty);
		return NULL;
	}

	char *buf = file_mmap_share(dirty, size);
	file_close(dirty);
	if (buf == NULL) {
		return NULL;
	}
	return buf;
}

struct hashdir *hashdir_new_load(struct db *db,
				 struct dir *dir, const char *filename)
{
	/* TODO: try to load dirty first. */
	struct file *mmap = file_open_read(dir, filename);
	if (mmap == NULL) {
		return NULL;
	}
	uint64_t size;
	char *buf = file_mmap_ro(mmap, &size);
	file_close(mmap);
	if (buf == NULL) {
		return NULL;
	}
	if (size < 4 || (size-4) % sizeof(struct item) != 0) {
		log_error(db, "%s can't load file: wrong size %llu",
			  filename, (unsigned long long)size);
		file_munmap(db, buf, size);
		return NULL;
	}
	uint32_t *checksum_ptr = (uint32_t*)(buf + size - 4);
	if (adler32(buf, size - 4) != *checksum_ptr) {
		log_error(db, "%s can't load file: broken checksum "
			  "%08x != %08x", filename,
			  adler32(buf, size-4), *checksum_ptr);
		file_munmap(db, buf, size);
		return NULL;
	}

	char *dirty = _hashdir_create_dirty(dir, filename, size);
	if (dirty == NULL) {
		log_error(db, "Can't open dirty index. %s", "");
		file_munmap(db, buf, size);
		return NULL;
	}

	memcpy(dirty, buf, size);
	file_munmap(db, buf, size);

	struct hashdir *hd = _hashdir_new(db);
	hd->items = (struct item*)dirty;
	hd->items_cnt = (size-4) / sizeof(struct item);
	hd->items_sz = hd->items_cnt;
	hd->mmap_sz = size;

	/* TODO: are two passess really necessary? */
	int max_bitmap_pos = 0;
	int i;
	for (i=1; i < hd->items_cnt; i++) {
		int bitmap_pos = hd->items[i].bitmap_pos;
		if (bitmap_pos > max_bitmap_pos) {
			max_bitmap_pos = bitmap_pos;
		}
	}
	assert(max_bitmap_pos);
	hd->bitmap = bitmap_new(max_bitmap_pos + 1, 1);
	for (i=1; i < hd->items_cnt; i++) {
		bitmap_clear(hd->bitmap, hd->items[i].bitmap_pos);
	}

	return hd;
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

struct hashdir *hashdir_dup_sorted(struct hashdir *hdo)
{
	struct hashdir *hd = _hashdir_new(hdo->db);
	hd->items_cnt = hdo->items_cnt;
	hd->items_sz = hdo->items_cnt;
	hd->items = malloc(sizeof(struct item) * hd->items_sz);
	memcpy(hd->items, hdo->items, sizeof(struct item) * hd->items_sz);
	qsort(hd->items, hd->items_cnt, sizeof(struct item),
	      _hashdir_offset_sort);
	return hd;
}

void hashdir_free(struct hashdir *hd)
{
	if (hd->mmap_sz == 0) {
		free(hd->items);
	} else {
		file_munmap(hd->db, hd->items, hd->mmap_sz);
	}
	if (hd->bitmap) {
		bitmap_free(hd->bitmap);
	}
	free(hd);
}

int hashdir_add(struct hashdir *hd, struct hashdir_item hi)
{
	assert(hd->bitmap == NULL);
	assert(hd->mmap_sz == 0);
	if (hd->items_cnt == hd->items_sz) {
		int new_items_sz = hd->items_sz * 2;
		hd->items = realloc(hd->items, new_items_sz*sizeof(struct item));
		hd->items_sz = new_items_sz;
	}
	int hdpos = hd->items_cnt++;
	hd->items[hdpos] = _pack(hi);
	return hdpos;
}

struct hashdir_item hashdir_get(struct hashdir *hd, int hdpos)
{
	assert(hdpos > 0  && hdpos < hd->items_cnt);
	return _unpack(hd->items[hdpos]);
}

int hashdir_del(struct hashdir *hd, int hdpos)
{
	assert(hdpos > 0  && hdpos < hd->items_cnt);
	if (hd->bitmap) {
		struct hashdir_item hdi = _unpack(hd->items[hdpos]);
		bitmap_set(hd->bitmap, hdi.bitmap_pos);
	}

	int last_pos = hd->items_cnt - 1;
	if (hdpos == last_pos) {
		hashdir_del_last(hd);
		return -1;
	} else {
		/* copy the last one to freed space */
		hd->items[hdpos] = hd->items[last_pos];
		return last_pos;
	}
}

int hashdir_del_last(struct hashdir *hd)
{
	int last_pos = hd->items_cnt - 1;
	assert(last_pos > 0);
	hd->items[last_pos] = (struct item){0,0,0,0};
	hd->items_cnt -= 1;

	if (hd->items_cnt + HASHDIR_SHRINK_THRESHOLD < hd->items_sz) {
		uint64_t new_sz = sizeof(struct item)*hd->items_cnt + 4;
		if (hd->mmap_sz) {
			hd->items = file_remap(hd->db, hd->items,
					       hd->mmap_sz, new_sz);
			hd->mmap_sz = new_sz;
			hd->items_sz = hd->items_cnt;
			return 1;
		} else {
			hd->items = realloc(hd->items, new_sz);
			hd->items_sz = hd->items_cnt;
		}
	}
	return 0;
}



static char *_temp_filename(const char *pathname)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "%s.new", pathname);
	return buf;
}

int hashdir_save(struct hashdir *hd, struct dir *dir, const char *filename)
{
	dir = dir;
	filename = filename;

	assert(hd->bitmap);
	assert(hd->mmap_sz);

	/* TODO: truncate */
	char *buf = (char*)hd->items;
	uint64_t size = sizeof(struct item)*hd->items_cnt;
	*(uint32_t*)(buf+size) = adler32(buf, size);

	int r = file_msync(hd->db, buf, size);
	if (r < 0) {
		return -1;
	}
	return 0;
}

int hashdir_size(struct hashdir *hd)
{
	return hd->items_cnt;
}

struct bitmap *hashdir_get_bitmap(struct hashdir *hd)
{
	return hd->bitmap;
}

static int _hashdir_freeze_save(struct hashdir *hd,
				struct dir *dir, const char *filename)
{
	char *tmpname = _temp_filename(filename);
	struct file *file = file_open_append_new(dir, tmpname);
	if (file == NULL) {
		return -1;
	}

	uint64_t size = sizeof(struct item)*hd->items_cnt;
	uint32_t checksum = adler32((void*)hd->items, size);

	int r = file_write(file, hd->items, size);
	if (r < 0) {
		file_close(file);
		return -1;
	}
	r = file_write(file, &checksum, 4);
	if (r < 0) {
		file_close(file);
		return -1;
	}
	/* Renameat is atomic. There is no point in relying on that
	 * unless we do a proper fsync(). Sorry. */
	r = file_sync(file);
	if (r < 0) {
		file_close(file);
		return -1;
	}
	file_close(file);

	r = dir_renameat(dir, tmpname, filename, 0);
	if (r < 0) {
		return -1;
	}

	assert(hd->mmap_sz == 0);

	/* We're using mmap from old file. Let's switch to new. */
	struct hashdir *new_hd = hashdir_new_load(hd->db, dir, filename);
	if (new_hd == NULL) {
		return -1;
	}
	/* Swap. */
	struct hashdir t = *hd;
	*hd = *new_hd;
	*new_hd = t;
	hashdir_free(new_hd);
	return 0;
}

int hashdir_freeze(struct hashdir *hd, struct dir *dir, const char *filename)
{
	assert(hd->bitmap == NULL);
	hd->bitmap = bitmap_new(hd->items_cnt+1, 0);
	int i;
	for (i=1; i < hd->items_cnt; i++) {
		hd->items[i].bitmap_pos = i;
	}

	return _hashdir_freeze_save(hd, dir, filename);
}

