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


#define HASHDIR_INITIAL_SIZE (128)

struct hashdir *hashdir_new_active(struct db *db,
				   hashdir_move_cb callback, void *userdata)
{
	struct hashdir *hd = _hashdir_new(db, callback, userdata);
	hd->items = malloc(sizeof(struct item)*HASHDIR_INITIAL_SIZE);
	hd->items[0] = (struct item){0,0,0,0};
	hd->items_cnt = 1;
	hd->items_sz = HASHDIR_INITIAL_SIZE;
	return hd;
}

void active_free(struct hashdir *hd)
{
	free(hd->items);
}

int hashdir_add(struct hashdir *hd, struct hashdir_item hi)
{
	assert(IS_ACTIVE(hd));
	if (hd->items_cnt == hd->items_sz) {
		int sz = hd->items_sz * 2;
		hd->items = realloc(hd->items, sz * sizeof(struct item));
		hd->items_sz = sz;
	}
	int hdpos = hd->items_cnt++;
	hd->items[hdpos] = _pack(hi);
	return hdpos;
}

struct hashdir_item active_del(struct hashdir *hd, int hdpos)
{
	assert(hdpos > 0  && hdpos < hd->items_cnt);
	struct hashdir_item hdi = _unpack(hd->items[hdpos]);

	int last_pos = hd->items_cnt - 1;
	if (hdpos != last_pos) {
		struct hashdir_item last = _unpack(hd->items[last_pos]);
		hd->items[hdpos] = hd->items[last_pos];
		hd->move_callback(hd->move_userdata, last.key_hash, hdpos, last_pos);
	}
	hd->items[last_pos] = (struct item){0,0,0,0};
	hd->items_cnt -= 1;
	return hdi;
}

static char *_temp_filename(const char *pathname)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "%s.new", pathname);
	return buf;
}

int hashdir_freeze(struct hashdir *hd, struct dir *dir, const char *filename)
{
	assert(IS_ACTIVE(hd));
	int i;
	for (i=1; i < hd->items_cnt; i++) {
		hd->items[i].bitmap_pos = i;
	}

	char *tmpname = _temp_filename(filename);
	struct file *file = file_open_append_new(dir, tmpname);
	if (file == NULL) goto error;

	uint64_t size = sizeof(struct item) * hd->items_cnt;
	uint32_t checksum = adler32((void*)hd->items, size);
	struct iovec iov[2] = {{hd->items, size},
			       {&checksum, 4}};
	int r = file_appendv(file, iov, 2, 0);
	if (r < 0) goto error;

	r = file_sync(file);
	if (r < 0) goto error;
	file_close(file);
	file = NULL;

	r = dir_renameat(dir, tmpname, filename, 0);
	if (r < 0) goto error;

	return 0;

error:;
	if (file) {
		file_close(file);
	}
	dir_unlink(dir, tmpname);
	return -1;
}

