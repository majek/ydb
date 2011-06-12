#define  _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "bitmap.h"

#include "ydb_common.h"
#include "ydb_db.h"
#include "ydb_file.h"
#include "ydb_logging.h"

#include "ydb_state.h"


#define STAT_MAGIC (0x57A78A61)

struct stat_record {
	uint32_t magic;
	uint32_t checksum;
	uint64_t log_number;
	uint32_t sz;
};

struct swriter {
	char *filename;
	struct file *file;
	uint64_t filesize;
	struct dir *dir;
};

static char *_new_filename(const char *filename)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "%s.new", filename);
	return buf;
}

struct swriter *swriter_new(struct dir *dir, const char *filename)
{
	struct file *file = file_open_append_new(dir, _new_filename(filename));
	if (file == NULL) {
		return NULL;
	}
	struct swriter *swriter = malloc(sizeof(struct swriter));
	swriter->filename = strdup(filename);
	swriter->file = file;
	swriter->filesize = 0;
	swriter->dir = dir;
	return swriter;
}

int swriter_write(struct swriter *swriter, uint64_t log_number,
		  struct bitmap *bitmap)
{
	int buf_sz;
	char *buf = bitmap_serialize(bitmap, &buf_sz);

	struct stat_record record;
	memset(&record, 0, sizeof(record));
	record = (struct stat_record) {
		.magic = STAT_MAGIC,
		.checksum = adler32(buf, buf_sz),
		.log_number = log_number,
		.sz = buf_sz
	};

	struct iovec iov[2] = {{&record, sizeof(record)},
			       {buf, buf_sz}};

	int r = file_appendv(swriter->file, iov, 2, swriter->filesize);
	if (r < 0) {
		return -1;
	}
	swriter->filesize += r;
	return 0;
}

int swriter_free(struct swriter *swriter, int move)
{
	int r = 0;
	if (move) {
		file_sync(swriter->file);
		file_close(swriter->file);
		r = dir_renameat(swriter->dir, _new_filename(swriter->filename),
				 swriter->filename, 1);
	} else {
		file_close(swriter->file);
	}
	free(swriter->filename);
	free(swriter);
	return r;
}

struct sreader {
	struct db *db;
	struct file *file;
	char *filename;
	char *buf_start;
	char *buf;
	char *buf_end;
	uint64_t size;
};

struct sreader *sreader_new_load(struct db *db, struct dir *dir,
				 const char *filename)
{
	struct file *file = file_open_read(dir, filename);
	if (file == NULL) {
		return NULL;
	}
	uint64_t size;
	char *buf_start = file_mmap_ro(file, &size);
	if (buf_start == NULL) {
		file_close(file);
		return NULL;
	}

	struct sreader *sreader = malloc(sizeof(struct sreader));
	sreader->db = db;
	sreader->filename = strdup(filename);
	sreader->file = file;
	sreader->buf_start = buf_start;
	sreader->buf = buf_start;
	sreader->buf_end = buf_start + size;
	sreader->size = size;
	return sreader;
}

void sreader_free(struct sreader *sreader)
{
	file_munmap(sreader->db, sreader->buf_start,  sreader->size);
	file_close(sreader->file);
	free(sreader->filename);
	free(sreader);
}

int sreader_read(struct sreader *sreader, struct sreader_item *item)
{
	if (sreader->buf == sreader->buf_end) {
		return 0;
	}

	if (sreader->buf + sizeof(struct stat_record) > sreader->buf_end) {
		goto truncated;
	}

	struct stat_record *record = (struct stat_record*)sreader->buf;
	if (record->magic != STAT_MAGIC) {
		log_error(sreader->db, "Can't load state from %s. Bad magic.",
			  sreader->filename);
		return -1;
	}

	sreader->buf += sizeof(struct stat_record);
	if (sreader->buf + record->sz > sreader->buf_end) {
		goto truncated;
	}
	if (adler32(sreader->buf, record->sz) != record->checksum) {
		log_error(sreader->db, "Can't load state from %s. Bad checksum.",
			  sreader->filename);
		return -1;
	}

	struct bitmap *bm = bitmap_new_from_blob(sreader->buf, record->sz);
	sreader->buf += record->sz;

	*item = (struct sreader_item){record->log_number, bm};
	return 1;

truncated:;
	log_error(sreader->db, "Can't load state from %s. File too short.",
		  sreader->filename);
	return -1;
}
