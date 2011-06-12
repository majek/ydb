#define _POSIX_C_SOURCE 200809L

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_record.h"
#include "ydb_reader.h"

struct reader {
	struct db *db;
	struct file *file;
	char *filename;
};

struct reader *reader_new(struct db *db, struct dir *dir, const char *filename)
{
	struct file *file = file_open_read(dir, filename);
	if (file == NULL) {
		return NULL;
	}

	struct reader *reader = malloc(sizeof(struct reader));
	memset(reader, 0, sizeof(struct reader));
	reader->db = db;
	reader->file = file;
	reader->filename = strdup(filename);
	return reader;
}

void reader_free(struct reader *reader)
{
	file_close(reader->file);
	free(reader->filename);
	free(reader);
}

void _reader_log_error(struct reader *reader, int r, uint64_t offset)
{
	switch (r) {
	case -1: log_error(reader->db, "%s#%llu can't read data, invalid magic",
			   reader->filename, (unsigned long long)offset);
		break;
	case -2: 	/* Record needs more data than read. */
		log_error(reader->db, "%s#%llu can't read data, record error or file truncated",
			  reader->filename, (unsigned long long)offset);
		break;
	case -3: log_error(reader->db, "%s#%llu can't read data, checksum error",
			   reader->filename, (unsigned long long)offset);
		break;
	default: log_error(reader->db, "%s#%llu can't read data",
			   reader->filename, (unsigned long long)offset);
	};
}

int reader_read(struct reader *reader,
		uint64_t offset,
		char *buffer, unsigned buffer_sz,
		struct keyvalue *kv)
{
	int r = file_pread(reader->file, buffer, buffer_sz, offset);
	if (r == -1) {
		return -1;
	}
	struct record rec;
	r = record_unpack(buffer, buffer_sz, &rec);
	if (r < 0) {
		_reader_log_error(reader, r, offset);
		return -1;
	}

	if (rec.magic != YDB_LOG_SET) {
		log_error(reader->db, "%s#%llu can't read record, it's not of type SET",
			  reader->filename, (unsigned long long)offset);
		return -1;
	}
	*kv = (struct keyvalue) {rec.key, rec.key_sz,
				 rec.value, rec.value_sz};
	return 0;
}

void reader_prefetch(struct reader *reader, uint64_t offset, uint64_t size)
{
	file_prefetch(reader->file, offset, size);
}

int reader_replay(struct reader *reader, reader_replay_cb callback, void *context)
{
	uint64_t size = 0;
	char *buf_start = file_mmap_ro(reader->file, &size);
	if (buf_start == NULL) {
		return -1;
	}
	char *buf_end = buf_start + size;
	char *buf = buf_start;
	char *buf_last_good = buf_start;

	while (buf < buf_end) {
		struct record rec;
		int r = record_unpack(buf, buf_end - buf, &rec);
		if (r < 0) {
			_reader_log_error(reader, r, buf - buf_start);
			log_error(reader->db, "In order to continue you may "
				  "want to truncate the log file %s to %li "
				  "bytes. In such case you will lose %.1f MB "
				  "of data.", reader->filename,
				  buf - buf_start,
				  (float)(buf_end - buf) / (1024*1024.));
			return -1;
		}
		callback(context, rec.magic, rec.key, rec.key_sz,
			 buf - buf_start, r);
		buf += r;
		buf_last_good = buf;
	}
	file_munmap(reader->db, buf_start, size);
	return 0;
}

uint64_t reader_size(struct reader *reader)
{
	uint64_t size = 0;
	file_size(reader->file, &size);
	return size;
}
