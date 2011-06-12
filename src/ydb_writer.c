#define _POSIX_C_SOURCE 200809L

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "ydb_logging.h"
#include "ydb_file.h"


struct writer {
	struct file *file;
	char *filename;
	uint64_t file_size;
};

struct writer *writer_new(struct dir *dir, const char *filename, int create)
{
	struct file *file;
	uint64_t fsize = 0;
	if (create) {
		file = file_open_append_new(dir, filename);
	} else {
		file = file_open_append(dir, filename);
		int r = file_size(file, &fsize);
		if (r == -1) {
			file_close(file);
			return NULL;
		}
	}
	if (file == NULL) {
		return NULL;
	}

	struct writer *writer = malloc(sizeof(struct writer));
	memset(writer, 0, sizeof(struct writer));
	writer->file = file;
	writer->filename = strdup(filename);
	writer->file_size = fsize;
	return writer;
}

int writer_write(struct writer *writer, struct iovec *iov, int iov_cnt,
		 uint64_t *offset_ptr)
{
	uint64_t prev_size = writer->file_size;
	int r = file_appendv(writer->file, iov, iov_cnt, prev_size);
	if (r == -1) {
		return -1;
	}
	writer->file_size += r;
	*offset_ptr = prev_size;
	return r;
}

void writer_free(struct writer *writer)
{
	/* TODO: fsync? */
	/* file_sync(writer->file); */
	file_close(writer->file);
	free(writer->filename);
	free(writer);
}

uint64_t writer_filesize(struct writer *writer)
{
	return writer->file_size;
}

void writer_sync(struct writer *writer)
{
	file_sync(writer->file);
}
