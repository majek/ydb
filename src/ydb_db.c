#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

#include "ydb_logging.h"
#include "ydb_file.h"

#include "ydb_db.h"
#include "ydb_worker.h"


struct db {
	int log_fd;
	struct dir *log_dir;
	struct dir *index_dir;
	struct worker *worker;
};

static struct db *_db_new(const char *directory)
{
	struct db *db = malloc(sizeof(struct db));
	memset(db, 0, sizeof(struct db));
	db->log_fd = 2; /* stderr */

	db->log_dir = dir_open(db, directory);
	if (db->log_dir == NULL) {
		free(db);
		return NULL;
	}
	db->worker = worker_new(db);
	if (db->worker == NULL) {
		dir_free(db->log_dir);
		free(db);
		return NULL;
	}
	return db;
}

struct db *db_new(const char *directory)
{
	struct db *db = _db_new(directory);
	struct file *logfile = file_open_append(db->log_dir, "ydb.log");
	if (logfile == NULL) {
		goto error;
	}

	int fd = file_rind(logfile);
	if (fd == -1) {
		goto error;
	}
	db->log_fd = fd;

	db->index_dir = dir_openat(db->log_dir, "index");
	if (db->index_dir == NULL) {
		db_free(db);
		return NULL;
	}

	return db;
error:;
	dir_free(db->log_dir);
	free(db);
	return NULL;
}

struct db *db_new_mock()
{
	struct db *db = _db_new(".");
	db->index_dir = dir_open(db, ".");
	assert(db->index_dir);

	db->log_fd = dup(2); /* stderr */
	return db;
}


void db_free(struct db *db)
{
	worker_free(db->worker);
	dir_free(db->log_dir);
	dir_free(db->index_dir);
	close(db->log_fd);
	free(db);
}

int db_log_fd(struct db *db)
{
	return db->log_fd;
}

struct dir *db_log_dir(struct db *db)
{
	return db->log_dir;
}

struct dir *db_index_dir(struct db *db)
{
	return db->index_dir;
}

void db_task(struct db *db, db_task_callback callback, void *userdata)
{
	worker_new_task(db->worker, callback, userdata);
}

void db_answer(struct db *db, db_task_callback callback, void *userdata)
{
	worker_new_answer(db->worker, callback, userdata);
}

int db_do_answers(struct db *db)
{
	return worker_do_answers(db->worker);
}
