#ifndef _YDB_H
#define _YDB_H

#ifdef __cplusplus
extern "C" {
#  if 0
} // Make c-mode happy.
#  endif
#endif


/* The database structure. Must be used from a single thread only. */
struct ydb;


struct ydb_options {
	unsigned long long log_file_size_limit;
	unsigned max_open_logs;
	unsigned long long index_size_limit;
};


/* Open a new or existing database.
 *
 * Only a single process can access a particular database at a
 * time. Writing to a database from a multiple processes will result
 * in a database corruption.
 *
 * Return:
 *      pointer to struct ydb on success
 *      NULL if an error occurred  */
struct ydb *ydb_open(const char *directory, struct ydb_options *options);

/* Close a database. */
void ydb_close(struct ydb *ydb);

/* Get disk utilization ratio (committed/used disk space). */
float ydb_ratio(struct ydb *ydb);

/* Roll at most gc_size bytes of data in an attempt to reclaim disk
 * space. The disk space can temporarly increase. After rolling enough
 * data, in ideal case, disk utilization ratio should drop to a value
 * close to 1.0. Keep running this method periodically, whenever you
 * feel that the database is using too much disk space or the ratio is
 * too high.
 *
 * By setting gc_size you may limit the time it takes to complete a
 * roll cycle. If you want to force a cycle that will free a single
 * database log file, set the gc_size to a high value, for example
 * 4GB. */
int ydb_roll(struct ydb *ydb, unsigned gc_size);


struct ydb_vec {
	char *key;
	unsigned key_sz;
	unsigned value_sz;
};

/* Ask operating system to prefetch items from the disk. This method
 * will significantly improve 'get' speed if you can predict which
 * items you're going to access in the future.
 *
 * For every item 'value_sz' field will be set to a length of
 * appropriate value or 0 if the item isn't found. */
void ydb_prefetch(struct ydb *ydb,
		  struct ydb_vec *keysv, unsigned keysv_cnt);


enum ydb_errors {
	YDB_NOT_FOUND = -1,
	YDB_IO_ERROR = -2,
	YDB_BUFFER_ERROR = -3
};
/* Get a value for a key.
 *
 * Return
 *    number of bytes written to buffer if the item is successfully read,
 *    the return value larger than 'buf_sz' means that the value was
 *    truncated because given buffer was too small
 *    -1 if the item is not found
 *    -2 read error
 *    -3 buffer too small */
int ydb_get(struct ydb *ydb,
	    const char *key, unsigned key_sz,
	    char *buf, unsigned buf_sz);


/* Allocate new batch structure. */
struct ydb_batch *ydb_batch();

/* Unconditionally set an item in a database. */
void ydb_set(struct ydb_batch *batch,
	     const char *key, unsigned key_sz,
	     const char *value, unsigned value_sz);

/* Delete a key from database. */
void ydb_del(struct ydb_batch *batch,
	     const char *key, unsigned key_sz);


/* Write batch to disk and release 'ydb_batch' structure.
 *
 * Flush kernel write buffers using fsync() if 'do_fsync' parameter is
 * non zero.
 *
 * Return
 *     0 success
 *     -2 write error
 *     -3 too much data */
int ydb_write(struct ydb *ydb, struct ydb_batch *batch, int do_fsync);

/* Free batch structure. */
void ydb_batch_free(struct ydb_batch *batch);


typedef int (*ydb_iter_callback)(void *ud,
				 const char *key, unsigned key_sz,
				 const char *value, unsigned value_sz);

int ydb_iterate(struct ydb *ydb, unsigned prefetch_size,
		ydb_iter_callback callback, void *userdata);


#ifdef __cplusplus
}
#endif
#endif // _YDB_H
