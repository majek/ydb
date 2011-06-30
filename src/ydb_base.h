
struct base {
	struct db *db;

	struct itree *itree;
	struct logs *logs;

	struct writer *writer;

	uint64_t log_file_size_limit;
	unsigned max_open_logs;
	unsigned index_slots_limit;

	struct stddev used_size; /* Records actually referenced by the db */
	struct stddev disk_size; /* Total log files size. sum_sq is not kept in sync.*/

	struct dir *log_dir;
	struct dir *index_dir;

	int snapshot_child_pid;
	struct frozen_list *frozen_list;
};

#define STATE_FILENAME "snapshot.bin"

/* ydb_base.c */
void base_move_callback(void *base_p, struct log *log,
			int new_hpos, int old_hpos);

struct base *base_new(struct db *db, struct dir *log_dir, struct dir *index_dir,
		      struct ydb_options *options);
void base_free(struct base *base);
int base_load(struct base *base);

/* ydb_base_aux.c */
int base_roll(struct base *base);
int base_schedule_snapshot(struct base *base);
int base_maybe_free_oldest(struct base *base);
void logs_enumerate(struct dir *log_dir, uint64_t log_number,
		    uint64_t **logno_list_ptr, int *logno_list_sz_ptr);

/* ydb_base_pub.c */
void base_prefetch(struct base *bas, struct ydb_vec *keysv, unsigned keysv_cnt);
int base_get(struct base *base,
	     const char *key, unsigned key_sz,
	     char *buf, unsigned buf_sz);
int base_write(struct base *base, struct batch *batch, int do_fsync);
float base_ratio(struct base *base);

void base_write_callback(void *base_p, uint32_t magic,
			 const char *key, unsigned key_sz,
			 uint64_t offset, uint64_t size);
int base_iterate(struct base *base, uint64_t prefetch_size,
		 ydb_iter_callback callback, void *userdata);

void base_print_stats(struct base *base);
int base_gc(struct base *base, unsigned gc_size);
