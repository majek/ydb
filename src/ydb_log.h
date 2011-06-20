struct log;

typedef int (*log_callback)(void *context, uint128_t key_hash, int hpos);
typedef void (*log_move_callback)(void *context, uint128_t key_hash, int hpos, int old_hpos);


struct log *log_new_fast(struct db *db, uint64_t log_number,
			 struct dir *log_dir, struct dir *index_dir,
			 struct bitmap *bitmap,
			 log_move_callback move_callback,
			 void *move_context);

struct log *log_new_replay(struct db *db, uint64_t log_number,
			   struct dir *log_dir, struct dir *index_dir,
			   log_move_callback move_callback,
			   void *move_context);

typedef void (*log_replay_cb)(void *context,
			      uint32_t magic,
			      const char *key, unsigned key_sz,
			      uint64_t offset, uint64_t size);
int log_do_replay(struct log *log, log_replay_cb callback, void *userdata);

int log_iterate(struct log *log, log_callback callback, void *userdata);

typedef int (*log_iterate_callback)(void *userdata,
				    const char *key, unsigned key_sz,
				    const char *value, unsigned value_sz);
int log_iterate_sorted(struct log *log, uint64_t prefetch_size,
		       log_iterate_callback callback, void *userdata);

void log_free_remove(struct log *log);
void log_free(struct log *log);


/* TODO: remove this method */
unsigned log_buffer_size(struct log *log, int hpos);
int log_read(struct log *log, int hpos,
	     char *buffer, unsigned int buffer_sz,
	     struct keyvalue *kv);
unsigned log_prefetch(struct log *log, int hpos);

struct hashdir_item log_get(struct log *log, int hpos);
int log_add(struct log *log, struct hashdir_item hdi);
struct hashdir_item log_del(struct log *log, int hpos);


int log_freeze(struct log *log);
uint64_t log_get_number(struct log *log);


struct bitmap *log_get_bitmap(struct log *log);


char *log_filename(uint64_t log_number);
int log_is_unused(struct log *log);

unsigned log_sets_count(struct log *log);

uint64_t log_disk_size(struct log *log);
uint64_t log_used_size(struct log *log);

void log_index_save(struct log *log);
