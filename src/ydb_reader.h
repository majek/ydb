struct reader;

struct reader *reader_new(struct db *db, struct dir *dir, const char *filename);
void reader_free(struct reader *reader);
int reader_read(struct reader *reader,
		uint64_t offset,
		char *buffer, unsigned buffer_sz,
		struct keyvalue *kv);
void reader_prefetch(struct reader *reader, uint64_t offset, uint64_t size);

typedef void (*reader_replay_cb)(void *context,
				 uint32_t magic,
				 const char *key, unsigned key_sz,
				 uint64_t offset, uint64_t size);

int reader_replay(struct reader *reader,
		  reader_replay_cb callback, void *context);
uint64_t reader_size(struct reader *reader);
