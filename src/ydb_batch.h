
struct batch;
struct batch *batch_new();
void batch_free(struct batch *batch);
void batch_set(struct batch *batch,
	       const char *key, unsigned key_sz,
	       const char *value, unsigned value_sz);
void batch_del(struct batch *batch,
	       char *key, unsigned key_sz);

typedef void (*batch_write_cb)(void *context,
			       uint32_t magic,
			       const char *key, unsigned key_sz,
			       uint64_t offset, uint64_t size);


int batch_write(struct batch *batch, struct writer *writer,
		batch_write_cb callback, void *context);

uint64_t batch_size(struct batch *batch);
unsigned batch_sets(struct batch *batch);

