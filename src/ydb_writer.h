struct writer;
struct writer *writer_new(struct dir *dir, const char *filename, int create);
int writer_write(struct writer *writer, struct iovec *iov, int iov_cnt,
		 uint64_t *offset_ptr);
void writer_free(struct writer *writer);
uint64_t writer_filesize(struct writer *writer);

void writer_sync(struct writer *writer);




