
struct dir;
struct file;


struct dir *dir_open(struct db *db, const char *pathname);
struct dir *dir_openat(struct dir *top_dir, const char *dirname);
void dir_free(struct dir *dir);
int dir_renameat(struct dir *dir,
		 const char *old_filename, const char *new_filename,
		 int keep_backup);
int dir_unlink(struct dir *dir, const char *filename);
char **dir_list(struct dir *dir,
		int (*filter)(void *ud, const char *name), void *filter_ud);
int dir_file_exists(struct dir *dir, const char *filename);
int dir_truncateat(struct dir *dir, const char *filename, uint64_t size);


struct file *file_open_append(struct dir *top_dir, const char *filename);
struct file *file_open_append_new(struct dir *top_dir, const char *filename);
struct file *file_open_new_rw(struct dir *top_dir, const char *filename);
struct file *file_open_rw(struct dir *top_dir, const char *filename);
struct file *file_open_read(struct dir *top_dir, const char *filename);
void file_close(struct file *file);
int file_rind(struct file *file);

int file_truncate(struct file *file, uint64_t size);
int file_sync(struct file *file);
int file_size(struct file *file, uint64_t *size_ptr);
int file_pread(struct file *file, void *buf, uint64_t count, uint64_t offset);
int file_write(struct file *file, void *start_buf, uint64_t count);
int file_appendv(struct file *file, const struct iovec *iov, int iovcnt,
		 uint64_t file_size);
void file_prefetch(struct file *file, uint64_t offset, uint64_t size);

/* TODO: remove one */
void *file_mmap_ro(struct file *file, uint64_t *size_ptr);
void *file_mmap(struct file *file, uint64_t size);
void *file_mmap_share(struct file *file, uint64_t size);
int file_msync(struct db *db, void *ptr, uint64_t size, int sync);
void file_munmap(struct db *db, void *ptr, uint64_t size);
void *file_remap(struct db *db, void *ptr,
		 uint64_t old_size, uint64_t new_size);


