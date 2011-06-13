#define _POSIX_C_SOURCE 200809L	/* openat(2) */
#define _GNU_SOURCE 		/* O_NOATIME */

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include "ydb_logging.h"
#include "ydb_file.h"

#define MIN(a,b) ((a) <= (b) ? (a) : (b))


#define DIRTRACE(dir, result, format, ...)				\
	do {								\
		if (result < 0) {					\
			char *buf = alloca(1024);			\
			snprintf(buf, 1024,  format, __VA_ARGS__);	\
			log_perror(dir->db, "%s = %i", buf, result);	\
		}							\
	} while (0)

#define FILETRACE(file, result, format, ...)				\
	do {								\
		if (result < 0) {					\
			char *buf = alloca(1024);			\
			snprintf(buf, 1024,  format, __VA_ARGS__);	\
			log_perror(file->db, "%s = %i", buf, result);	\
		}							\
	} while (0)

#define DBTRACE(db, result, format, ...)				\
	do {								\
		if (result < 0) {					\
			char *buf = alloca(1024);			\
			snprintf(buf, 1024,  format, __VA_ARGS__);	\
			log_perror(db, "%s = %i", buf, result);		\
		}							\
	} while (0)



struct dir {
	struct db *db;
	char *pathname;
	DIR *dir;
};

static struct dir *_dir_new(struct db *db, const char *top_pathname,
			    const char *dirname)
{
	struct dir *dir = malloc(sizeof(struct dir));
	memset(dir, 0, sizeof(struct dir));
	dir->db = db;

	if (top_pathname) {
		char pathname[FILENAME_MAX];
		snprintf(pathname, sizeof(pathname), "%s/%s",
			 top_pathname, dirname);
		dir->pathname = strdup(pathname);
	} else {
		dir->pathname = strdup(dirname);
	}
	if (!dir->pathname) abort();
	return dir;
}

struct dir *dir_open(struct db *db, const char *pathname)
{
	struct dir *dir = _dir_new(db, NULL, pathname);
	dir->dir = opendir(pathname);
	int r = (dir->dir != NULL) ? 0 : -1;
	if (r == -1) {
		r = mkdir(pathname, 0700);
		DIRTRACE(dir, r, "mkdirat(\"%s\")", dir->pathname);
		if (r == 0) {
			dir->dir = opendir(pathname);
			r = (dir->dir != NULL) ? 0 : -1;
			DIRTRACE(dir, r, "opendir(\"%s\")", pathname);
		}
		if (r == -1) {
			dir_free(dir);
			return NULL;
		}
	}
	return dir;
}

/* There is no opendirat() function. */
static DIR *opendirat(DIR *dirp, const char *pathname)
{
	int fd = openat(dirfd(dirp), pathname, O_RDONLY | O_DIRECTORY);
	if (fd != -1) {
		DIR *d = fdopendir(fd);
		if (d != NULL) {
			return d;
		}
		close(fd);
	}
	return NULL;
}

struct dir *dir_openat(struct dir *top_dir, const char *dirname)
{
	struct dir *dir = _dir_new(top_dir->db, top_dir->pathname, dirname);
	dir->dir = opendirat(top_dir->dir, dirname);
	int r = (dir->dir != NULL) ? 0 : -1;
	DIRTRACE(dir, r, "opendirat(\"%s\")", dir->pathname);
	if (r == -1) {
		r = mkdirat(dirfd(top_dir->dir), dirname, 0700);
		DIRTRACE(dir, r, "mkdirat(\"%s\")", dir->pathname);
		if (r == 0) {
			dir->dir = opendirat(top_dir->dir, dirname);
			r = (dir->dir != NULL) ? 0 : -1;
			DIRTRACE(dir, r, "opendirat(\"%s\")", dir->pathname);
		}
		if (r == -1) {
			dir_free(dir);
			return NULL;
		}
	}
	return dir;
}

void dir_free(struct dir *dir)
{
	int saved_errno = errno;
	if (dir->dir) {
		int r = closedir(dir->dir);
		DIRTRACE(dir, r, "closedir(\"%s\")", dir->pathname);
	}
	if (dir->pathname) free(dir->pathname);
	free(dir);
	errno = saved_errno;
}

int dir_file_exists(struct dir *dir, const char *filename)
{
	int r = faccessat(dirfd(dir->dir), filename, F_OK, 0);
	return r == 0;
}

static char *_backup_filename(const char *pathname)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "%s~", pathname);
	return buf;
}

int dir_renameat(struct dir *dir,
		 const char *old_filename, const char *new_filename,
		 int keep_backup)
{
	if (keep_backup && dir_file_exists(dir, new_filename)) {
		/* Try to preserve the target file, create a hardlink
		 * before overwriting it.  Ignore the return value,
		 * it's fine not to have backup. */
		int x;
		char *backup = _backup_filename(new_filename);
		if (dir_file_exists(dir, backup)) {
			x = dir_unlink(dir, backup);
		}
		x = linkat(dirfd(dir->dir), new_filename,
			   dirfd(dir->dir), backup, 0);
		x = x;
	}
	int r = renameat(dirfd(dir->dir), old_filename,
			 dirfd(dir->dir), new_filename);
	DIRTRACE(dir, r, "renameat(\"%s\", \"%s\")",
		 old_filename, new_filename);

	/* Sync directory. */
	fsync(dirfd(dir->dir));
	return r;
}

static int dir_is_file(struct dir *dir, const char *filename)
{
	struct stat stat;
	int r = fstatat(dirfd(dir->dir), filename, &stat, 0);
	DIRTRACE(dir, r, "fstatat(\"%s\", \"%s\")", dir->pathname, filename);
	if (r != 0) {
		return -1;
	}
	return S_ISREG(stat.st_mode);
}


int dir_unlink(struct dir *dir, const char *filename)
{

	int r = unlinkat(dirfd(dir->dir), filename, 0);
	DIRTRACE(dir, r, "unlinkat(\"%s\", \"%s\")",
		  dir->pathname, filename);
	return r;
}

static int _cmpstringp(const void *p1, const void *p2)
{
	const char *a = *(char * const*)p1;
	const char *b = *(char * const*)p2;
	return strcmp(a, b);
}

char **dir_list(struct dir *dir,
		int (*filter)(void *ud, const char *name), void *filter_ud)
{
	int p = 0;
	int sz = 1024;
	char **files = (char **)malloc(sizeof(char*) * sz);
	rewinddir(dir->dir);
	while (1) {
		struct dirent *entry = readdir(dir->dir);
		if (entry == NULL) {
			break;
		}
		if (filter(filter_ud, entry->d_name) &&
		    dir_is_file(dir, entry->d_name)) {
			files[p++] = strdup(entry->d_name);
			if (p == sz) {
				sz *= 2;
				files = (char **) \
					realloc(files, sizeof(char*) * sz);
			}
		}
	}
	rewinddir(dir->dir);
	qsort(files, p, sizeof(char*), _cmpstringp);
	files[p++] = NULL;
	return files;
}


struct file {
	struct db *db;
	struct dir *top_dir;
	char *pathname;
	int fd;
};

static struct file *_file_new(struct dir *top_dir, const char *filename,
			      int flags)
{
	struct file *file = malloc(sizeof(struct file));
	memset(file, 0, sizeof(struct file));
	file->db = top_dir->db;
	file->top_dir = top_dir;

	char pathname[FILENAME_MAX];
	snprintf(pathname, sizeof(pathname), "%s/%s",
		 top_dir->pathname, filename);

	file->fd = openat(dirfd(top_dir->dir), filename, flags, 0600);

	FILETRACE(file, file->fd, "openat(\"%s\")", pathname);
	if (file->fd == -1) {
		free(file);
		return NULL;
	}
	file->pathname = strdup(pathname);
	return file;
}

struct file *file_open_append(struct dir *top_dir, const char *filename)
{
	return _file_new(top_dir, filename,
			 O_WRONLY | O_APPEND | O_CREAT);
}

struct file *file_open_append_new(struct dir *top_dir, const char *filename)
{
	struct file *file = _file_new(top_dir, filename,
				      O_WRONLY | O_APPEND | O_CREAT | O_EXCL);
	if (file) {
		return file;
	}
	if (errno == EEXIST) {
		renameat(dirfd(top_dir->dir), filename,
			 dirfd(top_dir->dir), _backup_filename(filename));
		file = _file_new(top_dir, filename,
				 O_WRONLY | O_APPEND | O_CREAT | O_TRUNC);
	}
	return file;
}

struct file *file_open_new_rw(struct dir *top_dir, const char *filename)
{
	return _file_new(top_dir, filename, O_RDWR | O_CREAT | O_TRUNC);
}

struct file *file_open_read(struct dir *top_dir, const char *filename)
{
	return _file_new(top_dir, filename, O_RDONLY | O_NOATIME);
}


void file_close(struct file *file)
{
	int saved_errno = errno;
	int r = close(file->fd);
	FILETRACE(file, r, "close(\"%s\")", file->pathname);
	free(file->pathname);
	free(file);
	errno = saved_errno;
}

int file_rind(struct file *file)
{
	int r = dup(file->fd);
	FILETRACE(file, r, "dup(\"%s\")", file->pathname);
	file_close(file);
	return r;
}


int file_truncate(struct file *file, uint64_t size)
{
	int r = ftruncate(file->fd, size);
	FILETRACE(file, r, "ftruncate(\"%s\", %llu)", file->pathname,
		  (long long unsigned)size);
	return r;
}

int file_sync(struct file *file)
{
	int r = fsync(file->fd);
	FILETRACE(file, r, "fsync(\"%s\")", file->pathname);
	/* Sync also containing directory */
	fsync(dirfd(file->top_dir->dir));
	return r;
}

int file_size(struct file *file, uint64_t *size_ptr)
{
	struct stat stat;
	int r = fstat(file->fd, &stat);
	FILETRACE(file, r, "fstat(\"%s\")", file->pathname);
	if (r != -1) {
		*size_ptr = stat.st_size;
	}
	return r;
}

int file_pread(struct file *file, void *buf, uint64_t count, uint64_t offset)
{
	int r = pread(file->fd, buf, count, offset);
	FILETRACE(file, r, "pread(\"%s\", %llu, %llu)", file->pathname,
		  (unsigned long long)count, (unsigned long long)offset);
	/* TODO: naive, what if we encounter EOF */
	assert(r == -1 || (unsigned)r == count);
	return r;
}

int file_write(struct file *file, void *start_buf, uint64_t count)
{
	char *buf = start_buf;
	while(count > 0) {
		int r = write(file->fd, buf, count);
		FILETRACE(file, r, "write(\"%s\", %llu)",
			  file->pathname, (unsigned long long)count);
		if (r < 1) {
			break;
		}
		buf += r;
		count -= r;
	}
	return buf - (char*)start_buf;
}


int file_appendv(struct file *file, const struct iovec *iov, int iovcnt,
		 uint64_t file_size)
{
	int i;
	uint64_t len = 0;
	for (i=0; i < iovcnt; i++) {
		len += iov[i].iov_len;
	}

	int r = writev(file->fd, iov, iovcnt);
	FILETRACE(file, r, "writev(\"%s\", %llu)", file->pathname,
		  (unsigned long long)len);
	/* TODO: are we really sure writev won't write in chunks? */
	if (r < 0 || (unsigned)r != len) {
		file_truncate(file, file_size);
		return -1;
	}
	return len;
}

void file_prefetch(struct file *file, uint64_t offset, uint64_t size)
{
	int r = posix_fadvise(file->fd, offset, size, POSIX_FADV_WILLNEED);
	FILETRACE(file, r, "fadvise(\"%s\", %llu, %llu)", file->pathname,
		  (unsigned long long)offset, (unsigned long long)size);
}


/* Try to mmap files above 40' bits boundary. */
#define MMAP_HIGH_ADDR ((void*)(0x0000010000000000ULL))

void *file_mmap_ro(struct file *file, uint64_t *size_ptr)
{
	int r = file_size(file, size_ptr);
	if (r == -1) {
		return NULL;
	}
	if (*size_ptr == 0) {
		/* Can't mmap 0 bytes. */
		static int a;
		return &a;
	}
	void *ptr = mmap(MMAP_HIGH_ADDR, *size_ptr, PROT_READ | PROT_WRITE,
			 MAP_PRIVATE, file->fd, 0);
	if (ptr == MAP_FAILED) {
		FILETRACE(file, -1, "mmap(\"%s\", %llu)",
			  file->pathname, (unsigned long long)*size_ptr);
		return NULL;
	}

	/* Only hints, ignore errors.  */
	madvise(ptr, *size_ptr, MADV_SEQUENTIAL);
	madvise(ptr, MIN(4096*4, *size_ptr), MADV_WILLNEED);

	return ptr;
}

static void *_file_mmap(struct file *file, uint64_t size, int flags)
{
	if (size == 0) {
		/* Can't mmap 0 bytes. */
		static int a;
		return &a;
	}
	void *ptr = mmap(MMAP_HIGH_ADDR, size, PROT_READ | PROT_WRITE, flags,
			 file->fd, 0);
	if (ptr == MAP_FAILED) {
		FILETRACE(file, -1, "mmap(\"%s\", %llu, %i)",
			  file->pathname, (unsigned long long)size, flags);
		return NULL;
	}
	return ptr;
}

/* void *file_mmap_ro(struct file *file, uint64_t size) */
/* { */
/* 	void *ptr = _file_mmap(file, size, MAP_PRIVATE); */
/* 	if (ptr) { */
/* 		/\* Only hints, ignore errors.  *\/ */
/* 		madvise(ptr, *size_ptr, MADV_SEQUENTIAL); */
/* 		madvise(ptr, MIN(4096*4, *size_ptr), MADV_WILLNEED); */
/* 	} */
/* 	return ptr; */
/* } */

void *file_mmap_share(struct file *file, uint64_t size)
{
	return _file_mmap(file, size, MAP_SHARED);
}

int file_msync(struct db *db, void *ptr, uint64_t size)
{
	int r = msync(ptr, size, MS_SYNC);
	DBTRACE(db, r, "msync(%p, %llu, MS_SYNC)",
		ptr, (unsigned long long)size);
	return r;
}

void file_munmap(struct db *db, void *ptr, uint64_t size)
{
	if (size == 0) {
		return;
	}
        int r = munmap(ptr, size);
	DBTRACE(db, r, "munmmap(%p, %llu)",
		ptr, (unsigned long long)size);
}

void *file_remap(struct db *db, void *ptr,
		 uint64_t old_size, uint64_t new_size)
{
	assert(old_size);
	assert(new_size);
	void *new_ptr = mremap(ptr, old_size, new_size, MREMAP_MAYMOVE);
	if (new_ptr == MAP_FAILED) {
		DBTRACE(db, -1, "mremap(%p, %llu, %llu)",
			ptr, (unsigned long long)old_size,
			(unsigned long long)new_size);
		return NULL;
	}
	return new_ptr;
}
