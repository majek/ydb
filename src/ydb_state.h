struct sreader_item {
	uint64_t log_number;
	struct bitmap *bitmap;
};


struct swriter;

struct swriter *swriter_new(struct dir *dir, const char *filename);
int swriter_write(struct swriter *swriter, uint64_t log_number,
		  struct bitmap *bitmap);
int swriter_free(struct swriter *swriter, int move);


struct sreader;

struct sreader *sreader_new_load(struct db *db, struct dir *dir,
				 const char *filename);
void sreader_free(struct sreader *sreader);
int sreader_read(struct sreader *sreader, struct sreader_item *item);

