struct hashdir_item {
	uint128_t key_hash;
	uint64_t offset;
	uint64_t size;
	uint32_t bitmap_pos;
};

struct hashdir *hashdir_new(struct db *db);
struct hashdir *hashdir_new_load(struct db *db,
				 struct dir *dir, const char *filename);
struct hashdir *hashdir_dup_sorted(struct hashdir *hdo);
void hashdir_free(struct hashdir *hd);

int hashdir_add(struct hashdir *hd, struct hashdir_item hi);
struct hashdir_item hashdir_get(struct hashdir *hd, int hdpos);
int hashdir_del(struct hashdir *hd, int hdpos);
int hashdir_del_last(struct hashdir *hd);
int hashdir_save(struct hashdir *hd, struct dir *dir, const char *filename);
int hashdir_size(struct hashdir *hd);
void hashdir_freeze(struct hashdir *hd);

struct bitmap *hashdir_get_bitmap(struct hashdir *hd);
