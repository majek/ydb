struct hashdir_item {
	uint128_t key_hash;
	uint64_t offset;
	uint64_t size;
	uint32_t bitmap_pos;
};

typedef void (*hashdir_move_cb)(void *ud, int new_hpos, int old_hpos);

struct hashdir *hashdir_new_active(struct db *db,
				   hashdir_move_cb callback, void *userdata);
struct hashdir *hashdir_new_load(struct db *db,
				 hashdir_move_cb callback, void *userdata,
				 struct dir *dir, const char *filename,
				 struct bitmap *mask);


struct hashdir *hashdir_dup_sorted(struct hashdir *hdo);
void hashdir_free(struct hashdir *hd);

struct hashdir_item hashdir_get(struct hashdir *hd, int hdpos);
struct hashdir_item hashdir_del(struct hashdir *hd, int hdpos);

int hashdir_add(struct hashdir *hd, struct hashdir_item hi);
int hashdir_freeze(struct hashdir *hd,
		   struct dir *dir, const char *filename);

int hashdir_save(struct hashdir *hd);
int hashdir_size2(struct hashdir *hd);

struct bitmap *hashdir_get_bitmap(struct hashdir *hd);


void *_hashdir_next(struct hashdir *hd,
		    void *item_ptr,
		    struct hashdir_item *hdi,
		    int *hdpos_ptr);

#define hashdir_for_each(hd, ptr, hdi)			\
	for (ptr = _hashdir_next(hd, NULL, &hdi, NULL);	\
	     ptr;					\
	     ptr = _hashdir_next(hd, ptr, &hdi, NULL))

#define hashdir_for_each_hdpos(hd, ptr, hdi, hdpos)		\
	for (ptr = _hashdir_next(hd, NULL, &hdi, &hdpos);	\
	     ptr;						\
	     ptr = _hashdir_next(hd, ptr, &hdi, &hdpos))
