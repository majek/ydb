struct item {
	uint128_t key_hash;
	uint32_t a_offset:27;	// align: DATA_ALIGN
	uint32_t a_size:22;	// align: DATA_ALIGN
	uint32_t bitmap_pos:23;
}  __attribute__ ((packed));

struct hashdir {
	struct db *db;
	struct item *items;
	int items_cnt;
	int items_sz;

	hashdir_move_cb move_callback;
	void *move_userdata;

	char *dirtyname;
	struct dir *dir;
	uint64_t mmap_sz;
	struct bitmap *bitmap;
	uint32_t *deleted;
	uint32_t deleted_cnt;
	uint32_t deleted_sz;
};

#define DATA_ALIGN 5

static inline struct hashdir_item _unpack(struct item item)
{
	return (struct hashdir_item) {item.key_hash,
			(uint64_t)item.a_offset << DATA_ALIGN,
			(uint64_t)item.a_size << DATA_ALIGN,
			item.bitmap_pos
			};
}
static inline struct item _pack(struct hashdir_item hdi) {
	assert(hdi.offset % (1 << DATA_ALIGN) == 0);
	assert(hdi.size % (1 << DATA_ALIGN) == 0);
	return (struct item) {hdi.key_hash,
			(uint64_t)hdi.offset >> DATA_ALIGN,
			(uint64_t)hdi.size >> DATA_ALIGN,
			hdi.bitmap_pos
			};
}

#define IS_ACTIVE(hd) (!IS_FROZEN(hd))
#define IS_FROZEN(hd) ((hd)->dirtyname != NULL)

/* ydb_hashdir.c */
struct hashdir *_hashdir_new(struct db *db,
			     hashdir_move_cb callback, void *userdata);

/* ydb_hashdir_active.c */
void active_free(struct hashdir *hd);
struct hashdir_item active_del(struct hashdir *hd, int hdpos);

/* ydb_hashdir_frozen.c */
void frozen_free(struct hashdir *hd);
struct hashdir_item frozen_del(struct hashdir *hd, int hdpos);

