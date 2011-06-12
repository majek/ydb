typedef int (*move_item_callback)(void *context, uint128_t key_hash,
				   int new_hpos);

typedef struct hashdir_item (*rlog_get)(void *context,
					uint64_t log_remno, int hpos);
typedef void (*rlog_add)(void *context, struct hashdir_item hdi,
			 uint64_t *log_remno_ptr, int *hpos_ptr);
typedef void (*rlog_del)(void *context, uint64_t log_remno, int hpos,
			 move_item_callback callback, void *context_cb);


struct itree;

struct itree *itree_new(rlog_get get, rlog_add add, rlog_del del, void *ctx);
void itree_free(struct itree *itree);
void itree_add(struct itree *itree, struct hashdir_item hdi);
void itree_add_noidx(struct itree *itree, uint128_t key_hash,
		     uint64_t log_remno, int hpos);

int itree_del(struct itree *itree, uint128_t key_hash);
int itree_get2(struct itree *itree, uint128_t key_hash,
	       uint64_t *log_remno_ptr, int *hpos_ptr);

void itree_mem_stats(struct itree *itree,
		     unsigned long *allocated_ptr, unsigned long *wasted_ptr);
