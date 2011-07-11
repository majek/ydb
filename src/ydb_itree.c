#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "config.h"
#include "list.h"
#include "bitmap.h"

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_hashdir.h"
#include "ydb_itree.h"

#include "ohamt.h"

struct itree {
	struct ohamt_root tree;

	void *rlog_ctx;
	rlog_get rlog_get;
	rlog_add rlog_add;
	rlog_del rlog_del;
};


struct tree_item {
	uint64_t log_remno;
	int hpos;
};

struct _packed_tree_item {
	uint32_t _reserved2:1;
	uint32_t a_log_remno:16;
	uint32_t a_hpos:23;
	uint32_t _reserved1:20;
} __attribute__ ((packed));

union _pt_tree_item { 		/* Type-punning. */
	struct _packed_tree_item pti;
	uint64_t found;
} __attribute__ ((packed));



static uint64_t _pack(struct tree_item ti)
{
	union _pt_tree_item u = {.pti = {0, ti.log_remno, ti.hpos, 0}};
	return u.found;
}

static struct tree_item _unpack(uint64_t found)
{
	union _pt_tree_item u = {.found = found};
	return (struct tree_item){u.pti.a_log_remno, u.pti.a_hpos};
}

static uint128_t _itree_hash(void *itree_p, uint64_t found)
{
	struct itree *itree = (struct itree*)itree_p;
	struct tree_item ti = _unpack(found);
	struct hashdir_item hdi = itree->rlog_get(itree->rlog_ctx,
						  ti.log_remno, ti.hpos);
	return hdi.key_hash;
}

struct itree *itree_new(rlog_get get, rlog_add add, rlog_del del, void *ctx)
{
	struct itree *itree = malloc(sizeof(struct itree));
	memset(itree, 0, sizeof(struct itree));
	INIT_OHAMT_ROOT(&itree->tree, _itree_hash, itree);
	itree->rlog_ctx = ctx;
	itree->rlog_get = get;
	itree->rlog_add = add;
	itree->rlog_del = del;
	return itree;
}

void itree_free(struct itree *itree)
{
	ohamt_erase(&itree->tree);
	FREE_OHAMT_ROOT(&itree->tree);
	free(itree);
}

void itree_add(struct itree *itree, struct hashdir_item hdi)
{
	itree_del(itree, hdi.key_hash);

	struct tree_item ti;
	itree->rlog_add(itree->rlog_ctx, hdi, &ti.log_remno, &ti.hpos);

	uint64_t packed = _pack(ti);
	uint64_t found = ohamt_insert(&itree->tree, _pack(ti));
	assert(found == packed);
}

void itree_add_noidx(struct itree *itree, uint128_t key_hash,
		     uint64_t log_remno, int hpos)
{
	itree_del(itree, key_hash);
	struct tree_item ti = {log_remno, hpos};
	uint64_t packed = _pack(ti);
	uint64_t found = ohamt_insert(&itree->tree, packed);
	assert(found == packed);
}




void itree_move_callback(void *itree_p, uint64_t new_log_remno,
			 int new_hpos, int old_hpos)
{
	/* TODO: could be twice as fast - in-place */
	struct itree *itree = (struct itree*)itree_p;

	uint64_t t = _pack((struct tree_item){new_log_remno, new_hpos});
	uint64_t found = ohamt_replace(&itree->tree, t);
	assert(found);
	struct tree_item ti = _unpack(found);
	assert(ti.hpos == old_hpos);
	assert(ti.log_remno == new_log_remno);

	/* uint64_t found = ohamt_delete(&itree->tree, key_hash); */
	/* assert(found); */
	/* struct tree_item ti = _unpack(found); */
	/* assert(ti.hpos == old_hpos); */
	/* ti.hpos = new_hpos; */
	/* uint64_t t = _pack(ti); */
	/* found = ohamt_insert(&itree->tree, t); */
	/* assert(found == t); */
}

int itree_del(struct itree *itree, uint128_t key_hash)
{
	uint64_t found = ohamt_delete(&itree->tree, key_hash);
	if (found) {
		struct tree_item ti = _unpack(found);
		itree->rlog_del(itree->rlog_ctx, ti.log_remno, ti.hpos);
		return 1;
	}
	return 0;
}

int itree_get2(struct itree *itree, uint128_t key_hash,
	       uint64_t *log_remno_ptr, int *hpos_ptr)
{
	uint64_t found = ohamt_search(&itree->tree, key_hash);
	if (found) {
		struct tree_item ti = _unpack(found);
		*log_remno_ptr = ti.log_remno;
		*hpos_ptr = ti.hpos;
		return 1;
	}
	return 0;
}

void itree_mem_stats(struct itree *itree,
		     unsigned long *allocated_ptr, unsigned long *wasted_ptr)
{
	ohamt_allocated(&itree->tree, allocated_ptr, wasted_ptr);
}
