#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_file.h"
#include "ydb_hashdir.h"
#include "ydb_itree.h"

#include "memalloc.h"
#include "ohamt.h"

struct itree {
	void *mem_context;
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
	itree->mem_context = mem_context_new();
	itree->tree = OHAMT_ROOT(itree->mem_context, mem_alloc, mem_free,
				 _itree_hash, itree);
	itree->rlog_ctx = ctx;
	itree->rlog_get = get;
	itree->rlog_add = add;
	itree->rlog_del = del;
	return itree;
}

void itree_free(struct itree *itree)
{
	ohamt_erase(&itree->tree);
	mem_context_free(itree->mem_context);
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

static int _itree_move_callback(void *itree_p, uint128_t key_hash, int new_hpos)
{
	struct itree *itree = (struct itree*)itree_p;
	uint64_t found = ohamt_delete(&itree->tree, key_hash);
	assert(found);
	struct tree_item ti = _unpack(found);
	ti.hpos = new_hpos;
	uint64_t t = _pack(ti);
	found = ohamt_insert(&itree->tree, t);
	assert(found == t);
	return 0;
}

int itree_del(struct itree *itree, uint128_t key_hash)
{
	uint64_t found = ohamt_delete(&itree->tree, key_hash);
	if (found) {
		struct tree_item ti = _unpack(found);
		itree->rlog_del(itree->rlog_ctx, ti.log_remno, ti.hpos,
				_itree_move_callback, itree);
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
	mem_context_allocated(itree->mem_context, allocated_ptr, wasted_ptr);
}
