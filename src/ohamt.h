#ifndef _OHAMT_H
#define _OHAMT_H
/* Custom HAMT implementation.
 *   - hash is 128 bits
 *   - items and pointers are 40 bits (39 actually, last bit is reserved)
 *
 *
 * Unless differently stated:
 *   hash - a pointer to 16 bytes forming the 128 bit hash.
 *   item - a pointer to user data.
 */

/* You need:
#include <stdint.h>
 */

#ifndef __uint128_t_defined
# define __uint128_t_defined
typedef __uint128_t uint128_t;
#endif

#ifndef unlikely
# define unlikely(x)     __builtin_expect((x),0)
#endif

struct ohamt_slot {
	uint64_t off:40;
} __attribute__ ((packed));

struct ohamt_node {
	uint64_t mask;
	struct ohamt_slot slots[];
};

struct ohamt_state{
	int level;
	uint128_t hash;
	struct ohamt_slot *ptr[23]; /* ptr root->slot and 22 levels more  */
};

struct ohamt_root {
	struct ohamt_slot slot;

	void *mem_context;
	void *(*mem_alloc)(void *mem_context, unsigned size);
	void (*mem_free)(void *mem_context, void *ptr, unsigned size);
        uint128_t (*hash)(void *ud, uint64_t item);
	void *hash_ud;
};




#define OHAMT_ROOT(mem_context, alloc, free, hash, hash_ud)		\
	(struct ohamt_root) {{0}, mem_context, alloc, free, hash, hash_ud}


/* Find the item that matches the hash.*/
uint64_t ohamt_search(struct ohamt_root *root, uint128_t hash);

/* Insert an item, unless there is one with the same hash already.
 * Return an item that matches the hash. */
uint64_t ohamt_insert(struct ohamt_root *root, uint64_t item);

/* Replaces an item with new value. Returns overwritten value. */
uint64_t ohamt_replace(struct ohamt_root *root, uint64_t new_item);

/* Remove the item. */
uint64_t ohamt_delete(struct ohamt_root *root, uint128_t hash);

/* Remove all items. */
void ohamt_delete_all(struct ohamt_root *root,
		      void (*fun)(void *ud, uint64_t item), void *ud);
/* Remove all items, without telling anyone. */
void ohamt_erase(struct ohamt_root *root);

uint64_t ohamt_first(struct ohamt_root *root, struct ohamt_state *s);

#define OHAMT_NOT_FOUND (0)

#endif // _OHAMT_H
