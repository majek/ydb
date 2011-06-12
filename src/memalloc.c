#define _POSIX_C_SOURCE 200112L

#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include "config.h"
#include "queue.h"
#include "list.h"

#ifdef VALGRIND
# warning "Compiling in valgrind hacks. This results in slow code."
# include <valgrind/valgrind.h>
# include <valgrind/memcheck.h>
# include <valgrind/helgrind.h>
#endif

#define MIN_CHUNK_SIZE 14
#define MAX_CHUNK_SIZE 328

#define MAX_CHUNKS (MAX_CHUNK_SIZE/2 - MIN_CHUNK_SIZE/2 + 1)


struct mem_cache {
	struct list_head list_of_free_pages;
};

struct mem_context {
	int pages_allocated;
	int page_size;
	struct mem_cache caches[MAX_CHUNKS];
};

struct mem_page {
	struct list_head in_list;
	struct queue_root queue_of_free_chunks;
	int free_chunks;
	int chunks_count;
};

struct mem_chunk {
	struct queue_head in_queue;
};


void *mem_context_new()
{
	void *ptr;
	if (posix_memalign(&ptr, CACHELINE_SIZE, sizeof(struct mem_context)) != 0) {
		abort();	/* Out of memory. */
	}
	struct mem_context *mc = (struct mem_context*)ptr;
	mc->pages_allocated = 0;
	mc->page_size = 4096;
	int i;
	for (i=0; i < MAX_CHUNKS; i++) {
		INIT_LIST_HEAD(&mc->caches[i].list_of_free_pages);
	}
	return mc;
}


static void mem_free_cached(struct mem_context *mc);

void mem_context_free(void *_mc)
{
	struct mem_context *mc = (struct mem_context*)_mc;
	mem_free_cached(mc);
	assert(mc->pages_allocated == 0);
	free(mc);
}

void mem_context_allocated(void *_mc,
			   unsigned long *allocated_ptr,
			   unsigned long *wasted_ptr)
{
	struct mem_context *mc = (struct mem_context*)_mc;
	if (allocated_ptr) {
		*allocated_ptr = mc->pages_allocated * mc->page_size;
	}
	unsigned long wasted = mc->pages_allocated * sizeof(struct mem_page);
	int i;
	for (i=0; i < MAX_CHUNKS; i++) {
		struct list_head *head;
		list_for_each(head, &mc->caches[i].list_of_free_pages) {
			struct mem_page *page =
				container_of(head, struct mem_page, in_list);
			unsigned sz = i*2 + MIN_CHUNK_SIZE;
			wasted += sz * page->free_chunks;
			wasted += (mc->page_size - sizeof(struct mem_page)) % sz;
		}
	}
	if (wasted_ptr) {
		*wasted_ptr = wasted;
	}
}

static inline struct mem_page *page_alloc(struct mem_context *mc, unsigned size)
{
	mc->pages_allocated ++;
	void *ptr;
	if (posix_memalign(&ptr, mc->page_size, mc->page_size) != 0) {
		abort();	/* Out of memory. */
	}

	struct mem_page *page = (struct mem_page*)ptr;
	INIT_LIST_HEAD(&page->in_list);
	INIT_QUEUE_ROOT(&page->queue_of_free_chunks);
	page->free_chunks = 0;
	page->chunks_count = 0;

	char *chunk_ptr = (char*)ptr + sizeof(struct mem_page);
	char *end = (char*)ptr + mc->page_size;

	#ifdef VALGRIND
	/* For valgrind, leave a word between chunks. */
	size += 2;
	/* And one after the header. */
	chunk_ptr += 2;
	#endif


	#ifdef VALGRIND
	VALGRIND_CREATE_MEMPOOL(page, 0, 0);
	VALGRIND_MAKE_MEM_NOACCESS(chunk_ptr, end - chunk_ptr);
	#endif

	//for (item=chunk_ptr; chunk_ptr <= end
	while (1) {
		char *item = chunk_ptr;
		chunk_ptr += size;
		if (unlikely(chunk_ptr > end)) {
			break;
		}

		struct mem_chunk *chunk = (struct mem_chunk*)item;
		#ifdef VALGRIND
		VALGRIND_MAKE_MEM_DEFINED(chunk, sizeof(struct mem_chunk));
		#endif
		queue_put(&chunk->in_queue, &page->queue_of_free_chunks);
		page->chunks_count ++;
	}
	page->free_chunks = page->chunks_count;
	return page;
}

static inline void page_free(struct mem_context *mc, struct mem_page *page)
{
	mc->pages_allocated --;

	#ifdef VALGRIND
	VALGRIND_DESTROY_MEMPOOL(page);
	#endif
	assert(page->chunks_count == page->free_chunks);
	free(page);
}

static inline int get_chunk_idx(unsigned size)
{
	return (size - MIN_CHUNK_SIZE)/2;
}

static inline struct mem_page *page_from_chunk(struct mem_context *mc,
					       struct mem_chunk *chunk)
{
	unsigned long v_page =						\
		(unsigned long)chunk & ~((unsigned long)mc->page_size-1);
	return (struct mem_page *)v_page;
}

void *mem_alloc(void *_mc, unsigned size)
{
	size = ((size + 1) / 2) * 2;
	struct mem_context *mc = (struct mem_context*)_mc;
	if (unlikely(size < MIN_CHUNK_SIZE || size > MAX_CHUNK_SIZE)) {
		assert(0);
		return malloc(size);
	}
	struct mem_cache *cache = &mc->caches[get_chunk_idx(size)];

	struct mem_page *page;
	if (unlikely(list_empty(&cache->list_of_free_pages))) {
		page = page_alloc(mc, size);
		list_add(&page->in_list, &cache->list_of_free_pages);
	} else {
		page = list_first_entry(&cache->list_of_free_pages,
					struct mem_page, in_list);
	}

	struct queue_head *qhead = queue_get(&page->queue_of_free_chunks);
	struct mem_chunk *chunk =				\
		container_of(qhead, struct mem_chunk, in_queue);

	page->free_chunks --;
	if (unlikely(page->free_chunks == 0)) {
		list_del(&page->in_list);
		INIT_LIST_HEAD(&page->in_list);
	}

	#ifdef VALGRIND
	VALGRIND_MEMPOOL_ALLOC(page, chunk, size);
	#endif
	return chunk;
}

void mem_free(void *_mc, void *ptr, unsigned size)
{
	size = ((size + 1) / 2) * 2;
	struct mem_context *mc = (struct mem_context*)_mc;
	if (unlikely(size < MIN_CHUNK_SIZE || size > MAX_CHUNK_SIZE)) {
		assert(0);
		free(ptr);
		return;
	}
	struct mem_cache *cache = &mc->caches[get_chunk_idx(size)];
	struct mem_chunk *chunk = ptr;
	struct mem_page *page = page_from_chunk(mc, chunk);

	#ifdef VALGRIND
	VALGRIND_MEMPOOL_FREE(page, chunk);
	VALGRIND_MAKE_MEM_DEFINED(chunk, sizeof(struct mem_chunk));
	#endif

	queue_put_head(&chunk->in_queue,
		       &page->queue_of_free_chunks);
	page->free_chunks ++;
	if (unlikely(page->free_chunks == 1 && list_empty(&page->in_list))) {
		list_add(&page->in_list,
			 &cache->list_of_free_pages);
	}
	if (unlikely(page->free_chunks == page->chunks_count)) {
		list_del(&page->in_list);
		if (list_empty(&cache->list_of_free_pages)) {
			/* TODO: more efficient? */
			list_add(&page->in_list,
				 &cache->list_of_free_pages);
		} else {
			INIT_LIST_HEAD(&page->in_list);
			page_free(mc, page);
		}
	}
}

static void mem_free_cached(struct mem_context *mc)
{
	int i;
	for (i=0; i < MAX_CHUNKS; i++) {
		struct list_head *head, *tmp;
		list_for_each_safe(head, tmp, &mc->caches[i].list_of_free_pages) {
			struct mem_page *page =				\
				container_of(head, struct mem_page, in_list);

			if (page->free_chunks == page->chunks_count) {
				list_del(&page->in_list);
				INIT_LIST_HEAD(&page->in_list);
				page_free(mc, page);
			}
		}
	}

}
