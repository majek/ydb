#ifndef _MEMALLOC
#define _MEMALLOC

void *mem_context_new();
void mem_context_free(void *_mc);
void mem_context_allocated(void *_mc,
			   unsigned long *allocated_ptr,
			   unsigned long *wasted_ptr);

void *mem_alloc(void *_mc, unsigned size);
void mem_free(void *_mc, void *ptr, unsigned size);

#endif // _MEMALLOC
