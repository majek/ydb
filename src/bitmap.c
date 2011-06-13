#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define DIV_ROUND_UP(n,d) (((n) + (d) - 1) / (d))


struct bitmap {
	uint64_t *map;
	int cnt;
};

int bitmap_get(struct bitmap *bm, int n)
{
	assert(n >0 && n < bm->cnt);
	return (bm->map[ n / 64 ] >> (n % 64)) & 0x1;
}

void bitmap_set(struct bitmap *bm, int n)
{
	assert(n >0 && n < bm->cnt);
	bm->map[ n / 64 ] |= 0x1ULL << (n % 64);
}

void bitmap_clear(struct bitmap *bm, int n)
{
	assert(n >0 && n < bm->cnt);
	bm->map[ n / 64 ] &= ~(0x1ULL << (n % 64));
}

struct bitmap *bitmap_new(int count, int initial)
{
       	struct bitmap *bm = malloc(sizeof(struct bitmap));
	bm->cnt = DIV_ROUND_UP(count, 64) * 64;
	bm->map = malloc(bm->cnt / 8);
	memset(bm->map, initial ? 0xFF : 0, bm->cnt / 8);
	if (initial == 0) {
		int i;
		for (i = count; i < bm->cnt; i++) {
			bitmap_set(bm, i);
		}
	}
	return bm;
}

void bitmap_free(struct bitmap *bm)
{
	free(bm->map);
	free(bm);
}

int bitmap_size(struct bitmap *bm)
{
	return bm->cnt;
}

char *bitmap_serialize(struct bitmap *bm, int *size_ptr)
{
	assert(bm->cnt % 8 == 0);
	*size_ptr = bm->cnt / 8;
	return (char*)bm->map;
}

struct bitmap *bitmap_new_from_blob(char *buf, int buf_sz)
{
	struct bitmap *bm = bitmap_new(buf_sz * 8, 0);
	assert(buf_sz % 8 == 0);
	assert(buf_sz * 8 == bm->cnt);
	memcpy(bm->map, buf, buf_sz);
	return bm;
}
