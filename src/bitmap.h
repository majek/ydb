struct bitmap;
int bitmap_get(struct bitmap *bm, int n);
void bitmap_set(struct bitmap *bm, int n);
void bitmap_clear(struct bitmap *bm, int n);
struct bitmap *bitmap_new(int count, int initial);
void bitmap_free(struct bitmap *bm);
int bitmap_size(struct bitmap *bm);

char *bitmap_serialize(struct bitmap *bm, int *size_ptr);

struct bitmap *bitmap_new_from_blob(char *buf, int buf_sz);
