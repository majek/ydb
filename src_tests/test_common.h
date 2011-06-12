#define streq(a, b) (strcmp((a),(b)) == 0)

typedef int (*readlines_cb)(char *action, int tokc, char **tokv);
int readlines(FILE *file, readlines_cb fun);

struct ydb *test_ydb_open(int argc, char **argv, struct ydb_options opt);

char *hex_md5(void *key_hash);
char *md5_str(const char *key, unsigned key_sz);
