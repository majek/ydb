#define _POSIX_SOURCE
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <openssl/md5.h>

#include "ydb.h"
#include "test_common.h"


static char *strip(char *buf)
{
	if (buf == NULL) return NULL;

	while (1) {
		if (*buf == ' ' || *buf == '\n' || *buf == '\t') {
			buf++;
		} else {
			break;
		}
	}
	int len = strlen(buf);
	char *last = &buf[len - 1];
	while (last >= buf) {
		if (*last == ' ' || *last == '\t' || *last == '\n') {
			*last = '\0';
			last--;
		} else {
			break;
		}
	}
	return buf;
}

int readlines(FILE *file, readlines_cb fun)
{
	char buf[1024*1024];
	int lineno = 0;
	while (fgets(buf, sizeof(buf), file)) {
		lineno ++;
		char *saveptr;
		char *line = strip(buf);
		char *action = strtok_r(line, " \t", &saveptr);
		if (action == NULL) {
			continue;
		}
		int tokc;
		char *tokv[32];

		for (tokc = 0; tokc < 32; tokc++) {
			tokv[tokc] = strtok_r(NULL, " \t", &saveptr);
			if (tokv[tokc] == NULL) {
				break;
			}
		}

		int r = fun(action, tokc, tokv);
		if (r) {
			fprintf(stderr, "Error %i on line %i\n", r, lineno);
			return r;
		}
	}
	return 0;
}

struct ydb *test_ydb_open(int argc, char **argv, struct ydb_options opt)
{
	assert(argc == 2);
	struct ydb *ydb = ydb_open(argv[1], &opt);
	assert(ydb);
	return ydb;
}


char *hex_md5(void *key_hash)
{
	static char buf[42];
	unsigned char *h = key_hash;
	char *b = buf;
	int i;
	for (i=0; i < 16; i++) {
		b += sprintf(b, "%02x", *h++);
	}
	return buf;
}

char *md5_str(const char *key, unsigned key_sz)
{
        unsigned char digest[20];

        MD5_CTX md5;
        MD5_Init(&md5);
        MD5_Update(&md5, key, key_sz);
        MD5_Final(digest, &md5);
	return hex_md5(&digest);
}
