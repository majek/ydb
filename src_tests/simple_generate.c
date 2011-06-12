#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

char *key;
char *value;

static char *_rand_str(char *dst, int sz)
{
	int i;
	for (i=0; i < sz; i++) {
		dst[i] = 'a' + random() % 26;
	}
	return dst;
}

char *gen_key(unsigned key_sz)
{
	return _rand_str(key, key_sz);
}

char *gen_value(unsigned value_sz)
{
	if (value_sz > 6) {
		memset(&value[6], 'X', value_sz - 6);
		value_sz = 6;
	}
	return _rand_str(value, value_sz);
}

int main(int argc, char **argv)
{
	unsigned key_sz = 4;
	if (argc > 1) {
		key_sz = atoi(argv[1]);
		assert(key_sz > 0);
	}
	unsigned value_sz = 4;
	if (argc > 1) {
		value_sz = atoi(argv[2]);
	}
	key = malloc(key_sz + 1);
	value = malloc(value_sz + 1);

	fprintf(stderr, "key_sz=%u value_sz=%u\n", key_sz, value_sz);
	while (1) {
		switch(random() % 25) {
		case 0:
			printf("write\n");
			break;
		case 1:
		case 2:
		case 3:
		case 4:
			printf("del %s\n", gen_key(key_sz));
			break;
		default:
			printf("set %s %s\n", gen_key(key_sz),
			       gen_value(value_sz));
			break;
		}
	}
	return 0;
}
