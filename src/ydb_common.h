#ifndef __uint128_t_defined
# define __uint128_t_defined
typedef __uint128_t uint128_t;
#endif

uint32_t adler32(const char *buf, uint32_t len);
uint128_t md5(const char *buf, unsigned int buf_sz);

struct keyvalue {
	const char *key;
	unsigned key_sz;
	const char *value;
	unsigned value_sz;
};

#define TIMEVAL_MSEC_SUBTRACT(a,b) ((((a).tv_sec - (b).tv_sec) * 1000) + ((a).tv_usec - (b).tv_usec) / 1000)
