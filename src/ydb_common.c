#include <stdint.h>
#include <openssl/md5.h>

#include "ydb_common.h"

/* Stolen from zlib.h. */
#define BASE 65521L /* largest prime smaller than 65536 */
#define NMAX 5552
/* NMAX is the largest n such that 255n(n+1)/2 + (n+1)(BASE-1) <= 2^32-1 */

#define DO1(buf,i)  {s1 += buf[i]; s2 += s1;}
#define DO2(buf,i)  DO1(buf,i); DO1(buf,i+1);
#define DO4(buf,i)  DO2(buf,i); DO2(buf,i+2);
#define DO8(buf,i)  DO4(buf,i); DO4(buf,i+4);
#define DO16(buf)   DO8(buf,0); DO8(buf,8);

uint32_t adler32(const char *buf, uint32_t len)
{
	uint32_t adler = 1;
	uint32_t s1 = adler & 0xffff;
	uint32_t s2 = (adler >> 16) & 0xffff;
	int k;

	while (len > 0) {
		k = len < NMAX ? len : NMAX;
		len -= k;
		while (k >= 16) {
			DO16(buf);
			buf += 16;
			k -= 16;
		}
		if (k != 0) do {
				s1 += *buf++;
				s2 += s1;
			} while (--k);
		s1 %= BASE;
		s2 %= BASE;
	}
	return (s2 << 16) | s1;
}


uint128_t md5(const char *buf, unsigned int buf_sz)
{
        unsigned char digest[20];

        MD5_CTX md5;
        MD5_Init(&md5);
        MD5_Update(&md5, buf, buf_sz);
        MD5_Final(digest, &md5);

	uint128_t *a = (uint128_t*)digest;
        return *a;
}
