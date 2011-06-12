#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>

#include "ydb_common.h"
#include "ydb_record.h"


#define OFFSET_ALIGN 5
#define PADDING(v, p)				\
	(((p) - ((v) % (p))) & ((p)-1))
#define LOG_PADDING(sz)				\
	PADDING(sz, 1 << OFFSET_ALIGN);


struct _header {
	uint32_t magic;
	uint32_t key_sz;
	uint32_t key_sum;
	uint32_t value_sz;
	uint32_t value_sum;
}  __attribute__ ((packed));

struct iovec record_pack(struct record record)
{
	unsigned buf_sz = sizeof(struct _header) +
		record.key_sz + record.value_sz;
	unsigned padding_sz = LOG_PADDING(buf_sz);
	buf_sz += padding_sz;

	char *buf = malloc(buf_sz);
	char *b = buf;
	struct _header *header = (struct _header *)b;
	*header = (struct _header) {
		.magic = record.magic,
		.key_sz = record.key_sz,
		.key_sum = adler32(record.key, record.key_sz),
		.value_sz = record.value_sz,
		.value_sum = adler32(record.value, record.value_sz)
	};
	b += sizeof(struct _header);
	memcpy(b, record.key, record.key_sz);
	b += record.key_sz;
	memcpy(b, record.value, record.value_sz);
	b += record.value_sz;
	if (padding_sz) {
		memset(b, ' ', padding_sz);
		b += padding_sz;
	}
	return (struct iovec) {.iov_base = buf, .iov_len = buf_sz};
}

struct record record_unpack_force(struct iovec slot)
{
	char *b = slot.iov_base;
	assert(slot.iov_len >= sizeof(struct _header));
	struct _header *header = (struct _header *)b;
	assert(slot.iov_len >= sizeof(struct _header) +
	       header->key_sz + header->value_sz);
	b += sizeof(struct _header);
	return (struct record) {header->magic,
			b, header->key_sz,
			b + header->key_sz, header->value_sz};
}

int record_unpack(char *buffer, unsigned buffer_sz, struct record *record_ptr)
{
	char *b = buffer;
	struct _header *header = (struct _header *)b;
	b += sizeof(struct _header);
	if (buffer_sz < sizeof(uint32_t)) {
		return -2;
	}
	if (header->magic != YDB_LOG_SET && header->magic != YDB_LOG_DEL) {
		return -1;
	}

	if (buffer_sz < sizeof(struct _header)) {
		return -2;
	}
	if (buffer_sz < sizeof(struct _header) +
	    header->key_sz + header->value_sz) {
		return -2;
	}
	char *key = b;
	b += header->key_sz;
	char *value = b;
	b += header->value_sz;
	b += LOG_PADDING(b - buffer);
	if (adler32(key, header->key_sz) != header->key_sum) {
		/* fprintf(stderr, "a) %08x %08x\n", */
		/* 	adler32(key, header->key_sz), */
		/* 	header->key_sum); */
		return -3;
	}
	if (adler32(value, header->value_sz) != header->value_sum) {
		/* fprintf(stderr, "b) %08x %u %08x [%.*s]\n", */
		/* 	adler32(value, header->value_sz), */
		/* 	header->value_sz, */
		/* 	header->value_sum, */
		/* 	header->value_sz, value); */
		return -3;
	}
	*record_ptr = (struct record) {header->magic,
				       key, header->key_sz,
				       value, header->value_sz};
	__builtin_prefetch(b);
	return b - buffer;
}
