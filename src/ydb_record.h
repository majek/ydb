#define YDB_LOG_SET (0xADD0BEEF)
#define YDB_LOG_DEL (0xDE70BEEF)

struct record {
	uint32_t magic;
	const char *key;
	unsigned key_sz;
	const char *value;
	unsigned value_sz;
};

struct iovec record_pack(struct record record);
struct record record_unpack_force(struct iovec slot);
int record_unpack(char *buffer, unsigned buffer_sz, struct record *record_ptr);
