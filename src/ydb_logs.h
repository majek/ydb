struct logs;

struct logs *logs_new(struct db *db, unsigned slots);
void logs_free(struct logs *logs);
struct log *log_by_remno(struct logs *logs, uint64_t log_remno);
uint64_t log_to_remno(struct logs *logs, struct log *log);
uint64_t logs_new_number(struct logs *logs);
void logs_add(struct logs *logs, struct log *log);
void logs_del(struct logs *logs, struct log *log);

typedef int (*logs_callback)(void *context, struct log *log);
int logs_iterate(struct logs *logs, logs_callback callback, void *context);

struct log *logs_oldest(struct logs *logs);
struct log *logs_newest(struct logs *logs);
