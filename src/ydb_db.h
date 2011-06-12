struct db;

struct db *db_new(const char *directory);
struct db *db_new_mock();
void db_free(struct db *db);
int db_log_fd(struct db *db);
struct dir *db_log_dir(struct db *db);
struct dir *db_index_dir(struct db *db);


