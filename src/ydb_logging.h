#ifndef _YDB_LOGGING_H
#define _YDB_LOGGING_H

struct db;

#define log_info(db, format, ...)					\
	ydb_logger(db, __FILE__, __LINE__, "INFO", format, __VA_ARGS__)
#define log_warn(db, format, ...)					\
	ydb_logger(db, __FILE__, __LINE__, "WARN", format, __VA_ARGS__)
#define log_error(db, format, ...)					\
	ydb_logger(db, __FILE__, __LINE__, "ERROR", format, __VA_ARGS__)

#define log_perror(db, format, ...)					\
	ydb_logger_perror(db, __FILE__, __LINE__, format, __VA_ARGS__)

void ydb_logger(struct db *db, char *file, int line, const char *type,
		const char *fmt, ...) __attribute__ ((format (printf, 5, 6)));
void ydb_logger_perror(struct db *db, char *file, int line,
		       const char *fmt, ...) __attribute__ ((format (printf, 4, 5)));


#endif
