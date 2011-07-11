#define _POSIX_C_SOURCE 200809L	/* openat(2) */

#include <assert.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include "ydb_logging.h"
#include "ydb_db.h"

void ydb_logger(struct db *db, char *file, int line, const char *type,
		const char *fmt, ...)
{
	assert(fmt);
	char message[1024];
	va_list ap;
	va_start(ap, fmt);
	int message_len = vsnprintf(message, sizeof(message), fmt, ap);
	va_end(ap);

	char location[32];
	if (type[0] != 'I') {  	/* not info */
		char tmp_loc[32];
		snprintf(tmp_loc, sizeof(tmp_loc), "%s:%i", file, line);
		snprintf(location, sizeof(location), " %-18s", tmp_loc);
	} else {
		location[0] = '\0';
	}

	struct timeval tv;
	gettimeofday(&tv, NULL);

	struct tm tmp;
	gmtime_r(&tv.tv_sec, &tmp); // UTC

	char time[32];
	strftime(time, sizeof(time), "%Y-%m-%d %H:%M:%S", &tmp);

	char buf[1024];
	int buf_len = snprintf(buf, sizeof(buf),
			       "%s.%03li%s %-6s %.800s%s\n",
			       time, tv.tv_usec/1000,
			       location,
			       type,
			       message,
			       message_len > 800 ? "..." : "");

	int p = 0;
	while (p < buf_len) {
		int r = write(db_log_fd(db), &buf[p], buf_len - p);
		if (r > 0) {
			p += r;
		} else {
			break;
		}
	}
}


void ydb_logger_perror(struct db *db, char *file, int line,
		       const char *fmt, ...)
{
	assert(fmt);
	char *error = strerror(errno);

	char message[1024];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(message, sizeof(message), fmt, ap);
	va_end(ap);

	ydb_logger(db, file, line, "ERROR", "%s: %s", message, error);
}
