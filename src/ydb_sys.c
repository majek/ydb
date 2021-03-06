#define _POSIX_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "ydb_db.h"
#include "ydb_logging.h"
#include "ydb_sys.h"


int sys_pid_exist(int pid)
{
	/* Signal 0 has a special meaning. */
	int r = kill(pid, 0);
	if (r == -1) {
		return 0;
	} else {
		return 1;
	}
}

/* http://www.faqs.org/faqs/unix-faq/faq/part3/section-13.html
 *
 * One more thing -- if you don't want to go through all of this
 * trouble, there is a portable way to avoid this problem, although it
 * is somewhat less efficient.  Your parent process should fork, and
 * then wait right there and then for the child process to terminate.
 * The child process then forks again, giving you a child and a
 * grandchild.  The child exits immediately (and hence the parent
 * waiting for it notices its death and continues to work), and the
 * grandchild does whatever the child was originally supposed to.
 * Since its parent died, it is inherited by init, which will do
 * whatever waiting is needed.  This method is inefficient because it
 * requires an extra fork, but is pretty much completely portable.
 */
int sys_fork(fork_callback callback, void *userdata)
{
	callback(userdata);
	return 0;

	int pid = fork();
	if (pid == 0) {
		/* First child. */
		int pid2 = fork();
		if (pid == 0) {
			/* Grandchild. */
			callback(userdata);
			_exit(0);
		}
		_exit(pid2);
	}

	if (pid < 0) {
		return -1;
	}
	int status;
	if (waitpid(pid, &status, 0) == -1) {
		return -1;
	}
	return status;		/* Status has a pid of grandchild. */
}



/* Based on redis. */
static int _linux_get_overcommit()
{
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if (fp == NULL) return -1;
    char *r = fgets(buf, sizeof(buf), fp);
    fclose(fp);
    if (r) {
	    return atoi(buf);
    }
    return -1;
}

void linux_check_overcommit(struct db *db)
{
	if (_linux_get_overcommit() == 0) {
		log_warn(db, "WARNING overcommit_memory is disabled! Loading "
			 "large files may fail under low memory condition. To "
			 "fix this issue add 'vm.overcommit_memory = 1' to "
			 "/etc/sysctl.conf and then reboot or run the command "
			 "'sysctl vm.overcommit_memory=1'. %s", "");
	}
}
