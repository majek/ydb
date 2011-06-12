int sys_pid_exist(int pid);

typedef void (*fork_callback)(void *userdata);
int sys_fork(fork_callback callback, void *userdata);

void linux_check_overcommit(struct db *db);
