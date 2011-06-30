struct worker *worker_new(struct db *db);
void worker_free(struct worker *worker);

typedef void (*task_callback)(void *ud);
void worker_new_task(struct worker *worker,
		     task_callback callback, void *userdata);
void worker_new_answer(struct worker *worker,
		       task_callback callback, void *userdata);


void worker_sync(struct worker *worker);

int worker_do_answers(struct worker *worker);
