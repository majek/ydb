#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <pthread.h>

#include "config.h"
#include "queue.h"

#include "ydb_common.h"
#include "ydb_logging.h"
#include "ydb_worker.h"


struct task {
	struct queue_head head;
	task_callback callback;
	void *userdata;
};

struct worker {
	struct db *db;
	pthread_t thread;

	pthread_mutex_t mutex;
	pthread_cond_t empty_cond;
	pthread_cond_t full_cond;

	struct queue_root tasks;
	struct queue_root answers;
};


static void _worker_thread(struct worker *worker);

static void *_thread(void *worker_p) {
	struct worker *worker = worker_p;
	_worker_thread(worker);
	return NULL;
}

struct worker *worker_new(struct db *db)
{
	struct worker *worker = malloc(sizeof(struct worker));
	memset(worker, 0, sizeof(struct worker));
	worker->db = db;
	INIT_QUEUE_ROOT(&worker->tasks);
	INIT_QUEUE_ROOT(&worker->answers);

	pthread_mutex_init(&worker->mutex, NULL);
	pthread_cond_init(&worker->full_cond, NULL);
	pthread_cond_init(&worker->empty_cond, NULL);

	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	int r = pthread_create(&worker->thread, &attr, _thread, worker);
	pthread_attr_destroy(&attr);
	if (r != 0) {
		errno = r;
		log_perror(worker->db, "pthread_create()%s", "");
		pthread_mutex_destroy(&worker->mutex);
		pthread_cond_destroy(&worker->full_cond);
		pthread_cond_destroy(&worker->empty_cond);
		free(worker);
		return NULL;
	}
	return worker;
}

void worker_new_task(struct worker *worker,
		     task_callback callback, void *userdata)
{
	struct task *task = malloc(sizeof(struct task));
	memset(task, 0, sizeof(struct task));
	task->callback = callback;
	task->userdata = userdata;

	pthread_mutex_lock(&worker->mutex);
	int r = queue_put(&task->head, &worker->tasks);
	if (r) {		/* was empty */
		pthread_cond_signal(&worker->full_cond);
	}
	pthread_mutex_unlock(&worker->mutex);
}

static void _worker_thread(struct worker *worker)
{
	while (1) {
		pthread_mutex_lock(&worker->mutex);
		if (queue_empty(&worker->tasks)) {
			pthread_cond_signal(&worker->empty_cond);
			pthread_cond_wait(&worker->full_cond,
					  &worker->mutex);
		}
		struct queue_head *head = queue_get(&worker->tasks);
		pthread_mutex_unlock(&worker->mutex);
		if (head == NULL) {
			continue;
		}
		struct task *task = container_of(head, struct task, head);
		struct task t = *task;
		free(task);
		t.callback(t.userdata);
	}
}

static void _thread_exit_job(void *ud)
{
	ud = ud;
	pthread_exit(NULL);
}

void worker_free(struct worker *worker)
{
	worker_new_task(worker, _thread_exit_job, NULL);
	int r = pthread_join(worker->thread, NULL);
	if (r) {
		errno = r;
		log_perror(worker->db, "pthread_join()%s", "");
	}
	pthread_mutex_destroy(&worker->mutex);
	pthread_cond_destroy(&worker->full_cond);
	pthread_cond_destroy(&worker->empty_cond);
	free(worker);
}

void worker_sync(struct worker *worker)
{
	pthread_mutex_lock(&worker->mutex);
	if(!queue_empty(&worker->tasks)) {
		pthread_cond_wait(&worker->empty_cond,
				  &worker->mutex);
	}
	pthread_mutex_unlock(&worker->mutex);
}


void worker_new_answer(struct worker *worker,
		       task_callback callback, void *userdata)
{
	struct task *answer = malloc(sizeof(struct task));
	memset(answer, 0, sizeof(struct task));
	answer->callback = callback;
	answer->userdata = userdata;

	pthread_mutex_lock(&worker->mutex);
	int r = queue_put(&answer->head, &worker->answers);
	if (r) {		/* was empty */
		pthread_cond_signal(&worker->full_cond);
	}
	pthread_mutex_unlock(&worker->mutex);
}

int worker_do_answers(struct worker *worker)
{
	int i = 0;
	while (1) {
		pthread_mutex_lock(&worker->mutex);
		struct queue_head *head = queue_get(&worker->answers);
		pthread_mutex_unlock(&worker->mutex);
		if (head == NULL) {
			break;
		}
		struct task *answer = container_of(head, struct task, head);
		struct task a = *answer;
		free(answer);
		a.callback(a.userdata);
		i++;
	}
	return i;
}
