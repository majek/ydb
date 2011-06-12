#ifndef _STDDEV_H
#define _STDDEV_H
/*
 * Basic statistics module: standard deviation routines.

#include <stdint.h>
 */

struct stddev {
	int64_t sum;
	int64_t sum_sq;
	uint64_t count;
};

#define STDDEV_INIT (struct stddev){0, 0, 0}
static inline void INIT_STDDEV(struct stddev *sd)
{
	*sd = STDDEV_INIT;
}

static inline void stddev_add(struct stddev *sd, int64_t value)
{
	sd->count++;
	sd->sum += value;
	sd->sum_sq += value * value;
}

static inline void stddev_remove(struct stddev *sd, int64_t old_value)
{
	sd->count--;
	sd->sum -= old_value;
	sd->sum_sq -= old_value * old_value;
}

static inline void stddev_modify(struct stddev *sd, int64_t old_value,
				 int64_t new_value)
{
	stddev_remove(sd, old_value);
	stddev_add(sd, new_value);
}

void stddev_get(struct stddev *sd, uint64_t *counter_ptr,
		double *avg_ptr, double *stddev_ptr);
void stddev_merge(struct stddev *dst, struct stddev *a, struct stddev *b);
void stddev_split(struct stddev *dst, struct stddev *a, struct stddev *b);



#endif // _STDDEV_H
