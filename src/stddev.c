#include <stdint.h>
#include <stdlib.h>
#include <math.h>

#include "stddev.h"


void stddev_get(struct stddev *sd, uint64_t *counter_ptr,
		double *avg_ptr, double *stddev_ptr) {
	double avg = 0.0, dev = 0.0;
	if(sd->count != 0) {
		if (avg_ptr || stddev_ptr) {
			avg = (double)sd->sum / (double)sd->count;
		}
		if (stddev_ptr) {
			double variance = ((double)sd->sum_sq /
					   (double)sd->count) - (avg*avg);
			dev = sqrt(variance);
		}
	}
	if (counter_ptr) {
		*counter_ptr = sd->count;
	}
	if (avg_ptr) {
		*avg_ptr = avg;
	}
	if (stddev_ptr) {
		*stddev_ptr = dev;
	}
}


void stddev_merge(struct stddev *dst, struct stddev *a, struct stddev *b)
{
	dst->count = a->count + b->count;
	dst->sum = a->sum + b->sum;
	dst->sum_sq = a->sum_sq + b->sum_sq;
}

void stddev_split(struct stddev *dst, struct stddev *a, struct stddev *b)
{
	dst->count = a->count - b->count;
	dst->sum = a->sum - b->sum;
	dst->sum_sq = a->sum_sq - b->sum_sq;
}
