
/* for base */
struct frozen_list *frozen_list_new(struct db *db);
void frozen_list_free(struct frozen_list *fl);

/* for hashdir */
void frozen_list_add(struct frozen_list *fl, struct hashdir *hd);
void frozen_list_del(struct frozen_list *fl, struct hashdir *hd);
void frozen_list_incr(struct frozen_list *fl, struct hashdir *hd);


int frozen_list_marshall(struct frozen_list *fl);
int frozen_list_maybe_marshall(struct frozen_list *fl);
