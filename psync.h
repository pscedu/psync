/* $Id$ */
/* %PSC_COPYRIGHT% */

#ifndef _PSYNC_H_
#define _PSYNC_H_

struct stream {
	int		rfd;
	int		wfd;
	pthread_mutex_t	mut;
};

struct wkthr {
	struct stream	st;
};

#define push(da, ent)							\
	do {								\
		if (psc_dynarray_add((da), (ent)))			\
			err(1, NULL);					\
	} while (0)

#define IOP_READ	0
#define IOP_WRITE	1

#define atomic_read(fd, buf, len)	atomicio(IOP_READ, (fd), (buf), (len))
#define atomic_write(fd, buf, len)	atomicio(IOP_WRITE, (fd), (buf), (len))


#endif /* _PSYNC_H_ */