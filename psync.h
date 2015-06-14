/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2015, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, modify, and distribute this software
 * for any purpose with or without fee is hereby granted, provided
 * that the above copyright notice and this permission notice
 * appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS.  IN NO EVENT SHALL
 * THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _PSYNC_H_
#define _PSYNC_H_

#include <signal.h>

#include "pfl/completion.h"
#include "pfl/fts.h"
#include "pfl/hashtbl.h"
#include "pfl/heap.h"
#include "pfl/pthrutil.h"

struct iovec;
struct stat;

struct psc_thread;

#define MAX_STREAMS		64

struct stream {
	int			 rfd;
	int			 wfd;
	int			 done;
	psc_spinlock_t		 lock;
};

/* reference to a file that is being received */
struct file {
	struct pfl_hashentry	 hentry;
	uint64_t		 fid;
	psc_spinlock_t		 lock;
	int			 fd;
	int			 refcnt;
	uint64_t		 nchunks_seen;
	uint64_t		 nchunks;
	struct pfl_timespec	 tim[2];	/* mtime/atime upon completion */
	uint32_t		 flags;
	mode_t			 mode;		/* permission modes upon completion */
};

#define FF_SAWLAST		(1 << 0)
#define FF_LINKED		(1 << 1)

struct wkrthr {
	struct stream		*st;
};

struct rcvthr {
	struct stream		*st;
	char			 fnbuf[PATH_MAX];
	struct file		*last_f;
};

/* a file that is being sent */
struct filehandle {
	struct psc_listentry	 lentry;
	struct psc_hashentry	 hentry;
	void			*base;
	int			 fd;
	int			 refcnt;
	uint64_t		 fid;
	psc_spinlock_t		 lock;
	struct psc_waitq	 wq;
	struct psc_compl	 cmpl;
	size_t			 len;
	struct pfl_heap		 done_heap;
	off_t			 done_off;
};

struct buf {
	struct psc_listentry	 lentry;
	void			*buf;
	size_t			 len;
};

struct walkarg {
	const char		*prefix;
	int			 skip;
	int			 rflags;
};

#define push(da, ent)							\
	do {								\
		if (psc_dynarray_add((da), (ent)))			\
			psync_fatal("out of memory");			\
	} while (0)

#define PSYNC_LOG_TAG(fmt)						\
	psync_is_master ? "[master] " fmt : "[puppet] " fmt

#define psynclog_max(fmt, ...)		psclog_max(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)
#define psynclog_diag(fmt, ...)		psclog_diag(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)
#define psynclog_warn(fmt, ...)		psclog_warn(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)
#define psynclog_warnx(fmt, ...)	psclog_warnx(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)
#define psynclog_error(fmt, ...)	psclog_error(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)
#define psynclog_errorx(fmt, ...)	psclog_errorx(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)
#define psync_fatal(fmt, ...)		psc_fatal(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)
#define psync_fatalx(fmt, ...)		psc_fatalx(PSYNC_LOG_TAG(fmt), ##__VA_ARGS__)

#define IOP_READ	0
#define IOP_WRITE	1

#define atomicio_read(fd, buf, len)	atomicio(IOP_READ, (fd), (buf), (len))
#define atomicio_write(fd, buf, len)	atomicio(IOP_WRITE, (fd), (buf), (len))

int	  parsenum(int *, const char *, int, int);
int	  parsesize(uint64_t *, const char *, uint64_t);

void	  rcvthr_main(struct psc_thread *);

void	  objns_makepath(char *, uint64_t);

ssize_t	  atomicio(int, int, void *, size_t);

struct file *
	  fcache_search(uint64_t);
void	  fcache_close(struct file *);
void	  fcache_init(void);
void	  fcache_destroy(void);

int	  getnstreams(int);
int	  getnprocessors(void);

__dead void
	  usage(void);

int	  push_putfile_walkcb(FTSENT *, void *);

void	  psync_chown(const char *, uid_t, gid_t, int);
void	  psync_chmod(const char *, mode_t, int);
void	  psync_utimes(const char *, const struct pfl_timespec *, int);

#define stream_sendv(st, opc, iov, nio)					\
	stream_sendxv((st), 0, (opc), (iov), (nio))

#define stream_send(st, opc, p, len)					\
	stream_sendx((st), 0, (opc), (p), (len))

struct stream *
	 stream_cmdopen(const char *, ...);
struct stream *
	 stream_create(int, int);
void	 stream_sendx(struct stream *, uint64_t, int, void *, size_t);
void	 stream_sendxv(struct stream *, uint64_t, int, struct iovec *, int);

struct filehandle *
	 filehandle_search(uint64_t);

extern struct psc_hashtbl	 fcache;

extern char			 objns_path[PATH_MAX];
extern int			 objns_depth;

extern volatile sig_atomic_t	 exit_from_signal;

extern int			 psync_is_master;
extern psc_atomic64_t		 psync_xid;
extern psc_atomic64_t		 psync_nfiles_xfer;
extern mode_t			 psync_umask;

extern struct psc_compl		 psync_ready;

extern struct psc_dynarray	 streams;

extern struct psc_poolmaster	 buf_poolmaster;
extern struct psc_poolmgr	*buf_pool;

extern struct psc_poolmgr	*filehandles_pool;

extern struct pfl_opstat	*iostats;

extern struct psc_dynarray	 wkrthrs;
extern struct psc_dynarray	 rcvthrs;

extern psc_spinlock_t		 wkrthrs_lock;
extern psc_spinlock_t		 rcvthrs_lock;

#endif /* _PSYNC_H_ */
