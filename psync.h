/* $Id$ */
/* %PSC_COPYRIGHT% */

#ifndef _PSYNC_H_
#define _PSYNC_H_

#define PSYNC_VERSION "1.0"

#include <signal.h>

#include "pfl/hashtbl.h"
#include "pfl/pthrutil.h"

struct stat;

struct psc_thread;

struct stream {
	int			 rfd;
	int			 wfd;
	struct pfl_mutex	 mut;
};

struct recvthr {
	struct stream		*st;
	char			 fnbuf[PATH_MAX];
};

struct file {
	struct psc_hashent       hentry;
	uint64_t		 fid;
	int			 fd;
};

struct xid_mapping {
	struct psc_hashent       hentry;
	uint64_t		 xid;
	const char		*fn;
};

struct filehandle {
	int			 fd;
	void			*base;
	int			 flags;
	int			 refcnt;
	psc_spinlock_t		 lock;
	struct psc_waitq	 wq;
};

#define FHF_DONE		(1 << 0)

struct buf {
	struct psc_listentry	 lentry;
	void			*buf;
	size_t			 len;
};

struct walkarg {
	const char *prefix;
	int trim;
};

#define push(da, ent)							\
	do {								\
		if (psc_dynarray_add((da), (ent)))			\
			psync_fatal("out of memory");			\
	} while (0)

#define psynclog_debug(fmt, ...)					\
	psclog_debug(psync_is_master ?					\
	    "[master] " fmt : "[puppet] " fmt, ##__VA_ARGS__)

#define psynclog_tdebug(fmt, ...)					\
	psclogs_debug(PSS_TMP, psync_is_master ?			\
	    "[master] " fmt : "[puppet] " fmt, ##__VA_ARGS__)

#define psynclog_warn(fmt, ...)						\
	psclog_warn(psync_is_master ?					\
	    "[master] " fmt : "[puppet] " fmt, ##__VA_ARGS__)

#define psynclog_warnx(fmt, ...)					\
	psclog_warnx(psync_is_master ?					\
	    "[master] " fmt : "[puppet] " fmt, ##__VA_ARGS__)

#define psynclog_error(fmt, ...)					\
	psclog_error(psync_is_master ?					\
	    "[master] " fmt : "[puppet] " fmt, ##__VA_ARGS__)

#define psynclog_errorx(fmt, ...)					\
	psclog_errorx(psync_is_master ?					\
	    "[master] " fmt : "[puppet] " fmt, ##__VA_ARGS__)

#define psync_fatal(fmt, ...)						\
	psc_fatal(psync_is_master ?					\
	    "[master] " fmt : "[puppet] " fmt, ##__VA_ARGS__)

#define IOP_READ	0
#define IOP_WRITE	1

#define atomicio_read(fd, buf, len)	atomicio(IOP_READ, (fd), (buf), (len))
#define atomicio_write(fd, buf, len)	atomicio(IOP_WRITE, (fd), (buf), (len))

char	**str_split(char *);
int	  parsenum(int *, const char *, int, int);
int	  parsesize(uint64_t *, const char *, uint64_t);

void	  recvthr_main(struct psc_thread *);

void	  objns_makepath(char *, uint64_t);

ssize_t	  atomicio(int, int, void *, size_t);

#define fcache_search(fid)	_fcache_search((fid), -1)
#define fcache_insert(fid, fd)	_fcache_search((fid), (fd))

int	 _fcache_search(uint64_t, int);
void	  fcache_close(uint64_t);
void	  fcache_init(void);
void	  fcache_destroy(void);

#define stream_sendv(st, opc, iov, nio)					\
	stream_sendxv((st), 0, (opc), (iov), (nio))

#define stream_send(st, opc, p, len)					\
	stream_sendx((st), 0, (opc), (p), (len))

struct stream	*stream_cmdopen(const char *, ...);
struct stream	*stream_create(int, int);
struct stream	*stream_get(void);
void		 stream_release(struct stream *);
void		 stream_sendx(struct stream *, uint64_t, int, void *,
		    size_t);
void		 stream_sendxv(struct stream *, uint64_t, int,
		    struct iovec *, int);

int		 push_putfile_walkcb(const char *, const struct stat *,
		    int, int, void *);

extern struct psc_hashtbl	 fcache;

extern char			 objns_path[PATH_MAX];
extern int			 objns_depth;

extern volatile sig_atomic_t	 exit_from_signal;

extern psc_atomic32_t		 psync_xid;
extern psc_atomic32_t		 psync_nrecvthr;
extern int			 psync_is_master;
extern int			 psync_rm_objns;
extern int			 psync_finished;

extern int			 opt_puppet;
extern int			 opt_recursive;
extern int			 opt_streams;

extern struct psc_dynarray	 streams;

extern struct psc_poolmaster	 buf_poolmaster;
extern struct psc_poolmgr	*buf_pool;

extern struct psc_iostats	 iostats;

#endif /* _PSYNC_H_ */
