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

#define MAX_STREAMS	64

struct stream {
	int			 rfd;
	int			 wfd;
	int			 done;
	psc_spinlock_t		 lock;
};

struct file {
	struct psc_hashent       hentry;
	uint64_t		 fid;
	int			 fd;
};

struct wkrthr {
	struct stream		*st;
};

struct rcvthr {
	struct stream		*st;
	char			 fnbuf[PATH_MAX];
	struct file		*last_f;
};

struct filehandle {
	struct psc_listentry	 lentry;
	void			*base;
	int			 fd;
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
	psync_is_master ? "[master %d] " fmt : "[puppet %d] " fmt, getpid()

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

char	**str_split(char *);
int	  parsenum(int *, const char *, int, int);
int	  parsesize(uint64_t *, const char *, uint64_t);

void	  rcvthr_main(struct psc_thread *);

void	  objns_makepath(char *, uint64_t);

ssize_t	  atomicio(int, int, void *, size_t);

#define fcache_search(fid)	_fcache_search((fid), -1)
#define fcache_insert(fid, fd)	_fcache_search((fid), (fd))

struct file *
	 _fcache_search(uint64_t, int);
void	  fcache_close(struct file *);
void	  fcache_init(void);
void	  fcache_destroy(void);

int	  getnstreams(int);
__dead void
	  usage(void);

int	  push_putfile_walkcb(const char *, const struct stat *, int,
	    int, void *);

void	  psync_chown(const char *, uid_t, gid_t, int);
void	  psync_chmod(const char *, mode_t, int);
void	  psync_utimes(const char *, const struct pfl_timespec *, int);

#define stream_sendv(st, opc, iov, nio)					\
	stream_sendxv((st), 0, (opc), (iov), (nio))

#define stream_send(st, opc, p, len)					\
	stream_sendx((st), 0, (opc), (p), (len))

struct stream	*stream_cmdopen(const char *, ...);
struct stream	*stream_create(int, int);
void		 stream_sendx(struct stream *, uint64_t, int, void *,
		    size_t);
void		 stream_sendxv(struct stream *, uint64_t, int,
		    struct iovec *, int);

extern struct psc_hashtbl	 fcache;

extern char			 objns_path[PATH_MAX];
extern int			 objns_depth;

extern volatile sig_atomic_t	 exit_from_signal;

extern int			 psync_is_master;
extern mode_t			 psync_umask;

extern struct psc_dynarray	 streams;

extern struct psc_poolmaster	 buf_poolmaster;
extern struct psc_poolmgr	*buf_pool;

extern struct psc_iostats	 iostats;

extern struct psc_dynarray	 wkrthrs;
extern struct psc_dynarray	 rcvthrs;

extern psc_spinlock_t		 wkrthrs_lock;
extern psc_spinlock_t		 rcvthrs_lock;

#endif /* _PSYNC_H_ */
