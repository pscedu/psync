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

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/syscall.h>
#include <sys/un.h>

#include <ctype.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <sched.h>
#include <paths.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <term.h>
#include <unistd.h>

#include "pfl/alloc.h"
#include "pfl/cdefs.h"
#include "pfl/fmt.h"
#include "pfl/hashtbl.h"
#include "pfl/heap.h"
#include "pfl/opstats.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/net.h"
#include "pfl/pfl.h"
#include "pfl/pool.h"
#include "pfl/random.h"
#include "pfl/stat.h"
#include "pfl/str.h"
#include "pfl/thread.h"
#include "pfl/timerthr.h"
#include "pfl/types.h"
#include "pfl/umask.h"
#include "pfl/walk.h"

#include "options.h"
#include "psync.h"
#include "rpc.h"

#define PSYNC_BLKSZ	(64 * 1024)

#define MODE_GET	0
#define MODE_PUT	1

struct work {
	struct psc_listentry	  wk_lentry;
	struct pfl_heap_entry	  wk_hpentry;
	char			  wk_fn[PATH_MAX];
	char			  wk_basefn[NAME_MAX + 1];
	struct filehandle	 *wk_fh;
	char			 *wk_buf;
	char			  wk_host[PFL_HOSTNAME_MAX];
	int			  wk_type;
	int			  wk_rflags;
	size_t			  wk_len;
	struct stat		  wk_stb;
	uint64_t		  wk_xid;
	uint64_t		  wk_fid;
	uint64_t		  wk_nchunks;
	off_t			  wk_off;
};

struct ino_entry {
	uint64_t		  i_fid;
	struct psc_hashentry	  i_hentry;
};

enum {
	THRT_DISP,
	THRT_DONE,
	THRT_MAIN,
	THRT_RCV,
	THRT_OPSTIMER,
	THRT_WKR
};

const char		*progname;

struct psc_poolmaster	 buf_poolmaster;
struct psc_poolmgr	*buf_pool;

struct psc_poolmaster	 filehandles_poolmaster;
struct psc_poolmgr	*filehandles_pool;
struct psc_hashtbl	 filehandles_hashtbl;

struct psc_listcache	 workq;
struct psc_listcache	 psync_doneq;
struct psc_poolmaster	 work_poolmaster;
struct psc_poolmgr	*work_pool;

struct psc_hashtbl	 ino_hashtbl;

struct pfl_opstat	*iostats;

struct psc_dynarray	 streams = DYNARRAY_INIT;

int			 psync_is_master;	/* otherwise, is RPC puppet */
mode_t			 psync_umask;

struct psc_dynarray	 wkrthrs = DYNARRAY_INIT;
struct psc_dynarray	 rcvthrs = DYNARRAY_INIT;

psc_spinlock_t		 wkrthrs_lock = SPINLOCK_INIT;
psc_spinlock_t		 rcvthrs_lock = SPINLOCK_INIT;

psc_atomic64_t		 nbytes_total = PSC_ATOMIC64_INIT(0);
psc_atomic64_t		 nbytes_xfer = PSC_ATOMIC64_INIT(0);

psc_atomic64_t		 psync_offset_index = PSC_ATOMIC64_INIT(0);
psc_atomic64_t		 psync_fid = PSC_ATOMIC64_INIT(0);	/* file ID (fake inumber) */
psc_atomic64_t		 psync_nfiles_xfer = PSC_ATOMIC64_INIT(0);

struct psc_compl	 psync_ready = PSC_COMPL_INIT;

unsigned char		 psync_authbuf[AUTH_LEN];

struct filehandle *
filehandle_search(uint64_t fid)
{
	return (psc_hashtbl_search(&filehandles_hashtbl, &fid));
}

int
filehandle_dropref(struct filehandle *fh)
{
	int rc = 0;

	spinlock(&fh->lock);
	if (--fh->refcnt == 0) {
		if (fh->len)
			munmap(fh->base, fh->len);
		psynclog_diag("close fd=%d", fh->fd);
		close(fh->fd);
		psc_hashent_remove(&filehandles_hashtbl, fh);
		psc_assert(pfl_heap_nitems(&fh->done_heap) == 0);
		psc_pool_return(filehandles_pool, fh);
		rc = 1;
	} else
		freelock(&fh->lock);
	return (rc);
}

int
wk_heapcmp(const void *a, const void *b)
{
	const struct work *x = a, *y = b;

	return (CMP(x->wk_off, y->wk_off));
}

struct filehandle *
filehandle_new(uint64_t fid, size_t len)
{
	struct filehandle *fh;

	fh = psc_pool_get(filehandles_pool);
	if (fh == NULL)
		return (NULL);
	memset(fh, 0, sizeof(*fh));
	INIT_SPINLOCK(&fh->lock);
	INIT_LISTENTRY(&fh->lentry);
	pfl_heap_init(&fh->done_heap, struct work, wk_hpentry,
	    wk_heapcmp);
	fh->refcnt++;
	fh->fid = fid;
	fh->len = len;
	psc_hashent_init(&filehandles_hashtbl, fh);
	psc_hashtbl_add_item(&filehandles_hashtbl, fh);
	return (fh);
}

int
count_pages(unsigned char *buf, size_t len)
{
	unsigned char *p;
	int np = 0;

	for (p = buf; p < buf + len; p++)
		if (*p & 1)
			np++;
	return (np);
}

void
donethr_main(struct psc_thread *thr)
{
	struct filehandle *fh;
	struct work *wk;
	void *offp;
	int done;

	while (pscthr_run(thr)) {
		wk = lc_getwait(&psync_doneq);
		if (wk == NULL)
			break;
		fh = wk->wk_fh;
		pfl_heap_add(&fh->done_heap, wk);

		do {
			wk = pfl_heap_peek(&fh->done_heap);
			if (wk == NULL ||
			    wk->wk_off != fh->done_off)
				break;
			pfl_heap_remove(&fh->done_heap, wk);
			offp = fh->base + wk->wk_off;
			posix_madvise(offp, wk->wk_len,
			    POSIX_MADV_DONTNEED);
			fh->done_off = wk->wk_off + wk->wk_len;
			done = filehandle_dropref(fh);
			psc_pool_return(work_pool, wk);
		} while (!done);
	}
}

void
wkrthr_main(struct psc_thread *thr)
{
	unsigned char *vbuf = NULL;
	struct wkrthr *wkrthr = thr->pscthr_private;
	struct stream *st = wkrthr->st;
	struct work *wk;
	FILE *logfp;

	logfp = opts.offset_log_fp;
	if (logfp) {
		long pgsz, n;

		pgsz = sysconf(_SC_PAGESIZE);
		n = howmany(PSYNC_BLKSZ, pgsz);
		vbuf = PSCALLOC(n);
	}

	while (pscthr_run(thr)) {
		wk = lc_getwait(&workq);
		if (wk == NULL)
			break;

		switch (wk->wk_type) {
		case OPC_GETFILE_REQ:
			rpc_send_getfile(st, wk->wk_xid, wk->wk_fn,
			    wk->wk_basefn);
			break;
		case OPC_PUTDATA:
#if 0
			if (opts.partial) {
				psc_compl_wait(&wk->wk_fh->cmpl);
				if (checksum) {
					wk = work_getitem(OPC_GETCKSUM_REQ);
					wk->wk_off = off;
					wk->wk_len = ;
					lc_add(&workq, wk);
					return;
				}
				wk = work_getitem(OPC_GETSTAT_REQ);
				wk->wk_off = off;
				wk->wk_len = ;
				lc_add(&workq, wk);
				return;
			}
#endif

			if (opts.sparse == 0 ||
			    !pfl_memchk(wk->wk_fh->base + wk->wk_off, 0,
			    wk->wk_len)) {
				void *offp;

				offp = wk->wk_fh->base + wk->wk_off;
				if (logfp) {
					mincore(offp, wk->wk_len, vbuf);
					flockfile(logfp);
					fprintf(logfp, "%"PRId64"\t"
					    "%"PRId64"\t%d\n",
					    psc_atomic64_inc_getnew(
					    &psync_offset_index),
					    wk->wk_off,
					    count_pages(vbuf,
					    howmany(PSYNC_BLKSZ,
					    wk->wk_len)));
					funlockfile(logfp);
				}
				posix_madvise(offp, wk->wk_len * 2000,
				    POSIX_MADV_WILLNEED);
				rpc_send_putdata(st, wk->wk_fid,
				    wk->wk_off, offp, wk->wk_len,
				    wk->wk_rflags);
			}
			psc_atomic64_add(&nbytes_xfer, wk->wk_len);

			lc_addtail_ifalive(&psync_doneq, wk);
			wk = NULL;

			break;
		case OPC_PUTNAME_REQ:
			rpc_send_putname_req(st, wk->wk_fid, wk->wk_fn,
			    &wk->wk_stb, wk->wk_buf, wk->wk_nchunks,
			    wk->wk_rflags);
			PSCFREE(wk->wk_buf);
			break;
		}

		if (wk)
			psc_pool_return(work_pool, wk);

		if (exit_from_signal)
			break;
	}

	PSCFREE(vbuf);

	rpc_send_done(st);

	psynclog_diag("wkrthr done, close fd=%d", st->wfd);
	close(st->wfd);

	spinlock(&wkrthrs_lock);
	psc_dynarray_removeitem(&wkrthrs, thr);
	freelock(&wkrthrs_lock);
}

struct work *
work_getitem(int type)
{
	struct work *wk;

	wk = psc_pool_get(work_pool);
	memset(wk, 0, sizeof(*wk));
	INIT_LISTENTRY(&wk->wk_lentry);
	wk->wk_type = type;
	return (wk);
}

int
seen_fid(ino_t fid)
{
	struct psc_hashbkt *b;
	struct ino_entry *i;
	uint64_t kfid = fid;
	int seen = 1;

	b = psc_hashbkt_get(&ino_hashtbl, &kfid);
	i = psc_hashbkt_search(&ino_hashtbl, b, &kfid);
	if (i == NULL) {
		i = PSCALLOC(sizeof(*i));
		i->i_fid = kfid;
		psc_hashent_init(&ino_hashtbl, i);
		psc_hashbkt_add_item(&ino_hashtbl, b, i);
		seen = 0;
	}
	psc_hashbkt_put(&ino_hashtbl, b);
	return (seen);
}

/*
 * @stb: stat(2) buffer only used during PUTs.
 */
void
enqueue_put(const char *srcfn, const char *dstfn,
    const struct stat *stb, int rflags)
{
	struct filehandle *fh;
	struct work *wk;
	off_t off = 0;
	uint64_t fid;
	size_t blksz;

	blksz = opts.block_size ? (blksize_t)opts.block_size :
	    stb->st_blksize;
blksz = PSYNC_BLKSZ;

	if (S_ISLNK(stb->st_mode)) {
		if (!opts.links)
			return;
	} else if (S_ISCHR(stb->st_mode) || S_ISBLK(stb->st_mode)) {
		if (!opts.devices)
			return;
	} else if (S_ISSOCK(stb->st_mode) || S_ISFIFO(stb->st_mode)) {
		if (!opts.specials)
			return;
	}

	if (opts.links)
		fid = stb->st_ino;
	else
		fid = psc_atomic64_inc_getnew(&psync_fid);

	/* sending; push name first */
	wk = work_getitem(OPC_PUTNAME_REQ);
	wk->wk_fid = fid;
	memcpy(&wk->wk_stb, stb, sizeof(wk->wk_stb));
	strlcpy(wk->wk_fn, dstfn, sizeof(wk->wk_fn));
	wk->wk_rflags = rflags;
	if (S_ISLNK(stb->st_mode)) {
		int rc;

		wk->wk_buf = PSCALLOC(PATH_MAX);
		rc = readlink(srcfn, wk->wk_buf, PATH_MAX - 1);
		if (rc == -1) {
			psynclog_error("readlink %s", wk->wk_fn);
			wk->wk_buf[0] = '\0';
		} else
			wk->wk_buf[rc] = '\0';
	}
	psynclog_diag("enqueue PUTNAME_REQ localfn=%s dstfn=%s flags=%d",
	    srcfn, wk->wk_fn, rflags);

	psc_atomic64_inc(&psync_nfiles_xfer);

	if (!S_ISREG(stb->st_mode) ||
	    (opts.links && seen_fid(fid))) {
		lc_add(&workq, wk);
		pscthr_yield();
		return;
	}

	fh = filehandle_new(fid, stb->st_size);
	/*
	 * If exiting due to reception of SIGHUP, the pool will be dead.
	 */
	if (fh == NULL)
		return;

	if (opts.partial)
		psc_compl_init(&fh->cmpl);

	wk->wk_nchunks = howmany(stb->st_size, blksz);
	lc_add(&workq, wk);

	fh->fd = open(srcfn, O_RDONLY);
	if (fh->fd == -1)
		err(1, "%s", srcfn);

	if (stb->st_size) {
		fh->base = mmap(NULL, stb->st_size, PROT_READ,
		    MAP_FILE | MAP_PRIVATE, fh->fd, 0);
		if (fh->base == MAP_FAILED)
			err(1, "mmap %s", srcfn);
		posix_madvise(fh->base, stb->st_size,
		    POSIX_MADV_SEQUENTIAL);

	}

	psc_atomic64_add(&nbytes_total, stb->st_size);

	/*
	 * Now push data chunks from the file onto a work queue consumed
	 * by worker threads.
	 */
	for (; off < stb->st_size; off += blksz) {
		wk = work_getitem(OPC_PUTDATA);
		wk->wk_fh = fh;

		wk->wk_fid = fid;
		wk->wk_stb.st_size = stb->st_size;

		/* Mark EOF. */
		if (off + (off_t)blksz >= stb->st_size)
			wk->wk_rflags |= RPC_PUTDATA_F_LAST;

		spinlock(&fh->lock);
		fh->refcnt++;
		freelock(&fh->lock);

		wk->wk_off = off;
		if (off + (off_t)blksz > stb->st_size)
			wk->wk_len = stb->st_size % blksz;
		else
			wk->wk_len = blksz;
		lc_add(&workq, wk);

		pscthr_yield();
	}
	filehandle_dropref(fh);
}

int
push_putfile_walkcb(FTSENT *f, void *arg)
{
	struct walkarg *wa = arg;
	char dstfn[PATH_MAX];
	const char *t;
	int rc = 0;

	if (exit_from_signal)
		return (-1);

#if 0
	struct filterpat *fp;

	ok = 1;
	DYNARRAY_FOREACH(fp, j, &opts.filter) {
		if ()
			ok = ;
	}
	if (!ok) {
		fts_prune;
		return;
	}
#endif
	t = f->fts_path + wa->skip;
	while (*t == '/')
		t++;
	rc = snprintf(dstfn, sizeof(dstfn), "%s%s%s", wa->prefix, t[0] ?
	    "/" : "", t);
	if (rc == -1)
		psync_fatal("snprintf");
///	if (f->fts_level == 0)
//		strlcat(dstfn, pfl_basename(fn), sizeof(dstfn));

	if (f->fts_level > 0)
		wa->rflags &= ~RPC_PUTNAME_F_TRYDIR;

	enqueue_put(f->fts_path, dstfn, f->fts_statp, wa->rflags);
	return (0);
}

/*
 * put:
 *	psync file remote:dir/file
 *	psync file remote:dir/
 *	psync dir remote:dir/file
 *	psync dir remote:dir/
 * get:
 *	psync remote:file dir/file
 *	psync remote:file dir/
 *	psync remote:dir dir/file
 *	psync remote:dir dir/
 */
int
walkfiles(int mode, const char *srcfn, int travflags, int rflags,
    const char *dstfn)
{
	char buf[PATH_MAX];
	const char *finalfn;
	struct stat tstb;
	struct work *wk;
	int rc;

	if (mode == MODE_PUT) {
		struct walkarg wa;
		char *p;

		p = strrchr(srcfn, '/');
		if (p)
			wa.skip = p - srcfn;
		else
			wa.skip = 0;
		wa.rflags = rflags;
		wa.prefix = dstfn;
		return (pfl_filewalk(srcfn, travflags, NULL,
		    push_putfile_walkcb, &wa));
	}

	/* otherwise, the operation is a FETCH */

	/*
	 * If destination is local and a directory, append
	 * remote source filename.
	 *
	 *	psync remote:file file
	 *	psync remote:file dir
	 *	psync remote:dir file
	 *	psync remote:dir dir
	 */
	finalfn = dstfn;
	if (stat(dstfn, &tstb) == 0 && S_ISDIR(tstb.st_mode)) {
		rc = snprintf(buf, sizeof(buf), "%s/%s", finalfn,
		    pfl_basename(srcfn));
		if (rc == -1)
			psync_fatal("snprintf");
		finalfn = buf;
	}

	wk = work_getitem(OPC_GETFILE_REQ);
	wk->wk_xid = psc_atomic64_inc_getnew(&psync_xid);
	strlcpy(wk->wk_fn, srcfn, sizeof(wk->wk_fn));
	strlcpy(wk->wk_basefn, finalfn, sizeof(wk->wk_basefn));
//	if (!opts.partial)
//		truncate(finalfn, 0);
	lc_add(&workq, wk);

	return (0);
}

int
filesfrom(int mode, const char *fromfn, int travflags,
    const char *dstfn)
{
	char fn[PATH_MAX], *p = fn;
	int rc = 0, rv, c, lineno = 1;
	FILE *fp;

	fp = fopen(fromfn, "r");
	if (fp == NULL)
		psync_fatal("open %s", fromfn);
	for (;;) {
		c = fgetc(fp);
		if (c == EOF)
			break;
		if (c == '\n' || c == '\r') {
			lineno++;
			*p = '\0';
			if (p != fn) {
				rv = walkfiles(mode, fn, travflags, 0,
				    dstfn);
				if (rv)
					rc = rv;
			}
			p = fn;
		} else {
			if (p == fn + sizeof(fn) - 1) {
				errno = ENAMETOOLONG;
				psynclog_warn("%s:%d", fromfn, lineno);
			} else
				*p++ = c;
		}
	}
	fclose(fp);
	if (p != fn) {
		*p = '\0';
		rv = walkfiles(mode, fn, travflags, 0, dstfn);
		if (rv)
			rc = rv;
	}
	return (rc);
}

union ctlmsg {
	struct cmsghdr		hdr;
	unsigned char		buf[CMSG_SPACE(sizeof(int))];
};

void
send_fd(int s, int fd)
{
	struct cmsghdr *c;
	struct iovec iov;
	struct msghdr m;
	union ctlmsg cm;
	ssize_t nb;
	int *pfd;
	char ch;

	memset(&m, 0, sizeof(m));
	iov.iov_base = &ch;
	iov.iov_len = 1;
	m.msg_iov = &iov;
	m.msg_iovlen = 1;
	m.msg_control = cm.buf;
	m.msg_controllen = sizeof(cm.buf);

	c = CMSG_FIRSTHDR(&m);
	c->cmsg_len = CMSG_LEN(sizeof(int));
	c->cmsg_level = SOL_SOCKET;
	c->cmsg_type = SCM_RIGHTS;

	pfd = (void *)CMSG_DATA(c);
	*pfd = fd;

	nb = sendmsg(s, &m, MSG_WAITALL);
	if (nb == -1)
		psync_fatal("sendmsg");
}

int
recv_fd(int s)
{
	struct cmsghdr *c;
	struct iovec iov;
	struct msghdr m;
	union ctlmsg cm;
	ssize_t rc;
	char ch;

	memset(&m, 0, sizeof(m));
	iov.iov_base = &ch;
	iov.iov_len = 1;
	m.msg_iov = &iov;
	m.msg_iovlen = 1;
	m.msg_control = cm.buf;
	m.msg_controllen = sizeof(cm.buf);

	for (;;) {
		rc = recvmsg(s, &m, MSG_WAITALL);
		if (rc == -1) {
			if (errno == EINTR)
				continue;
			psync_fatal("recvmsg");
		}
		break;
	}
	if (rc == 0)
		psync_fatalx("recvmsg: unexpected EOF");
	if (m.msg_flags & MSG_TRUNC ||
	    m.msg_flags & MSG_CTRUNC)
		psync_fatalx("recvmsg: received truncated message");

	for (c = CMSG_FIRSTHDR(&m); c; c = CMSG_NXTHDR(&m, c))
		if (c->cmsg_len == CMSG_LEN(sizeof(int)) &&
		    c->cmsg_level == SOL_SOCKET &&
		    c->cmsg_type == SCM_RIGHTS) {
			int *pfd;

			pfd = (void *)CMSG_DATA(c);
			return (*pfd);
		}
	psync_fatalx("no fd found in ctlmsg");
	return (-1); /* gcc */
}

int
recv_auth(int fd, unsigned char *p)
{
	return (atomicio_read(fd, p, AUTH_LEN) == AUTH_LEN);
}

void
send_auth(int fd, unsigned char *buf)
{
	if (atomicio_write(fd, buf, AUTH_LEN) != AUTH_LEN)
		psc_fatalx("short I/O");
}

int
puppet_limb_mode(void)
{
	struct sockaddr_un sun;
	int s, rc;
	char ch;

	s = socket(AF_LOCAL, SOCK_STREAM, 0);
	if (s == -1)
		psync_fatal("socket");

	memset(&sun, 0, sizeof(sun));
	sun.sun_family = AF_LOCAL;
	SOCKADDR_SETLEN(&sun);
	rc = snprintf(sun.sun_path, sizeof(sun.sun_path),
	    "%s/.psync.%d.sock", _PATH_TMP, opts.puppet);
	if (rc == -1)
		psync_fatal("snprintf");
	if (rc < 1 || (size_t)rc > sizeof(sun.sun_path))
		psync_fatalx("snprintf: invalid path");

	for (;;) {
		if (connect(s, (struct sockaddr *)&sun,
		    sizeof(sun)) == -1) {
			if (errno == ENOENT) {
				usleep(1000);
				continue;
			}
			psync_fatal("connect: %s", sun.sun_path);
		}
		break;
	}

	if (!recv_auth(STDIN_FILENO, psync_authbuf))
		psync_fatal("no auth received");

	send_auth(s, psync_authbuf);
	send_fd(s, 0);
	send_fd(s, 1);

	do
		rc = read(s, &ch, 1);
	while (rc == -1 && errno == EINTR);

	return (0);
}

void
spawn_worker_threads(struct stream *st)
{
	struct psc_thread *thr;
	struct wkrthr *wkrthr;
	struct rcvthr *rcvthr;
	int n;

	n = psc_dynarray_len(&streams);

	thr = pscthr_init(THRT_WKR, wkrthr_main, NULL, sizeof(*wkrthr),
	    "wkrthr%d", n);
	wkrthr = thr->pscthr_private;
	wkrthr->st = st;
	pscthr_setready(thr);

	spinlock(&wkrthrs_lock);
	push(&wkrthrs, thr);
	freelock(&wkrthrs_lock);

	thr = pscthr_init(THRT_RCV, rcvthr_main, NULL, sizeof(*rcvthr),
	    "rcvthr%d", n);
	rcvthr = thr->pscthr_private;
	rcvthr->st = st;
	pscthr_setready(thr);

	spinlock(&rcvthrs_lock);
	push(&rcvthrs, thr);
	freelock(&rcvthrs_lock);
}

int
puppet_head_mode(void)
{
	unsigned char tbuf[AUTH_LEN];
	int i, rc, clifd, s, rfd, wfd;
	struct psc_dynarray puppet_strings = DYNARRAY_INIT;
	struct sockaddr_un sun;
	struct stream *st;
	mode_t old_umask;
	void *p;

	s = socket(AF_LOCAL, SOCK_STREAM, 0);
	if (s == -1)
		psync_fatal("socket");

	memset(&sun, 0, sizeof(sun));
	sun.sun_family = AF_LOCAL;
	SOCKADDR_SETLEN(&sun);
	rc = snprintf(sun.sun_path, sizeof(sun.sun_path),
	    "%s/.psync.%d.sock", _PATH_TMP, opts.puppet);
	if (rc == -1)
		psync_fatal("snprintf");
	if (rc < 1 || (size_t)rc > sizeof(sun.sun_path))
		psync_fatalx("snprintf: invalid path");

	if (unlink(sun.sun_path) == -1 && errno != ENOENT)
		psynclog_error("unlink %s", sun.sun_path);

	spinlock(&psc_umask_lock);
	old_umask = umask(_S_IXUGO | S_IRWXG | S_IRWXO);
	if (bind(s, (struct sockaddr *)&sun, sizeof(sun)) == -1)
		psync_fatal("bind %s", sun.sun_path);
	umask(old_umask);
	freelock(&psc_umask_lock);

	if (chmod(sun.sun_path, S_IRUSR | S_IWUSR) == -1)
		psync_fatal("chmod %s", sun.sun_path);

#define QLEN	(opts.streams * 2)
	if (listen(s, 128) == -1)
		psync_fatal("listen");
	psynclog_diag("listening on %s", sun.sun_path);

	if (chdir(opts.dstdir) == -1) {
		char *sep;

		if (errno != EEXIST)
			psync_fatal("%s", opts.dstdir);
		sep = strchr(opts.dstdir, '/');
		if (sep)
			*sep = '\0';
		if (chdir(opts.dstdir) == -1)
			psync_fatal("%s", opts.dstdir);
	}

	/*
	 * Receive an 'authentication buffer' from our socket to the
	 * remote side.  This is used to authenticate connections made
	 * to our UNIX domain socket from psync slaves.
	 */
	if (!recv_auth(STDIN_FILENO, psync_authbuf))
		psync_fatal("no auth received");

	st = stream_create(STDIN_FILENO, STDOUT_FILENO);
	rpc_send_ready(st);
	spawn_worker_threads(st);

	psynclog_diag("waiting for %d puppet strings", opts.streams);
	for (i = 1; i < opts.streams; i++) {
		clifd = accept(s, NULL, NULL);
		if (!recv_auth(clifd, tbuf) ||
		    memcmp(psync_authbuf, tbuf, AUTH_LEN)) {
			close(clifd);
			i--;
			continue;
		}
		rfd = recv_fd(clifd);
		wfd = recv_fd(clifd);
		st = stream_create(rfd, wfd);
		spawn_worker_threads(st);
		push(&puppet_strings,
		    (void *)(unsigned long)clifd);
	}
	psynclog_diag("attached all puppet strings");

	close(s);

	while (psc_dynarray_len(&rcvthrs))
		usleep(10000);

	psynclog_diag("rcvthrs done");

	lc_kill(&workq);
	lc_kill(&psync_doneq);

	while (psc_dynarray_len(&wkrthrs))
		usleep(10000);
	psynclog_diag("wkrthrs done");

	DYNARRAY_FOREACH(p, i, &puppet_strings)
		close((int)(unsigned long)p);

	fcache_destroy();

	return (0);
}

void
dispthr_main(struct psc_thread *thr)
{
	char totalbuf[PSCFMT_HUMAN_BUFSIZ], xferbuf[PSCFMT_HUMAN_BUFSIZ];
	char *ce_seq = NULL, ratebuf[PSCFMT_HUMAN_BUFSIZ];
	char ratbuf[PSCFMT_RATIO_BUFSIZ];
	struct psc_waitq wq = PSC_WAITQ_INIT;
	struct timespec ts, start, d;
	uint64_t xnb, tnb;
	time_t sec = 0;

	if (tgetent(NULL, NULL) == 1)
		ce_seq = tgetstr("ce", &ce_seq);
	if (ce_seq == NULL)
		ce_seq = "";

	PFL_GETTIMESPEC(&start);
	ts = start;
	ts.tv_nsec = 0;
	while (pscthr_run(thr)) {
		/* dispthr and opstimerthr */
		if (psc_dynarray_len(&rcvthrs) == 0 &&
		    psc_dynarray_len(&wkrthrs) == 0)
			break;

		ts.tv_sec++;
		psc_waitq_waitabs(&wq, NULL, &ts);

		if (!opts.progress)
			continue;

#if 0
		POOL_LOCK(filehandles_pool);
		nfd = filehandles_pool->ppm_total -
		    filehandles_pool->ppm_nfree;
		POOL_ULOCK(filehandles_pool);
#endif

		timespecsub(&ts, &start, &d);
		sec = d.tv_sec;

		tnb = psc_atomic64_read(&nbytes_total);
		xnb = psc_atomic64_read(&nbytes_xfer);

		psc_fmt_human(ratebuf, iostats->opst_last);

		psc_fmt_human(totalbuf, tnb);
		psc_fmt_human(xferbuf, xnb);
		flockfile(stdout);
		printf("\r%2d thr  "
		    "elapsed %3ld:%02ld:%02ld  "
		    "%s xfer  "
		    "%s ",
		    opts.streams,
		    sec / 60 / 60, (sec / 60) % 60, sec % 60,
		    xferbuf, totalbuf);
		if (workq.plc_flags & PLCF_DYING) {
			psc_fmt_ratio(ratbuf, xnb, tnb);
			printf("total %6s    ", ratbuf);
		} else {
			printf("calculating...  ");
		}
		printf("%7s/s%s ", ratebuf, ce_seq);
		fflush(stdout);
		funlockfile(stdout);
	}
	if (!opts.progress)
		return;

	PFL_GETTIMESPEC(&ts);
	timespecsub(&ts, &start, &d);
	psc_fmt_human(ratebuf, iostats->opst_lifetime /
	    (d.tv_sec + d.tv_nsec * 1e-9));
	psc_fmt_human(totalbuf, psc_atomic64_read(&nbytes_total));

	printf("\r%2d thr  "
	    "elapsed %02ld:%02ld:%02ld  "
	    "%s bytes  "
	    "%6"PRId64" file(s)  "
	    "avg %7s/s%s\n",
	    opts.streams,
	    sec / 60 / 60, (sec / 60) % 60, sec % 60,
	    totalbuf,
	    psc_atomic64_read(&psync_nfiles_xfer),
	    ratebuf, ce_seq);
}

#if defined(SYS_sched_getaffinity) && !defined(CPU_COUNT)
#  define CPU_COUNT(set) _cpu_count(set)
int
_cpu_count(cpu_set_t *set)
{
	int i, n = 0;

	for (i = 0; i < sizeof(*set) / sizeof(__cpu_mask); i++)
		if (CPU_ISSET(i, set))
			n++;
	return (n);
}
#endif

int
getnprocessors(void)
{
	int np = 1;

#ifdef SYS_sched_getaffinity	/* Linux */
	cpu_set_t mask;

	if (sched_getaffinity(0, sizeof(mask), &mask) == -1)
		psynclog_warn("sched_getaffinity");
	else
		np = CPU_COUNT(&mask);

#elif defined(HW_LOGICALCPU)	/* MacOS X */
	int mib[2];

	int mib[2];
	size_t size;

	size = sizeof(np);
	mib[0] = CTL_HW;
	mib[1] = HW_LOGICALCPU;
	if (sysctl(mib, 2, &np, &size, NULL, 0) == 0)
		np = size;

#elif defined(HW_NCPU)		/* BSD */
	int mib[2];
	size_t size;

	size = sizeof(np);
	mib[0] = CTL_HW;
	mib[1] = HW_NCPU;
	if (sysctl(mib, 2, &np, &size, NULL, 0) == 0)
		np = size;

#endif
	return (np);
}

/* XXX not dynamic adjusting but better than nothing */
/* when copying local-to-local, make sure to cut estimate by half */
int
getnstreams(int want)
{
	int nstr = want;

#ifdef HAVE_GETLOADAVG
	{
		double avg;

		if (getloadavg(&avg, 1) == -1)
			psynclog_warn("getloadavg");
		// XXX round up

		nstr = MAX(1, nstr - avg);
	}
#endif
	return (MIN(nstr, MAX_STREAMS));
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [options] src ... dst\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *p, *fn, *host, *dstfn, *dstdir;
	int mode, travflags, rflags, i, rv, rc;
	struct psc_thread *dispthr, *donethr;
	struct sigaction sa;
	struct stream *st;

#if 0
	setenv("PSC_LOG_FORMAT", "%n: ", 0);
	setenv("PSC_LOG_LEVEL", "warn", 0);

	if (getenv("PSC_DUMPSTACK") == NULL)
		setenv("PSC_LOG_LEVEL", "diag", 0);
	setenv("PSC_DUMPSTACK", "1", 0);
#endif

	pfl_init();
	progname = argv[0];

	parseopts(argc, argv);
	argc -= optind;
	argv += optind;

	psync_umask = umask(0);
	umask(psync_umask);

	pscthr_init(THRT_MAIN, NULL, NULL, 0, "mainthr");

	psc_poolmaster_init(&buf_poolmaster, struct buf, lentry,
	    PPMF_AUTO, 16, 16, 0, NULL, NULL, NULL, "buf");
	buf_pool = psc_poolmaster_getmgr(&buf_poolmaster);

	psc_poolmaster_init(&work_poolmaster, struct work, wk_lentry,
	    PPMF_AUTO, 16, 16, 0, NULL, NULL, NULL, "work");
	work_pool = psc_poolmaster_getmgr(&work_poolmaster);

	psc_poolmaster_init(&filehandles_poolmaster, struct filehandle,
	    lentry, PPMF_AUTO, 16, 16, 512, NULL, NULL, NULL, "fh");
	filehandles_pool = psc_poolmaster_getmgr(&filehandles_poolmaster);

	psc_hashtbl_init(&filehandles_hashtbl, 0, struct filehandle,
	    fid, hentry, 1531, NULL, "filehandles");

	psc_hashtbl_init(&ino_hashtbl, 0, struct ino_entry, i_fid,
	    i_hentry, 1531, NULL, "ino");

	fcache_init();

	lc_reginit(&workq, struct work, wk_lentry, "workq");
	lc_reginit(&psync_doneq, struct work, wk_lentry, "doneq");

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = handle_signal;
	if (sigaction(SIGINT, &sa, NULL) == -1)
		psync_fatal("sigaction");

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = handle_signal;
	if (sigaction(SIGPIPE, &sa, NULL) == -1)
		psync_fatal("sigaction");

	donethr = pscthr_init(THRT_DONE, donethr_main, NULL, 0,
	    "donethr");

	if (opts.head)
		exit(puppet_head_mode());
	if (opts.puppet)
		exit(puppet_limb_mode());

	psync_is_master = 1;

	if (argc < 2 ||
	    (argc == 1 && psc_dynarray_len(&opts.files) == 0))
		usage();

	if (opts.offset_log_file) {
		opts.offset_log_fp = fopen(opts.offset_log_file, "a");
		if (opts.offset_log_fp == NULL)
			warn("%s", opts.offset_log_file);
	}

	/*
	 * psync a ... b
	 * psync nonexist ... b
	 * psync loc rem:fn
	 * psync nonexist ... rem:fn
	 * psync rem:fn ... loc
	 *
	 * psync rem1:fn ... rem2:fn2
	 */
	p = argv[--argc];
	dstfn = strchr(p, ':');
	if (dstfn) {
		*dstfn++ = '\0';
		host = p;
		mode = MODE_PUT;

		dstdir = dstfn;
		dstfn = strrchr(dstfn, '/');
		if (dstfn) {
			*dstfn++ = '\0';
			if (dstfn[0] == '\0')
				dstfn = ".";
		} else {
			dstfn = dstdir;
			dstdir = ".";
		}
	} else {
		struct stat stb;

		/* psync remote:file [dir/]file */
		/* psync remote:file dir */
		if (stat(p, &stb) == 0 && S_ISDIR(stb.st_mode)) {
			dstfn = "";
		} else {
			dstfn = strrchr(p, '/');
			if (dstfn == NULL) {
				dstfn = p;
				p = NULL;
			} else if (dstfn > p)
				*dstfn++ = '\0';
			else
				p = NULL;
		}
		if (p && chdir(p) == -1)
			psync_fatal("chdir %s", p);

		host = argv[0];
		for (i = 0; i < argc; i++) {
			p = strchr(argv[i], ':');
			if (p == NULL)
				psync_fatalx("no source host specified");
			*p++ = '\0';
			if (strcmp(argv[i], host)) {
				errno = ENOTSUP;
				psync_fatal("multiple source hosts");
			}
			argv[i] = p;
		}
		mode = MODE_GET;
		dstdir = ".";
	}

	iostats = pfl_opstat_init("iostats");
	pfl_opstimerthr_spawn(THRT_OPSTIMER, "opstimerthr");

	do
		opts.puppet = psc_random32u(1000000);
	while (!opts.puppet);

	pfl_random_getbytes(psync_authbuf, sizeof(psync_authbuf));

	/*
	 * XXX add:
	 *	--exclude filter patterns
	 *	--block-size
	 */
	st = stream_cmdopen("%s %s %s --PUPPET=%d --dstdir=%s --HEAD "
	    "%s%s%s-%s%s%s%s%sN%d",
	    opts.rsh, host, opts.psync_path, opts.puppet, dstdir,
	    opts.devices	? "--devices " : "",
	    opts.partial	? "--partial " : "",
	    opts.specials	? "--specials " : "",
	    opts.links		? "l" : "",
	    opts.perms		? "p" : "",
	    opts.recursive	? "r" : "",
	    opts.sparse		? "S" : "",
	    opts.times		? "t" : "",
	    opts.streams);
	send_auth(st->wfd, psync_authbuf);
	spawn_worker_threads(st);

	if (psc_compl_wait(&psync_ready) == -1)
		errx(1, "remote process failed to start\n\ncheck:\n"
		    "- psync is installed on remote host (ssh host psync -V)\n"
		    "- passwordless SSH is setup such as pubkeys or GSSAPI (e.g. kinit)");

	for (i = 1; i < opts.streams; i++) {
		/* spawning multiple ssh processes too quickly fails */
		usleep(30000 * (1 + i / 10));

		st = stream_cmdopen("%s %s %s --PUPPET=%d",
		    opts.rsh, host, opts.psync_path, opts.puppet);
		send_auth(st->wfd, psync_authbuf);
		spawn_worker_threads(st);
	}

	travflags = PFL_FILEWALKF_NOCHDIR;
	if (opts.recursive)
		travflags |= PFL_FILEWALKF_RECURSIVE;
	if (opts.verbose)
		travflags |= PFL_FILEWALKF_VERBOSE;

	dispthr = pscthr_init(THRT_DISP, dispthr_main, NULL, 0,
	    "dispthr");

	rflags = 0;
	if (argc == 1)
		rflags |= RPC_PUTNAME_F_TRYDIR;
	rc = 0;
	for (i = 0; i < argc; i++) {
		rv = walkfiles(mode, argv[i], travflags, rflags, dstfn);
		if (rv)
			rc = rv;
	}
	DYNARRAY_FOREACH(fn, i, &opts.files) {
		rv = filesfrom(mode, fn, travflags, dstfn);
		if (rv)
			rc = rv;
	}
	lc_kill(&workq);

	while (psc_dynarray_len(&rcvthrs) || psc_dynarray_len(&wkrthrs))
		usleep(10000);

	lc_kill(&psync_doneq);

	pthread_join(dispthr->pscthr_pthread, NULL);
	pthread_join(donethr->pscthr_pthread, NULL);

	fcache_destroy();

	exit(rc);
}
