/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
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
#include "pfl/iostats.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/net.h"
#include "pfl/pfl.h"
#include "pfl/pool.h"
#include "pfl/random.h"
#include "pfl/str.h"
#include "pfl/thread.h"
#include "pfl/timerthr.h"
#include "pfl/types.h"
#include "pfl/umask.h"
#include "pfl/walk.h"

#include "options.h"
#include "psync.h"
#include "rpc.h"

#define MODE_GET	0
#define MODE_PUT	1

struct work {
	struct psc_listentry	  wk_lentry;
	char			  wk_fn[PATH_MAX];
	char			  wk_basefn[NAME_MAX + 1];
	struct filehandle	 *wk_fh;
	char			 *wk_buf;
	char			  wk_host[HOST_NAME_MAX + 1];
	int			  wk_type;
	int			  wk_rflags;
	size_t			  wk_len;
	struct stat		  wk_stb;
	uint64_t		  wk_xid;
	off_t			  wk_off;
};

enum {
	THRT_DISP,
	THRT_MAIN,
	THRT_RCV,
	THRT_TIOS,
	THRT_WKR
};

const char		*progname;

struct psc_poolmaster	 buf_poolmaster;
struct psc_poolmgr	*buf_pool;

struct psc_poolmaster	 filehandles_poolmaster;
struct psc_poolmgr	*filehandles_pool;

struct psc_listcache	 workq;
struct psc_poolmaster	 work_poolmaster;
struct psc_poolmgr	*work_pool;

struct psc_iostats	 iostats;

struct psc_dynarray	 streams = DYNARRAY_INIT;

int			 psync_is_master;	/* otherwise, is RPC puppet */
mode_t			 psync_umask;

struct psc_dynarray	 wkrthrs = DYNARRAY_INIT;
struct psc_dynarray	 rcvthrs = DYNARRAY_INIT;

psc_spinlock_t		 wkrthrs_lock = SPINLOCK_INIT;
psc_spinlock_t		 rcvthrs_lock = SPINLOCK_INIT;

void
filehandle_dropref(struct filehandle *fh, size_t len)
{
	spinlock(&fh->lock);
	if (--fh->refcnt == 0 &&
	    fh->flags & FHF_DONE) {
		munmap(fh->base, len);
psynclog_tdebug("CLOSE %d\n", fh->fd);
		close(fh->fd);
		psc_pool_return(filehandles_pool, fh);
	} else
		freelock(&fh->lock);
}

void
wkrthr_main(struct psc_thread *thr)
{
	struct wkrthr *wkrthr = thr->pscthr_private;
	struct stream *st = wkrthr->st;
	struct work *wk;

	spinlock(&wkrthrs_lock);
	push(&wkrthrs, thr);
	freelock(&wkrthrs_lock);

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
//psynclog_tdebug("PUTDATA");
			if (opts.sparse == 0 ||
			    !pfl_memchk(wk->wk_fh->base + wk->wk_off, 0,
			    wk->wk_len))
				rpc_send_putdata(st, wk->wk_stb.st_ino,
				    wk->wk_off, wk->wk_fh->base +
				    wk->wk_off, wk->wk_len);
			filehandle_dropref(wk->wk_fh,
			    wk->wk_stb.st_size);
			break;
		case OPC_PUTNAME:
			rpc_send_putname(st, wk->wk_fn, &wk->wk_stb,
			    wk->wk_buf, wk->wk_rflags);
			PSCFREE(wk->wk_buf);
			break;
		}

		psc_pool_return(work_pool, wk);

		if (exit_from_signal)
			break;
	}

psynclog_tdebug("CLOSE1 %d", st->wfd);
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
	size_t blksz;

	blksz = opts.block_size ? (blksize_t)opts.block_size :
	    stb->st_blksize;
blksz = 64 * 1024;

	/* sending; push name first */
	wk = work_getitem(OPC_PUTNAME);
	memcpy(&wk->wk_stb, stb, sizeof(wk->wk_stb));
	strlcpy(wk->wk_fn, dstfn, sizeof(wk->wk_fn));
	wk->wk_rflags = rflags;
psynclog_tdebug("PUTNAME local=%s DSTFN %s fl %d", srcfn, wk->wk_fn, rflags);
	if (S_ISLNK(stb->st_mode)) {
		wk->wk_buf = PSCALLOC(PATH_MAX);
		if (readlink(srcfn, wk->wk_buf, PATH_MAX) == -1)
			psynclog_error("readlink %s", wk->wk_fn);
	}
	lc_add(&workq, wk);

	if (!S_ISREG(stb->st_mode))
		return;

	fh = psc_pool_get(filehandles_pool);
	memset(fh, 0, sizeof(*fh));
	INIT_LISTENTRY(&fh->lentry);
	fh->fd = open(srcfn, O_RDONLY);
	if (fh->fd == -1)
		err(1, "%s", srcfn);

	INIT_SPINLOCK(&fh->lock);
	fh->base = mmap(NULL, stb->st_size, PROT_READ, MAP_FILE |
	    MAP_SHARED, fh->fd, 0);

	/* push data chunks */
	for (; off < stb->st_size; off += blksz) {
#if 0
		if (opts.partial) {
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

		wk = work_getitem(OPC_PUTDATA);
		wk->wk_fh = fh;

		wk->wk_stb.st_ino = stb->st_ino;
		wk->wk_stb.st_size = stb->st_size;

		spinlock(&fh->lock);
		fh->refcnt++;
		freelock(&fh->lock);

		wk->wk_off = off;
		if (off + (off_t)blksz > stb->st_size)
			wk->wk_len = stb->st_size % blksz;
		else
			wk->wk_len = blksz;
		lc_add(&workq, wk);

		//pscthr_yield();
	}
	spinlock(&fh->lock);
	fh->flags |= FHF_DONE;
	psc_waitq_wakeall(&fh->wq);
	freelock(&fh->lock);
warnx("done with enqueueing");
}

int
push_putfile_walkcb(const char *fn, const struct stat *stb,
    __unusedx int type, __unusedx int level, void *arg)
{
	struct walkarg *wa = arg;
	char dstfn[PATH_MAX];
	const char *t;
	int rc = 0;

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
	t = fn + wa->skip;
	while (*t == '/')
		t++;
	rc = snprintf(dstfn, sizeof(dstfn), "%s/%s", wa->prefix, t);
	if (rc == -1)
		psync_fatal("snprintf");
///	if (level == 0)
//		strlcat(dstfn, pfl_basename(fn), sizeof(dstfn));

	if (level > 0)
		wa->rflags &= ~RPC_PUTNAME_F_TRYDIR;
psynclog_tdebug("ENQUEUE_PUT %s -> %s [prefix %s] rfl %d", fn, dstfn, wa->prefix, wa->rflags);

	enqueue_put(fn, dstfn, stb, wa->rflags);
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
	wk->wk_xid = psc_atomic32_inc_getnew(&psync_xid);
psynclog_tdebug("MAP %lx -> %s", wk->wk_xid, finalfn);
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
psynclog_tdebug("recvmsg");
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
puppet_limb_mode(void)
{
	struct sockaddr_un sun;
	int s, rc;
	char ch;

psynclog_tdebug("in limb");

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
	if (rc < 1 || rc > (int)sizeof(sun.sun_path))
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

	send_fd(s, 0);
	send_fd(s, 1);

psynclog_tdebug("string waiting");
	do
		rc = read(s, &ch, 1);
	while (rc == -1 && errno == EINTR);
psynclog_tdebug("string exiting");

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

	thr = pscthr_init(THRT_WKR, 0, wkrthr_main, NULL,
	    sizeof(*wkrthr), "wkrthr%d", n);
psynclog_tdebug("spawn wkrthr %p", thr);
	wkrthr = thr->pscthr_private;
	wkrthr->st = st;
	pscthr_setready(thr);

	thr = pscthr_init(THRT_RCV, 0, rcvthr_main, NULL,
	    sizeof(*rcvthr), "rcvthr%d", n);
psynclog_tdebug("spawn rcvthr %p", thr);
	rcvthr = thr->pscthr_private;
	rcvthr->st = st;
	pscthr_setready(thr);
}

int
puppet_head_mode(void)
{
	int i, rc, clifd, s, rfd, wfd;
	struct psc_dynarray puppet_strings = DYNARRAY_INIT;
	struct psc_thread *thr;
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
	if (rc < 1 || rc > (int)sizeof(sun.sun_path))
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
psynclog_tdebug("listening on %s", sun.sun_path);

	if (chdir(opts.dstdir) == -1) {
		if (errno != EEXIST)
			psync_fatal("%s", opts.dstdir);
		if (mkdir(opts.dstdir, 0755) == -1)
			psync_fatal("%s", opts.dstdir);
		if (chdir(opts.dstdir) == -1)
			psync_fatal("%s", opts.dstdir);
	}

	signal(SIGINT, handle_signal);
	signal(SIGPIPE, handle_signal);

	st = stream_create(STDIN_FILENO, STDOUT_FILENO);
	spawn_worker_threads(st);

psynclog_tdebug("waiting for strings %d", opts.streams);
	for (i = 1; i < opts.streams; i++) {
		clifd = accept(s, NULL, NULL);
		rfd = recv_fd(clifd);
		wfd = recv_fd(clifd);
		st = stream_create(rfd, wfd);
		spawn_worker_threads(st);
		push(&puppet_strings,
		    (void *)(unsigned long)clifd);
	}
psynclog_tdebug("attached all strings");

	close(s);

	DYNARRAY_FOREACH(p, i, &rcvthrs)
		pthread_join((pthread_t)p, NULL);

psynclog_tdebug("rcvthrs done");

	lc_kill(&workq);

	DYNARRAY_FOREACH(thr, i, &wkrthrs)
		pthread_join(thr->pscthr_pthread, NULL);
psynclog_tdebug("wkrthrs done");

	DYNARRAY_FOREACH(p, i, &puppet_strings)
		close((int)(unsigned long)p);
psynclog_tdebug("strings done");

	fcache_destroy();

//psynclog_tdebug("exit");
	return (0);
}

void
dispthr_main(struct psc_thread *thr)
{
	char *ce_seq = NULL, ratebuf[PSCFMT_HUMAN_BUFSIZ];
	struct psc_waitq wq = PSC_WAITQ_INIT;
	struct timespec ts, start, d;
	struct timeval dv;
	double rate;
	int sec, inuse;

	if (tgetent(NULL, NULL) == 1)
		ce_seq = tgetstr("ce", &ce_seq);
	if (ce_seq == NULL)
		ce_seq = "";

	PFL_GETTIMESPEC(&start);
	ts = start;
	ts.tv_nsec = 0;
	while (pscthr_run(thr)) {
		/* dispthr and tiosthr */
		if (psc_dynarray_len(&rcvthrs) == 0 &&
		    psc_dynarray_len(&wkrthrs) == 0)
			break;

		ts.tv_sec++;
		psc_waitq_waitabs(&wq, NULL, &ts);

		if (!opts.progress)
			continue;

		POOL_LOCK(filehandles_pool);
		inuse = filehandles_pool->ppm_total -
		    filehandles_pool->ppm_nfree;
		POOL_ULOCK(filehandles_pool);

		timespecsub(&ts, &start, &d);
		sec = d.tv_sec;

		rate = psc_iostats_getintvrate(&iostats, 0);

		psc_fmt_human(ratebuf, rate);
		printf(" %d thr  %6d fd  "
		    "elapsed %3d:%02d:%02d(s)  %7s/s%s\r",
		    opts.streams, inuse,
		    sec / 60 / 60, (sec / 60) % 60, sec % 60,
		    ratebuf, ce_seq);
		fflush(stdout);
	}
	if (!opts.progress)
		return;

	PFL_GETTIMESPEC(&ts);
	timespecsub(&ts, &start, &d);
	dv.tv_sec = sec = d.tv_sec;
	dv.tv_usec = d.tv_nsec / 1000;
	rate = psc_iostats_calcrate(iostats.ist_len_total, &dv);
	psc_fmt_human(ratebuf, rate);

	printf("summary: elapsed %02d:%02d:%02d.%02d(s) avg %7s/s%s\n",
	    sec / 60 / 60, sec / 60, sec % 60,
	    (int)(d.tv_nsec / 10000000), ratebuf, ce_seq);
}

int
getnprocessors(void)
{
#ifndef SYS_sched_getaffinity	/* Linux */
	cpu_set_t mask;

	if (sched_getaffinity(0, sizeof(mask), &mask) == -1)
		psynclog_warn("sched_getaffinity");
	else
		return (CPU_COUNT(&mask));

#elif defined(HW_LOGICALCPU)	/* MacOS X */
	int mib[2];

	int np, mib[2];
	size_t size;

	size = sizeof(np);
	mib[0] = CTL_HW;
	mib[1] = HW_LOGICALCPU;
	if (sysctl(mib, 2, &np, &size, NULL, 0) == -1)
		return (-1);

#elif defined(HW_NCPU)		/* BSD */
	int np, mib[2];
	size_t size;

	size = sizeof(np);
	mib[0] = CTL_HW;
	mib[1] = HW_NCPU;
	if (sysctl(mib, 2, &np, &size, NULL, 0) == -1)
		return (-1);

#endif
	return (1);
}

/* XXX not dynamic adjusting but better than nothing */
/* when copying to local machine, make sure to cut estimate by half */
int
getnstreams(int want)
{
	int np;

	np = getnprocessors();

#ifndef HAVE_GETLOADAVG
	{
		double avg;

		if (getloadavg(&avg, 1) == -1)
			psynclog_warn("getloadavg");

		want = MAX(np - avg, 1);
		want = MIN(want, MAX_STREAMS);
	}
#endif
	return (MIN(want, np));
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
	int mode, travflags, rflags, i, rv, rc = 0;
	struct psc_thread *dispthr;

#if 0
	setenv("PSC_LOG_FORMAT", "%n: ", 0);
	setenv("PSC_LOG_LEVEL", "warn", 0);
#endif

	pfl_init();
	progname = argv[0];

	parseopts(argc, argv);
	argc -= optind;
	argv += optind;

	psync_umask = umask(0);
	umask(psync_umask);

	pscthr_init(THRT_MAIN, 0, NULL, NULL, 0, "mainthr");

	psc_poolmaster_init(&buf_poolmaster, struct buf, lentry,
	    PPMF_AUTO, 16, 16, 0, NULL, NULL, NULL, "buf");
	buf_pool = psc_poolmaster_getmgr(&buf_poolmaster);

	psc_poolmaster_init(&work_poolmaster, struct work, wk_lentry,
	    PPMF_AUTO, 16, 16, 0, NULL, NULL, NULL, "work");
	work_pool = psc_poolmaster_getmgr(&work_poolmaster);

	psc_poolmaster_init(&filehandles_poolmaster, struct filehandle,
	    lentry, PPMF_AUTO, 16, 16, 768, NULL, NULL, NULL, "fh");
	filehandles_pool = psc_poolmaster_getmgr(&filehandles_poolmaster);

	fcache_init();

	lc_reginit(&workq, struct work, wk_lentry, "workq");

	if (opts.head)
		exit(puppet_head_mode());
	if (opts.puppet)
		exit(puppet_limb_mode());

	psync_is_master = 1;

	if (argc < 2 ||
	    (argc == 1 && psc_dynarray_len(&opts.files) == 0))
		usage();

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

	psc_iostats_init(&iostats, "iostats");
	psc_tiosthr_spawn(THRT_TIOS, "tios");

	do
		opts.puppet = psc_random32u(1000000);
	while (!opts.puppet);

	for (i = 0; i < opts.streams; i++) {
		struct stream *st;

		/* spawning multiple ssh too quickly fails */
		if (i)
			usleep(30000);

		/*
		 * XXX add:
		 *	--exclude filter patterns
		 */
		st = stream_cmdopen("%s %s %s --PUPPET=%d --dstdir=%s "
		    "%s %s %s %s -N%d",
		    opts.rsh, host, opts.psync_path, opts.puppet,
		    dstdir, i ? "" : "--HEAD",
		    opts.perms		? "-p" : "",
		    opts.recursive	? "-r" : "",
		    opts.sparse		? "-S" : "",
		    opts.streams);
		spawn_worker_threads(st);

		// XXX send nstreams request
	}

	travflags = PFL_FILEWALKF_RELPATH;
	if (opts.recursive)
		travflags |= PFL_FILEWALKF_RECURSIVE;
	if (opts.verbose)
		travflags |= PFL_FILEWALKF_VERBOSE;

	signal(SIGINT, handle_signal);
	signal(SIGPIPE, handle_signal);

	dispthr = pscthr_init(THRT_DISP, 0, dispthr_main, NULL, 0,
	    "dispthr");

	rflags = 0;
	if (argc == 1)
		rflags |= RPC_PUTNAME_F_TRYDIR;
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

	pthread_join(dispthr->pscthr_pthread, NULL);

psynclog_tdebug("exiting");

	fcache_destroy();

	exit(rc);
}
